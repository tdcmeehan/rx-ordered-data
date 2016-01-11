package com.conductor.rx.operators;

import com.google.common.collect.Ordering;
import rx.Observable;
import rx.Producer;
import rx.Subscriber;
import rx.exceptions.MissingBackpressureException;
import rx.internal.operators.BackpressureUtils;
import rx.internal.operators.NotificationLite;
import rx.internal.util.RxRingBuffer;
import rx.internal.util.unsafe.MpscLinkedQueue;

import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An RxJava operator for merging two or more streams of {@link rx.Observable} entities that are known to emit entities in
 * a consistent and specified order.
 *
 * Adapted from code written by David Karnok &lt;akarnokd@gmail.com&gt;:
 *  <a href="https://gist.github.com/akarnokd/c86a89738199bbb37348">SortedMerge.java</a>
 *
 * @param <T> the entity type
 */
public class SortedMerge<T> implements Observable.OnSubscribe<T> {
    private final List<Observable<? extends T>> sources;
    private final Comparator<? super T> comparator;

    private SortedMerge(final List<Observable<? extends T>> sources, final Comparator<? super T> comparator) {
        this.sources = checkNotNull(sources);
        this.comparator = checkNotNull(comparator);
    }

    /**
     * Creates and returns a new sorted merge {@code Observable} that emits (in sorted order) the merged emissions of
     * the specified ordered sources, where the entities emitted are themselves naturally {@link Comparable}.
     *
     * @param sources the ordered sources
     * @param <U> the entity type
     * @return a sorted merge observable
     */
    public static <U extends Comparable<? super U>> Observable<U> create(final List<Observable<? extends U>> sources) {
        return create(sources, Ordering.<U>natural());
    }

    /**
     * Creates and returns a new sorted merge {@code Observable} that emits (in sorted order) the merged emissions of
     * the specified ordered sources, using the specified {@link java.util.Comparator} to order the downstream output.
     *
     * @param sources the ordered sources
     * @param comparator the downstream comparator
     * @param <U> the entity type
     * @return a sorted merge observable
     */
    public static <U> Observable<U> create(
            final List<Observable<? extends U>> sources, final Comparator<? super U> comparator) {

        return Observable.create(new SortedMerge<>(sources, comparator));
    }

    @Override
    public void call(final Subscriber<? super T> child) {
        final SourceSubscriber<T>[] sources = new SourceSubscriber[this.sources.size()];
        final MergeProducer<T> mp = new MergeProducer<>(sources, child, comparator);
        for (int i = 0; i < sources.length; i++) {
            if (child.isUnsubscribed()) {
                return;
            }
            final SourceSubscriber<T> s = new SourceSubscriber<>(mp);
            sources[i] = s;
            child.add(s);
        }
        mp.set(0);
        child.setProducer(mp);
        int i = 0;
        for (final Observable<? extends T> source : this.sources) {
            if (child.isUnsubscribed()) {
                return;
            }
            source.unsafeSubscribe(sources[i]);
            i++;
        }
    }

    private static final class MergeProducer<T> extends AtomicLong implements Producer {
        private final NotificationLite<T> nl = NotificationLite.instance();
        private final Comparator<? super T> comparator;

        private final SourceSubscriber[] sources;
        private final Subscriber<? super T> child;

        private final Queue<Throwable> errors;

        private boolean emitting;
        private boolean missed;

        public MergeProducer(
                final SourceSubscriber[] sources, final Subscriber<? super T> child,
                final Comparator<? super T> comparator) {

            this.sources = sources;
            this.errors = new MpscLinkedQueue<>();
            this.child = child;
            this.comparator = comparator;
        }

        @Override
        public void request(final long n) {
            BackpressureUtils.getAndAddRequest(this, n);
            emit();
        }

        public void error(final Throwable ex) {
            errors.offer(ex);
            emit();
        }

        public void emit() {
            synchronized (this) {
                if (emitting) {
                    missed = true;
                    return;
                }
                emitting = true;
            }

            final SourceSubscriber<T>[] sources = this.sources;
            final int n = sources.length;
            final Subscriber<? super T> child = this.child;

            while (true) {
                if (child.isUnsubscribed()) {
                    return;
                }

                if (!errors.isEmpty()) {
                    child.onError(errors.poll());
                    return;
                }

                long r = get();
                long e = 0;

                if (r == 0) {
                    int doneCount = 0;
                    for (final SourceSubscriber<T> s : sources) {
                        if (s == null) {
                            doneCount++;
                        } else {
                            if (s.done && s.queue.isEmpty()) {
                                doneCount++;
                            }
                        }
                    }
                    if (doneCount == n) {
                        child.onCompleted();
                        return;
                    }
                }
                while (r != 0) {
                    if (child.isUnsubscribed()) {
                        return;
                    }

                    if (!errors.isEmpty()) {
                        child.onError(errors.poll());
                        return;
                    }

                    boolean fullRow = true;
                    boolean hasAtLeastOne = false;

                    T minimum = null;
                    int toPoll = -1;
                    int doneCount = 0;

                    for (int i = 0; i < n; i++) {
                        final SourceSubscriber<T> s = sources[i];

                        if (s == null) {
                            doneCount++;
                            continue;
                        }

                        boolean d = s.done;
                        final Object o = s.queue.peek();

                        if (o == null) {
                            if (d) {
                                sources[i] = null;
                                doneCount++;
                                continue;
                            }
                            fullRow = false;
                            break;
                        }
                        if (hasAtLeastOne) {
                            T v = nl.getValue(o);
                            int c = comparator.compare(minimum, v);
                            if (c > 0) {
                                minimum = v;
                                toPoll = i;
                            }
                        } else {
                            minimum = nl.getValue(o);
                            hasAtLeastOne = true;
                            toPoll = i;
                        }
                    }
                    if (doneCount == n) {
                        child.onCompleted();
                        return;
                    }
                    if (fullRow) {
                        if (toPoll >= 0) {
                            final SourceSubscriber<T> s = sources[toPoll];
                            s.queue.poll();
                            s.requestMore(1);
                        }
                        child.onNext(minimum);
                        if (r != Long.MAX_VALUE) {
                            r--;
                            e++;
                        }
                    } else {
                        break;
                    }
                }

                if (e != 0) {
                    addAndGet(-e);
                }

                synchronized (this) {
                    if (!missed) {
                        emitting = false;
                        return;
                    }
                    missed = false;
                }
            }
        }
    }

    private static final class SourceSubscriber<T> extends Subscriber<T> {
        private final RxRingBuffer queue;
        private final MergeProducer<T> parent;
        private volatile boolean done;

        public SourceSubscriber(final MergeProducer<T> parent) {
            queue = RxRingBuffer.getSpscInstance();
            this.parent = parent;
        }

        @Override
        public void onStart() {
            add(queue);
            request(RxRingBuffer.SIZE);
        }

        public void requestMore(final long n) {
            request(n);
        }

        @Override
        public void onNext(final T t) {
            try {
                queue.onNext(parent.nl.next(t));
            } catch (final MissingBackpressureException mbe) {
                try {
                    onError(mbe);
                } finally {
                    unsubscribe();
                }
                return;
            } catch (final IllegalStateException ex) {
                // This gets thrown in RxRingBuffer when this stream has been unsubscribed
                // This can only happen when SourceSubscriber#unsubscribe has been called (so the Subscriber
                // must be unsubscribed at this point)
                return;
            }
            parent.emit();
        }

        @Override
        public void onError(final Throwable e) {
            done = true;
            parent.error(e);
        }

        @Override
        public void onCompleted() {
            done = true;
            parent.emit();
        }
    }
}