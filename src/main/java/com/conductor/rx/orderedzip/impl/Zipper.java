package com.conductor.rx.orderedzip.impl;

import com.conductor.rx.ordered.flow.join.ZipType;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.OnErrorThrowable;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.internal.util.RxRingBuffer;
import rx.subscriptions.CompositeSubscription;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is the main class used to manage upstream emissions and downstream backpressure.  Notes on the implementation:
 * 1) No locks or blocking whatsoever.  The way it works is it receives the upstream emissions and puts them in a
 *    small queue; when one side is slower, the other side is requested more items with reactive pull backpressure
 * 2) This class is stateful, but each of the "stateful" aspects of the class are managed in a thread safe way in
 *    a CAS protected block, which ensures they're only ever accessed from just one thread.
 */
final class Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT>  {
    private final CompositeSubscription childSubscription; // An all in one subscription representing all the upstreams
    private final Subscriber<? super RESULT> downstream; // The downstream subscriber
    private final Comparator<KEY> comparator; // The function used to determine relative position in the stream
    private final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction; // The function used construct the joined output
    private final ZipType zipType; // The join type to operate on
    private final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction; // The keying function for the left upstream
    private final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction; // The keying function for the right upstream
    private final AtomicLong requestedEmissions = new AtomicLong(0); // The number of requested emissions from the upstreams

    private UpstreamSubscriberQueue<KEY, LEFT_VALUE, LEFT_VALUE, RIGHT_VALUE, RESULT> leftHandSide; // The left upstream
    private UpstreamSubscriberQueue<KEY, RIGHT_VALUE, LEFT_VALUE, RIGHT_VALUE, RESULT> rightHandSide; // The right upstream
    private AtomicLong requested = null; // This is the Producer from the JoinSubscriber, which tells us the number requested downstream

    // Stateful variables which respectively keep track of the first emission, and the number of items drained from
    // the left and right upstreams
    boolean allUpstreamsStarted = false;
    int leftPolled = 0;
    int rightPolled = 0;

    // Per the Zip operator--this is a slightly arbitrary number.  The point is to batch upstream requests to
    // avoid too much context switching
    private static int EMITTED_THRESHOLD = (int) (RxRingBuffer.SIZE * 0.7);

    final AtomicReference<Throwable> error;

    // package private
    Zipper(
            final Subscriber<? super RESULT> downstream,
            final Comparator<KEY> comparator,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final ZipType zipType
    ) {
        this.childSubscription = new CompositeSubscription(downstream);
        this.downstream = downstream;
        this.comparator = comparator;
        this.leftHandKeyingFunction = leftHandKeyingFunction;
        this.rightHandKeyingFunction = rightHandKeyingFunction;
        this.joinFunction = joinFunction;
        this.zipType = zipType;
        this.error = new AtomicReference<Throwable>(null); // Magic number--should be more than reasonable
    }

    // package private
    void error(final Throwable ex) {
        if (!error.compareAndSet(null, ex)) {
            // ERROR, swallowing exception due to another error being emitted
        }
        emit();
    }

    // package private
    void emit() {
        // Concurrency time: the below requestedEmissions atomic will ensure that only one thread will win when
        // attempting to enter this block.
        if (requestedEmissions.getAndIncrement() == 0) {
            // Explanation on the do/while: it's not sufficient to simply have one thread do the work.  We should
            // handle cases where one thread keeps notifying the current thread of extra emissions.  This will reduce
            // context switching and keep the logic easier to reason about (one thread loses and does work for the others,
            // and the others simply notify the loser how much they need to drain)
            do {
                if (!allUpstreamsStarted) {
                    allUpstreamsStarted = leftHandSide != null && rightHandSide != null && leftHandSide.isStarted() && rightHandSide.isStarted();
                }
                // If both sides haven't started, attempt to exit early
                if (allUpstreamsStarted) {
                    // Handle any upstream errors
                    if (error.get() != null) {
                        downstream.onError(error.get());
                        return;
                    }

                    // Explanation on this loop: because this class also manages backpressure, it knows when to
                    // request more items need to be sent to it.  So even though the other do/while keeps track of
                    // how many times an emission has been requested, that count may not be enough, or it may be too
                    // much.  For efficiency's sake, just keep working until told when to stop.
                    while (true) {
                        // Don't care about extra work--eagerly attempt to reset dupes (safe because of the outer loop)
                        requestedEmissions.lazySet(1);

                        LEFT_VALUE leftValue = leftHandSide.peek();
                        RIGHT_VALUE rightValue = rightHandSide.peek();
                        boolean leftSideCompleted = leftValue != null && leftHandSide.isCompleted(leftValue);
                        boolean rightSideCompleted = rightValue != null && rightHandSide.isCompleted(rightValue);

                        if (leftSideCompleted && rightSideCompleted) {
                            downstream.onCompleted();
                            childSubscription.unsubscribe();
                            return;
                        }
                        // If downstream backpressure has kicked in, stop emitting if we've already given it the
                        // amount it has requested
                        if (requested.get() > 0) {
                            // Otherwise, process the stream!  If we have values for both the left stream and right
                            // stream, process them
                            try {
                                if (leftValue != null && rightValue != null && !leftSideCompleted && !rightSideCompleted) {
                                    int comparisonResult = comparator.compare(leftHandKeyingFunction.call(leftValue), rightHandKeyingFunction.call(rightValue));
                                    // If the sides are equal, then no matter the join time, process both sides
                                    if (comparisonResult == 0) {
                                        processBothSides(leftValue, rightValue);
                                    }
                                    // If the left side is behind the right side, process the left side
                                    else if (comparisonResult < 0) {
                                        processLeftSide(leftValue);
                                    }
                                    // If the right side is behind the left side, process the right side
                                    else {
                                        processRightSide(rightValue);
                                    }
                                }
                                // If the left side is completed, but this is a full outer join, drain the right hand side until that completes
                                else if (rightValue != null && leftSideCompleted) {
                                    processRightSide(rightValue);
                                }
                                // If the right side is completed but this is a left join or outer join, drain the left hand side until that completes
                                else if (leftValue != null && rightSideCompleted) {
                                    processLeftSide(leftValue);
                                } else {
                                    // Not all values are ready
                                    break;
                                }
                            }
                            catch (final Throwable e) {
                                downstream.onError(OnErrorThrowable.addValueAsLastCause(e, new Object[]{leftValue, rightValue}));
                                return;
                            }
                            // If any of the items above were processed, then check if the number processed exceeded
                            // an arbitrary threshold; if it did, request more items downstream
                            boolean anyRequestedMore = false;
                            if (leftPolled > EMITTED_THRESHOLD) {
                                if (!leftSideCompleted) {
                                    leftHandSide.requestMore(leftPolled);
                                    leftPolled = 0;
                                    anyRequestedMore = true;
                                }
                            }
                            if (rightPolled > EMITTED_THRESHOLD) {
                                if (!rightSideCompleted) {
                                    rightHandSide.requestMore(rightPolled);
                                    rightPolled = 0;
                                    anyRequestedMore = true;
                                }
                            }
                            if (anyRequestedMore && (leftHandSide.peek() == null || rightHandSide.peek() == null)) {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
            } while (requestedEmissions.decrementAndGet() != 0);
        }
    }

    private void processBothSides(final LEFT_VALUE leftValue, final RIGHT_VALUE rightValue) {
        leftHandSide.poll();
        rightHandSide.poll();
        downstream.onNext(joinFunction.call(leftValue, rightValue));
        requested.decrementAndGet();
        leftPolled++;
        rightPolled++;
    }

    private void processLeftSide(final LEFT_VALUE leftValue) {
        leftHandSide.poll();
        // If the zip type is outer or left, then we can process this value
        if (zipType == ZipType.LEFT || zipType == ZipType.OUTER) {
            downstream.onNext(joinFunction.call(leftValue, null));
            requested.decrementAndGet();
        }
        leftPolled++;
    }

    private void processRightSide(final RIGHT_VALUE rightValue) {
        rightHandSide.poll();
        // If the zip type is outer, then we can process this value
        if (zipType == ZipType.OUTER) {
            downstream.onNext(joinFunction.call(null, rightValue));
            requested.decrementAndGet();
        }
        rightPolled++;
    }

    /**
     * Start the work of managing backpressure and upstream emissions.
     * @param leftSideObservable the left observable
     * @param rightSideObservable the right observable
     * @param requested an atomic reference to the downstream requested amount
     */
    public void start(final Observable<LEFT_VALUE> leftSideObservable, Observable<RIGHT_VALUE> rightSideObservable, final AtomicLong requested) {
        this.requested = requested;
        this.leftHandSide = UpstreamSubscriberQueue.createLeft(this);
        this.rightHandSide = UpstreamSubscriberQueue.createRight(this);
        this.childSubscription.add(leftHandSide);
        this.childSubscription.add(rightHandSide);
        leftSideObservable.unsafeSubscribe(leftHandSide);
        rightSideObservable.unsafeSubscribe(rightHandSide);
    }
}
