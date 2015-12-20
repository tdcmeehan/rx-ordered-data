package rx.orderedzip.impl;

import rx.Subscriber;
import rx.internal.util.RxRingBuffer;

/**
 * Represents a single upstream Subscriber that maintains a small queue of notifications from the upstream observable.
 * This shouldn't raise any questions of buffer bloat; the queue itself is only a maximum of 128 elements (unless
 * it's overridden somehow).  It's primarily used as a lightweight storage so we can process individual subscriptions
 * without them existing in separate threads.
 */
final class UpstreamSubscriberQueue<KEY, SUBSCRIBER_VALUE, LEFT_VALUE, RIGHT_VALUE, RESULT> extends Subscriber<SUBSCRIBER_VALUE> {
    // This subscriber's "downstream" which delegates notifications to a ring buffer queue.
    private final RxRingBuffer queue;
    // The Joiner class which performs the business logic and orchestrates the joining and backpressure management
    private final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper;

    // Is the stream started
    private volatile boolean started = false;

    // Some helpers to somewhat alleviate generics insanity
    public static <KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> UpstreamSubscriberQueue<KEY, LEFT_VALUE, LEFT_VALUE, RIGHT_VALUE, RESULT> createLeft(final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper) {
        return new UpstreamSubscriberQueue<KEY, LEFT_VALUE, LEFT_VALUE, RIGHT_VALUE, RESULT>(zipper);
    }

    public static <KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> UpstreamSubscriberQueue<KEY, RIGHT_VALUE, LEFT_VALUE, RIGHT_VALUE, RESULT> createRight(final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper) {
        return new UpstreamSubscriberQueue<KEY, RIGHT_VALUE, LEFT_VALUE, RIGHT_VALUE, RESULT>(zipper);
    }

    private UpstreamSubscriberQueue(final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper) {
        this.queue = RxRingBuffer.getSpscInstance();
        this.zipper = zipper;
    }

    public final boolean isCompleted(SUBSCRIBER_VALUE value) {
        return queue.isCompleted(value) || queue.isError(value);
    }

    public final boolean isStarted() {
        return started;
    }

    @Override
    public final void onCompleted() {
        queue.onCompleted();
        zipper.emit(); // Emit so the Joiner can handle downstream notification if it's ready
    }

    @Override
    public final void onError(Throwable e) {
        zipper.error(e);
    }

    @Override
    public final void onNext(final SUBSCRIBER_VALUE value) {
        try {
            queue.onNext(value);
            zipper.emit();
        } catch (Exception mbe) {
            try {
                onError(mbe);
            } finally {
                if (!queue.isUnsubscribed()) {
                    unsubscribe();
                }
            }
        }
    }

    @Override
    public final void onStart() {
        started = true;
        // Batch upfront an initial state of elements to process
        request(RxRingBuffer.SIZE);
    }

    @SuppressWarnings("unchecked")
    public final SUBSCRIBER_VALUE poll() {
        return (SUBSCRIBER_VALUE) queue.getValue(queue.poll());
    }

    @SuppressWarnings("unchecked")
    public final SUBSCRIBER_VALUE peek() {
        return (SUBSCRIBER_VALUE) queue.getValue(queue.peek());
    }

    // package private
    void requestMore(final long amount) {
        request(amount);
    }
}
