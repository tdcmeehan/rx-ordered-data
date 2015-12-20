package rx.orderedzip.impl;

import rx.Producer;
import rx.internal.operators.BackpressureUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple Producer which delegates complex backpressure management to the Joiner.  For simplicity this extends
 * AtomicLong so it itself can atomically keep track of the number of requests, and it can be passed as an AtomicLong
 * to the {@link Zipper}.
 */
final class OrderedZipProducer<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> implements Producer {
    private final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper;
    final AtomicLong requested = new AtomicLong(); // package private

    public OrderedZipProducer(final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper) {
        this.zipper = zipper;
    }

    @Override
    public final void request(final long n) {
        BackpressureUtils.getAndAddRequest(requested, n);
        zipper.emit();
    }
}
