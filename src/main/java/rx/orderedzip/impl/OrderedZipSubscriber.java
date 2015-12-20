package rx.orderedzip.impl;

import com.google.common.base.Preconditions;
import rx.ordered.internal.util.Duple;
import rx.Observable;
import rx.Subscriber;

/**
 * A thin factory subscriber which forwards a list of Observables to a {@link Zipper}
 * for notification management.
 */
final class OrderedZipSubscriber<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> extends Subscriber<Duple<Observable<LEFT_VALUE>, Observable<RIGHT_VALUE>>> {
    private final Subscriber<? super RESULT> downstream;
    private final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper;
    private final OrderedZipProducer<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> orderedZipProducer;

    // Keeping track of the first instance of this being invoked
    boolean started = false;

    // package private
    OrderedZipSubscriber(
            final Subscriber<? super RESULT> downstream,
            final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper,
            OrderedZipProducer<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> orderedZipProducer
    ) {
        super(downstream);
        this.downstream = downstream;
        this.zipper = zipper;
        this.orderedZipProducer = orderedZipProducer;
    }

    @Override
    public final void onCompleted() {
        Preconditions.checkState(started, "Error: empty stream detected");
    }

    @Override
    public final void onError(Throwable e) {
        downstream.onError(e);
    }

    @Override
    public void onNext(Duple<Observable<LEFT_VALUE>, Observable<RIGHT_VALUE>> observables) {
        Preconditions.checkNotNull(observables);
        Preconditions.checkState(observables.getFst() != null && observables.getSnd() != null);
        started = true;
        zipper.start(observables.getFst(), observables.getSnd(), orderedZipProducer.requested);
    }
}
