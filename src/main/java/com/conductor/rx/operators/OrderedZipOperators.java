package com.conductor.rx.operators;

import com.conductor.rx.ordered.flow.join.ZipType;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;
import com.conductor.rx.orderedzip.impl.OrderedZipTransformer;

import java.util.Comparator;

/**
 * Returns an Observable which zips the results from two upstream Observables based on a joining function, keeping track
 * of relative ordering.
 *
 * Created by tmeehan on 5/17/15.
 */
public final class OrderedZipOperators {
    private OrderedZipOperators() {}

    /**
     * Returns an Observable which zips the results from two upstream Observables based on a joining function.  Similar
     * to the Zip operator, except this offers greater flexibility to selectively filter out data based on the type of
     * zip type selected.
     *
     * This operator uses "reactive pull" backpressure to slow down the source observable in case it's over producing.
     * This means the source of data from this stream should support backpressure.  The easiest way to do this is to model
     * it as an Iterable or Generator, or if this isn't possible, to use {@link rx.observables.SyncOnSubscribe} in the
     * Observable.create method.
     *
     * @param leftHandSide the left side observable
     * @param rightHandSide the right side observable to be joined against
     * @param ordering a comparator which specifies the relative ordering of both streams
     * @param leftHandKeyingFunction a function which returns a key value, used by the comparator to determine the relative
     *                               location in the stream
     * @param rightHandKeyingFunction a function which returns a key value, used by the comparator to determine the relative
     *                                location in the stream
     * @param joinFunction a function which join the values from the two streams
     * @param zipType the type of join to perform -- left, full outer, and inner (as in SQL)
     * @return the joined observable
     */
    public static final <KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> Observable<RESULT> zip(
            final Observable<LEFT_VALUE> leftHandSide,
            final Observable<RIGHT_VALUE> rightHandSide,
            final Comparator<KEY> ordering,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final ZipType zipType
    ) {
        return leftHandSide.compose(zipWith(
                rightHandSide,
                ordering,
                leftHandKeyingFunction,
                rightHandKeyingFunction,
                joinFunction,
                zipType
        ));
    }

    /**
     * Returns a Transformer which zips the results from the source observable with another observable based on a joining
     * function.  Similar to the Zip operator, except this offers greater flexibility to selectively filter out data
     * based on the type of zip type selected.  Used with the {@link rx.Observable#compose} method to provide method
     * chaining (for an example see above).
     *
     * This operator uses "reactive pull" backpressure to slow down the source observable in case it's over producing.
     * This means the source of data from this stream should support backpressure.  The easiest way to do this is to model
     * it as an Iterable or Generator, or if this isn't possible, to use {@link rx.observables.SyncOnSubscribe} in the
     * Observable.create method.
     *
     * @param rightHandSide the observable to join against
     * @param ordering a comparator which specifies the relative ordering of both streams
     * @param leftHandKeyingFunction a function which returns a key value, used by the comparator to determine the relative
     *                               location in the stream
     * @param rightHandKeyingFunction a function which returns a key value, used by the comparator to determine the relative
     *                                location in the stream
     * @param joinFunction a function which join the values from the two streams
     * @param zipType the type of join to perform -- left, outer, and inner
     * @return the joined observable
     */
    public static final <KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> Observable.Transformer<LEFT_VALUE, RESULT> zipWith(
            final Observable<RIGHT_VALUE> rightHandSide,
            final Comparator<KEY> ordering,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final ZipType zipType
    ) {
        return new OrderedZipTransformer<>(
                rightHandSide,
                ordering,
                leftHandKeyingFunction,
                rightHandKeyingFunction,
                joinFunction,
                zipType
        );
    }
}