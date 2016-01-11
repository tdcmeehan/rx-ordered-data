package com.conductor.rx.operators;

import com.google.common.base.Preconditions;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

/**
 * Given a stream of items which are grouped sequentially by key, transform an incoming Stream into a list of the
 * grouped items.
 *
 * Created by tmeehan on 5/17/15.
 */
public final class AggregationOperators {

    private AggregationOperators() {} // Static util

    /**
     * Buffers a stream until the keying function returns a different value, returning a an Observable of Lists of
     * the desired type.  Buffering occurs one-at-a-time; whenever the key value has changed, the previous key's buffer
     * is emitted immediately.  Note that the emitted list is a Guava {@link com.google.common.collect.ImmutableList},
     * which means any operation which would modify the contents of the list will throw an exception.
     *
     * @param keyFunction a function to determine the grouped sequence
     * @return
     */
    public static <T, K> Observable.Transformer<? super T, List<T>> bufferByKey(Func1<? super T, K> keyFunction) {
        Preconditions.checkNotNull(keyFunction);
        return (observableToAggregate) -> observableToAggregate.lift(new BufferByKeyOperator<>(keyFunction));
    }

    /**
     * Buffers a stream until the keying function returns a different value, returning a an Observable of Lists of
     * the desired type.  Buffering occurs one-at-a-time; whenever the key value has changed, the previous key's buffer
     * is emitted immediately.
     *
     * @param keyFunction a function to determine the grouped sequence
     * @param listCopyStrategy a function reference to use when copying the aggregated list--supply this when the emitted
     *                         list needs to be mutable or has some special concurrency requirements
     * @return
     */
    public static <T, K> Observable.Transformer<? super T, List<T>> bufferByKey(Func1<? super T, K> keyFunction, final Func1<List<T>, List<T>> listCopyStrategy) {
        Preconditions.checkNotNull(keyFunction);
        return (observableToAggregate) -> observableToAggregate.lift(new BufferByKeyOperator<>(keyFunction, listCopyStrategy));
    }

    /**
     * Similar to {@link #bufferByKey}, except an additional map is called to transform the List emissions into a
     * dedicated type.
     *
     * @param keyFunction a function to determine the grouped sequence
     * @param aggregateFunction a function which takes in a List and maps this to a distinct type
     * @return
     */
    public static <VALUE, KEY, AGGREGATE> Observable.Transformer<VALUE, AGGREGATE> aggregate(
            final Func1<VALUE, KEY> keyFunction,
            final Func1<List<VALUE>, AGGREGATE> aggregateFunction
    ) {
        return valueObservable -> valueObservable.compose(AggregationOperators.bufferByKey(keyFunction))
                .map(aggregateFunction);
    }

    /**
     * Similar to {@link #bufferByKey}, except an additional map is called to transform the List emissions into a
     * dedicated type.
     *
     * @param keyFunction a function to determine the grouped sequence
     * @param listCopyStrategy a function reference to use when copying the aggregated list--supply this when the emitted
     *                         list needs to be mutable or has some special concurrency requirements
     * @param aggregateFunction a function which takes in a List and maps this to a distinct type
     * @return
     */
    public static <VALUE, KEY, AGGREGATE> Observable.Transformer<VALUE, AGGREGATE> aggregate(
            final Func1<VALUE, KEY> keyFunction,
            final Func1<List<VALUE>, List<VALUE>> listCopyStrategy,
            final Func1<List<VALUE>, AGGREGATE> aggregateFunction
    ) {
        return valueObservable -> valueObservable.compose(AggregationOperators.bufferByKey(keyFunction, listCopyStrategy))
                .map(aggregateFunction);
    }

}