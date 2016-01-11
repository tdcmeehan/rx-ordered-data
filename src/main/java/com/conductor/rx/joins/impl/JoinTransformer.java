package com.conductor.rx.joins.impl;

import com.conductor.rx.operators.AggregationOperators;
import com.conductor.rx.operators.OrderedZipOperators;
import com.conductor.rx.ordered.flow.join.JoinType;
import com.conductor.rx.ordered.flow.join.ZipType;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;

import java.util.Comparator;
import java.util.List;

/**
 * Created by tmeehan on 9/1/15.
 */
public class JoinTransformer<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> implements Observable.Transformer<LEFT_VALUE, RESULT> {

    private final Observable<RIGHT_VALUE> rightHandSide;
    private final Comparator<KEY> comparator;
    private final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction;
    private final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction;
    private final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction;
    private final JoinType joinType;

    public JoinTransformer(
            final Observable<RIGHT_VALUE> rightHandSide,
            final Comparator<KEY> comparator,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final JoinType joinType
    ) {
        this.rightHandSide = rightHandSide;
        this.comparator = comparator;
        this.leftHandKeyingFunction = leftHandKeyingFunction;
        this.rightHandKeyingFunction = rightHandKeyingFunction;
        this.joinFunction = joinFunction;
        this.joinType = joinType;
    }

    /**
     * Calls the Transformer.  Breakdown of the flow:
     * 1. Left side observable is buffered by the keying function
     * 2. Right side observable is buffered by the keying function
     * 3. An ordered zip is applied on each streams of buffers
     * 3.a. Streams are processed according to the cross product function, which preserves SQL functionality
     * 4. The results of this are flattened back into an Observable of RESULTs
     * @param leftObservable
     * @return
     */
    @Override
    public Observable<RESULT> call(final Observable<LEFT_VALUE> leftObservable) {
        final ZipType zipType = joinType.toZipType();
        final Observable<List<LEFT_VALUE>> bufferedLeftObservable = leftObservable.compose(AggregationOperators.bufferByKey(leftHandKeyingFunction));
        final Observable<List<RIGHT_VALUE>> bufferedRightObservable = rightHandSide.compose(AggregationOperators.bufferByKey(rightHandKeyingFunction));
        return Observable.concat(
                bufferedLeftObservable
                .compose(OrderedZipOperators.zipWith(
                        bufferedRightObservable,
                        comparator,
                        JoinTransformer.<LEFT_VALUE, KEY>bufferedKeyFunction(leftHandKeyingFunction),
                        JoinTransformer.<RIGHT_VALUE, KEY>bufferedKeyFunction(rightHandKeyingFunction),
                        crossProductJoinFunction(joinFunction, joinType),
                        zipType
                ))
        );
    }

    /**
     * Transforms a function which applies on a single element of {@code VALUE} to a function which applies on the first
     * element of a list of {@code VALUE}s.
     * @param innerKeyFunction the function to be applied to the first element
     * @return the key value of the function result
     */
    public static <VALUE, KEY> Func1<List<VALUE>, KEY> bufferedKeyFunction(final Func1<? super VALUE, KEY> innerKeyFunction) {
        return input -> innerKeyFunction.call(input.get(0));
    }

    /**
     * Returns a function which, given two lists of values, returns an Observable of their cross product.  Depending
     * on the join type, null left or right side values are permitted; see {@link com.conductor.rx.ordered.flow.join.JoinType}
     * for more details.
     * @param joinFunction the function to apply the product between the two lists
     * @param joinType the type of join being applied
     * @return a function which operates on list of left values, lists of right values, and returns an Observable of the result
     */
    public static <LEFT_VALUE, RIGHT_VALUE, RESULT> Func2<List<LEFT_VALUE>, List<RIGHT_VALUE>, Observable<RESULT>> crossProductJoinFunction(
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final JoinType joinType
    ) {
        return (leftValuesInput, rightValuesInput) -> {
            final List<RESULT> resultList = Lists.newArrayList();
            if (leftValuesInput != null && rightValuesInput != null) {
                for (LEFT_VALUE leftValue : leftValuesInput) {
                    for (RIGHT_VALUE rightValue : rightValuesInput) {
                        resultList.add(joinFunction.call(leftValue, rightValue));
                    }
                }
            } else if (leftValuesInput == null && rightValuesInput != null
                    && joinType == JoinType.FULL_OUTER) {
                for (RIGHT_VALUE rightValue : rightValuesInput) {
                    resultList.add(joinFunction.call(null, rightValue));
                }
            } else if (rightValuesInput == null && leftValuesInput != null
                    && (joinType == JoinType.LEFT || joinType == JoinType.FULL_OUTER)) {
                for (LEFT_VALUE leftValue : leftValuesInput) {
                    resultList.add(joinFunction.call(leftValue, null));
                }
            } else {
                throw new IllegalStateException(
                        String.format("Encountered an incorrect join state, join type: %s, left side: %s, right side: %s",
                                joinType.toString(), Objects.firstNonNull(leftValuesInput, "null").toString(), Objects.firstNonNull(rightValuesInput, "null").toString())
                );
            }
            return Observable.from(resultList);
        };
    }
}
