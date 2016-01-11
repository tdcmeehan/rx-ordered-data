package com.conductor.rx.orderedzip.impl;

import com.conductor.rx.ordered.flow.join.ZipType;
import com.conductor.rx.ordered.internal.util.Duple;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.functions.Func2;

import java.util.Comparator;

/**
 * An Operator which performs a streaming join.  This primarily delegates to {@link OrderedZipProducer}
 * and {@link OrderedZipSubscriber}.
 */
final class OrderedZipOperator<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> implements Observable.Operator<RESULT, Duple<Observable<LEFT_VALUE>, Observable<RIGHT_VALUE>>> {
    private final Comparator<KEY> comparator;
    private final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction;
    private final ZipType zipType;
    private final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction;
    private final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction;

    public OrderedZipOperator(
            final Comparator<KEY> comparator,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final ZipType zipType
    ) {
        this.comparator = comparator;
        this.leftHandKeyingFunction = leftHandKeyingFunction;
        this.rightHandKeyingFunction = rightHandKeyingFunction;
        this.joinFunction = joinFunction;
        this.zipType = zipType;
    }

    @Override
    public Subscriber<? super Duple<Observable<LEFT_VALUE>, Observable<RIGHT_VALUE>>> call(final Subscriber<? super RESULT> downstream) {
        final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper = new Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT>(
                downstream,
                comparator,
                leftHandKeyingFunction,
                rightHandKeyingFunction,
                joinFunction,
                zipType
        );
        final OrderedZipProducer<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> orderedZipProducer = new OrderedZipProducer<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT>(zipper);
        downstream.setProducer(orderedZipProducer);
        return new OrderedZipSubscriber<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT>(
                downstream,
                zipper,
                orderedZipProducer
        );
    }

}
