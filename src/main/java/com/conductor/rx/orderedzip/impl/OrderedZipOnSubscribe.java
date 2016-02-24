package com.conductor.rx.orderedzip.impl;

import com.conductor.rx.ordered.flow.join.ZipType;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.functions.Func2;

import java.util.Comparator;

/**
 * An implementation of {@link rx.Observable.OnSubscribe} which will set up and initiate an instance of a {@link Zipper}.
 * Use this with {@link Observable#create(Observable.OnSubscribe)} to create a Zipped Observable.
 */
public class OrderedZipOnSubscribe<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> implements Observable.OnSubscribe<RESULT> {

    private final Observable<LEFT_VALUE> leftHandSide;
    private final Observable<RIGHT_VALUE> rightHandSide;
    private final Comparator<KEY> ordering;
    private final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction;
    private final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction;
    private final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction;
    private final ZipType zipType;

    private OrderedZipOnSubscribe(
            final Observable<LEFT_VALUE> leftHandSide,
            final Observable<RIGHT_VALUE> rightHandSide,
            final Comparator<KEY> ordering,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final ZipType zipType
    ) {
        this.leftHandSide = leftHandSide;
        this.rightHandSide = rightHandSide;
        this.ordering = ordering;
        this.leftHandKeyingFunction = leftHandKeyingFunction;
        this.rightHandKeyingFunction = rightHandKeyingFunction;
        this.joinFunction = joinFunction;
        this.zipType = zipType;
    }

    public static <KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> Observable.OnSubscribe<RESULT> create(
            final Observable<LEFT_VALUE> leftHandSide,
            final Observable<RIGHT_VALUE> rightHandSide,
            final Comparator<KEY> ordering,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final ZipType zipType
    ) {
        return new OrderedZipOnSubscribe<>(
                leftHandSide, rightHandSide, ordering, leftHandKeyingFunction, rightHandKeyingFunction, joinFunction, zipType
        );
    }

    @Override
    public void call(final Subscriber<? super RESULT> downstream) {
        final Zipper<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> zipper = new Zipper<>(
                downstream,
                ordering,
                leftHandKeyingFunction,
                rightHandKeyingFunction,
                joinFunction,
                zipType
        );
        final OrderedZipProducer<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> orderedZipProducer = new OrderedZipProducer<>(zipper);
        downstream.setProducer(orderedZipProducer);

        zipper.start(leftHandSide, rightHandSide, orderedZipProducer.requested);
    }
}
