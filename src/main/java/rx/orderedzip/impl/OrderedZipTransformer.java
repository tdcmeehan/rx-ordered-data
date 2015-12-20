package rx.orderedzip.impl;

import conductor.rx.ordered.flow.join.ZipType;
import rx.ordered.internal.util.Duple;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;

import java.util.Comparator;

/**
 * A Transformer which takes in a right hand stream and transforms the current stream into a result stream.  This
 * primarily delegates to {@link OrderedZipOperator}.
 */
public final class OrderedZipTransformer<KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> implements Observable.Transformer<LEFT_VALUE, RESULT> {
    private final Observable<RIGHT_VALUE> rightHandSide;
    private final Comparator<KEY> comparator;
    private final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction;
    private final ZipType zipType;
    private final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction;
    private final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction;

    public OrderedZipTransformer(
            final Observable<RIGHT_VALUE> rightHandSide,
            final Comparator<KEY> comparator,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final ZipType zipType
    ) {
        this.rightHandSide = rightHandSide;
        this.comparator = comparator;
        this.leftHandKeyingFunction = leftHandKeyingFunction;
        this.rightHandKeyingFunction = rightHandKeyingFunction;
        this.joinFunction = joinFunction;
        this.zipType = zipType;
    }

    @Override
    public final Observable<RESULT> call(final Observable<LEFT_VALUE> leftHandSide) {
        return Observable.just((new Duple<>(leftHandSide, rightHandSide))).lift(new OrderedZipOperator<>(
                comparator,
                leftHandKeyingFunction,
                rightHandKeyingFunction,
                joinFunction,
                zipType
        ));
    }
}
