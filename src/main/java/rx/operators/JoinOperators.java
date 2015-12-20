package rx.operators;

import conductor.rx.ordered.flow.join.JoinType;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.joins.impl.JoinTransformer;

import java.util.Comparator;

/**
 * Created by tmeehan on 9/1/15.
 */
public class JoinOperators {
    private JoinOperators() {}

    /**
     * Returns a Transformer which performs a SQL join on sorted streams.  There are three join types supported: full
     * inner, full outer, and left.  In case of duplicates, as in SQL, a product will be applied on the output.  Used
     * with the {@link rx.Observable#compose} method to provide method chaining.
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
     * @param joinType the type of join to perform -- left, outer, and inner
     * @return the joined observable
     */
    public static final <KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> Observable.Transformer<LEFT_VALUE, RESULT> joinWith(
            final Observable<RIGHT_VALUE> rightHandSide,
            final Comparator<KEY> ordering,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final JoinType joinType
    ) {
        return new JoinTransformer<>(
                rightHandSide,
                ordering,
                leftHandKeyingFunction,
                rightHandKeyingFunction,
                joinFunction,
                joinType
        );
    }

    /**
     * Returns a Transformer which performs a SQL join on sorted streams.  There are three join types supported: full
     * inner, full outer, and left.  In case of duplicates, as in SQL, a product will be applied on the output.  Used
     * with the {@link rx.Observable#compose} method to provide method chaining.
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
     * @param joinType the type of join to perform -- left, outer, and inner
     * @return the joined observable
     */
    public static final <KEY, LEFT_VALUE, RIGHT_VALUE, RESULT> Observable<RESULT> join(
            final Observable<LEFT_VALUE> leftHandSide,
            final Observable<RIGHT_VALUE> rightHandSide,
            final Comparator<KEY> ordering,
            final Func1<? super LEFT_VALUE, KEY> leftHandKeyingFunction,
            final Func1<? super RIGHT_VALUE, KEY> rightHandKeyingFunction,
            final Func2<LEFT_VALUE, RIGHT_VALUE, RESULT> joinFunction,
            final JoinType joinType
    ) {
        return leftHandSide.compose(joinWith(
                rightHandSide,
                ordering,
                leftHandKeyingFunction,
                rightHandKeyingFunction,
                joinFunction,
                joinType
        ));
    }
}
