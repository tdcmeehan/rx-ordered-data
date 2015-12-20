package rx.operators;

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
public class AggregationOperators {

    public static final Func1<List<?>, Integer> IDENTITY_HASH_FUNCTION = new Func1<List<?>, Integer>() {
        @Override
        public Integer call(final List<?> ts) {
            return System.identityHashCode(ts);
        }
    };

    /**
     * Buffers a stream until the keying function returns a different value, returning a an Observable of Lists of
     * the desired type.  Buffering occurs one-at-a-time; whenever the key value has changed, the previous key's buffer
     * is emitted immediately.
     *
     * @param keyFunction a function to determine the grouped sequence
     * @return
     */
    public static <T, K> Observable.Transformer<? super T, List<T>> bufferByKey(Func1<? super T, K> keyFunction) {
        Preconditions.checkNotNull(keyFunction);
        return new BufferByKeyTransformer<>(keyFunction);
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
                .<AGGREGATE>map(aggregateFunction);
    }

    private static class BufferByKeyTransformer<T, K> implements Observable.Transformer<T, List<T>> {
        private final Func1<? super T, K> keyFunction;

        public BufferByKeyTransformer(final Func1<? super T, K> keyFunction) {
            this.keyFunction = keyFunction;
        }

        @Override
        public Observable<List<T>> call(final Observable<T> observableToAggregate) {
            return observableToAggregate.lift(BufferByKeyOperator.toImmutableList(keyFunction));
        }

    }

}