package rx.operators;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.List;

/**
 * An operator which groups the incoming Observable into emissions of lists of values
 *
 * Created by tmeehan on 12/12/15.
 */
/**
 * An operator which groups the incoming Observable into emissions of lists of values.
 *
 * Created by tmeehan on 12/12/15.
 */
class BufferByKeyOperator<T, K> implements Observable.Operator<List<T>, T> {
    private final Func1<? super T, K> keyFunction;
    private final Func1<List<?>, List<?>> arrayCopyStrategy;

    private static final Func1<List<?>, List<?>> MUTABLE_STRATEGY = Lists::newArrayList;

    private static final Func1<List<?>, List<?>> IMMUTABLE_STRATEGY = ImmutableList::copyOf;

    private BufferByKeyOperator(final Func1<? super T, K> keyFunction, final Func1<List<?>, List<?>> arrayCopyStrategy) {
        this.keyFunction = keyFunction;
        this.arrayCopyStrategy = arrayCopyStrategy;
    }

    public static <T, K> BufferByKeyOperator<T, K> toMutableList(final Func1<? super T, K> keyFunction) {
        return new BufferByKeyOperator<>(keyFunction, MUTABLE_STRATEGY);
    }

    public static <T, K> BufferByKeyOperator<T, K> toImmutableList(final Func1<? super T, K> keyFunction) {
        return new BufferByKeyOperator<>(keyFunction, IMMUTABLE_STRATEGY);
    }

    @Override
    public Subscriber<? super T> call(final Subscriber<? super List<T>> downstreamSubscriber) {
        return new Subscriber<T>(downstreamSubscriber) {
            private final List<T> objects = Lists.newArrayList();

            @Override
            @SuppressWarnings("unchecked")
            public void onCompleted() {
                if (!objects.isEmpty()) {
                    downstreamSubscriber.onNext((List<T>) arrayCopyStrategy.call(objects));
                    objects.clear();
                }
                downstreamSubscriber.onCompleted();
            }

            @Override
            public void onError(final Throwable e) {
                downstreamSubscriber.onError(e);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void onNext(final T t) {
                if (objects.isEmpty() || keyFunction.call(objects.get(0)).equals(keyFunction.call(t))) {
                    objects.add(t);
                    request(1);
                }
                else {
                    downstreamSubscriber.onNext((List<T>) arrayCopyStrategy.call(objects));
                    objects.clear();
                    objects.add(t);
                }
            }
        };
    }
}


