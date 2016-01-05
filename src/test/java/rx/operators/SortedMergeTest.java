package rx.operators;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.MissingBackpressureException;
import rx.internal.util.RxRingBuffer;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Adapted from code written by David Karnok &lt;akarnokd@gmail.com&gt;:
 *  {@link <a href="https://gist.github.com/akarnokd/c86a89738199bbb37348">SortedMergeTest.java</a>}
 */
public class SortedMergeTest {

    @Test
    public void testSymmetricMerge() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.just(2, 4, 6, 8);

        final TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8);
    }

    @Test
    public void testAsymmetricMerge() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.just(2, 4);

        final TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 4, 5, 7);
    }

    @Test
    public void testSymmetricMergeAsync() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7).observeOn(Schedulers.computation());
        final Observable<? extends Integer> o2 = Observable.just(2, 4, 6, 8).observeOn(Schedulers.computation());

        final TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.awaitTerminalEvent(1, TimeUnit.SECONDS);
        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8);
    }

    @Test
    public void testAsymmetricMergeAsync() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7).observeOn(Schedulers.computation());
        final Observable<? extends Integer> o2 = Observable.just(2, 4).observeOn(Schedulers.computation());

        final TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.awaitTerminalEvent(1, TimeUnit.SECONDS);
        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2, 3, 4, 5, 7);
    }
    @Test
    public void testEmptyEmpty() {
        final Observable<? extends Integer> o1 = Observable.empty();
        final Observable<? extends Integer> o2 = Observable.empty();

        final TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertNoValues();
    }

    @Test
    public void testEmptySomething1() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.empty();

        final TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 3, 5, 7);
    }
    @Test
    public void testEmptySomething2() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.empty();

        final TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        SortedMerge.create(ImmutableList.of(o2, o1)).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 3, 5, 7);
    }

    @Test
    public void testErrorInMiddle() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.just(2).concatWith(Observable.<Integer>error(new Exception()));

        final TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.assertError(Exception.class);
        ts.assertNotCompleted();
        ts.assertValues(1, 2);
    }

    @Test
    public void testErrorImmediately() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.<Integer>error(new Exception());

        final TestSubscriber<Integer> ts = new TestSubscriber<Integer>();
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.assertError(Exception.class);
        ts.assertNotCompleted();
        ts.assertNoValues();
    }

    @Test
    public void testTake() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.just(2, 4, 6, 8);

        final TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(ImmutableList.of(o1, o2)).take(2).subscribe(ts);

        ts.assertNoErrors();
        ts.assertCompleted();
        ts.assertValues(1, 2);

    }
    @Test
    public void testBackpressure() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.just(2, 4, 6, 8);

        final TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.requestMore(2);

        ts.assertNoErrors();
        ts.assertNotCompleted();
        ts.assertValues(1, 2);
    }

    @Test
    public void testBackpressureException() {
        final Observable<? extends Integer> o1 = Observable.create(new Observable.OnSubscribe<Integer>() {
            @Override
            public void call(Subscriber<? super Integer> subscriber) {
                subscriber.onStart();
                for (int i = 1; i < RxRingBuffer.SIZE + 10; i++) {
                    subscriber.onNext(i);
                }
                subscriber.onCompleted();
            }
        });
        final Observable<? extends Integer> o2 = Observable.just(2, 4, 6, 8);

        final TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.assertNoValues();
        ts.assertError(MissingBackpressureException.class);
    }

    @Test
    public void testBackpressureExceptionOpposite() {
        final Observable<? extends Integer> o2 = Observable.create(new Observable.OnSubscribe<Integer>() {
            @Override
            public void call(Subscriber<? super Integer> subscriber) {
                subscriber.onStart();
                for (int i = 1; i < RxRingBuffer.SIZE + 10; i++) {
                    subscriber.onNext(i);
                }
                subscriber.unsubscribe();
                subscriber.onCompleted();
            }
        });
        final Observable<? extends Integer> o1 = Observable.just(2, 4, 6, 8);

        final TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        SortedMerge.create(ImmutableList.of(o1, o2)).subscribe(ts);

        ts.assertNoValues();
        ts.assertError(MissingBackpressureException.class);
    }

    @Test
    public void testAlreadyUnsubscribed() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.just(2, 4, 6, 8);

        final TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        ts.unsubscribe();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        ts.assertNoValues();
    }
    @Test
    public void testChildUnsubscribed() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.create(new Observable.OnSubscribe<Integer>() {
            @Override
            public void call(Subscriber<? super Integer> subscriber) {
                subscriber.onStart();
                subscriber.onNext(2);
                subscriber.onNext(4);
                subscriber.unsubscribe();
                subscriber.onNext(6);
                subscriber.onNext(8);
                subscriber.onCompleted();
            }
        });

        final TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        System.out.println(ts.getOnNextEvents());
        ts.assertValues(1, 2, 3, 4, 5, 7);
    }

    @Test
    public void testIllegalStateExceptionInChildEvent() {
        final Observable<? extends Integer> o1 = Observable.just(1, 3, 5, 7);
        final Observable<? extends Integer> o2 = Observable.just(2, 4).concatWith(Observable.error(new IllegalStateException("BOO")));

        final TestSubscriber<Integer> ts = new TestSubscriber<>();
        SortedMerge.create(Arrays.asList(o1, o2)).subscribe(ts);

        System.out.println(ts.getOnNextEvents());
        ts.assertValues(1, 2, 3, 4);
    }


}
