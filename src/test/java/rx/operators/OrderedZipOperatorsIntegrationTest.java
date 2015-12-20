package rx.operators;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import conductor.rx.ordered.flow.join.ZipType;
import org.junit.Test;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observers.TestSubscriber;
import rx.ordered.internal.util.Duple;
import rx.schedulers.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class OrderedZipOperatorsIntegrationTest {

    private static final int MAX_RUNS = 1000;

    @Test
    public void testJoinOnTwoThreads() throws Exception {
        // Run this repeatedly to attempt to unearth random threading issues (in case any crop up in the future)
        for (int i = 0; i < MAX_RUNS; i++) {
            assertJoinInTwoSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.computation()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.computation()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION
            );
            assertJoinInTwoSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.io()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.io()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION
            );
            assertJoinInTwoSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.trampoline()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.trampoline()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION
            );
            assertJoinInTwoSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.io()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.computation()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION
            );
            assertJoinInTwoSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.trampoline()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.computation()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION
            );
        }
    }

    @Test
    public void testJoinOnThreeSchedulers() throws Exception {
        // Run this repeatedly to attempt to unearth random threading issues (in case any crop up in the future)
        for (int i = 0; i < MAX_RUNS; i++) {
            assertJoinInThreeSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.computation()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.computation()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION,
                    Schedulers.computation()
            );
            assertJoinInThreeSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.io()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.io()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION,
                    Schedulers.io()
            );
            assertJoinInThreeSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.trampoline()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.trampoline()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION,
                    Schedulers.trampoline()
            );
            assertJoinInThreeSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.io()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.computation()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION,
                    Schedulers.trampoline()
            );
            assertJoinInThreeSchedulers(
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.trampoline()),
                    OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.computation()),
                    OrderedZipOperatorsTest.KEYING_FUNCTION, OrderedZipOperatorsTest.JOIN_FUNCTION,
                    Schedulers.io()
            );
        }
    }

    private <INPUT, OUTPUT> void assertJoinInTwoSchedulers(
            Observable<INPUT> observable1,
            Observable<INPUT> observable2,
            Func1<INPUT, Integer> keyingFunction,
            Func2<INPUT, INPUT, OUTPUT> joinFunction
    ) {
        assertJoinInThreeSchedulers(observable1, observable2, keyingFunction, joinFunction, null);
    }

    private <INPUT, OUTPUT> void assertJoinInThreeSchedulers(
            Observable<INPUT> observable1,
            Observable<INPUT> observable2,
            Func1<INPUT, Integer> keyingFunction,
            Func2<INPUT, INPUT, OUTPUT> joinFunction,
            Scheduler scheduler) {
        TestSubscriber<OUTPUT> testSubscriber = new TestSubscriber<OUTPUT>();

        Observable<OUTPUT> joinedObservable = observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        keyingFunction,
                        keyingFunction,
                        joinFunction,
                        ZipType.INNER
                ));

        if (scheduler != null) {
            joinedObservable = joinedObservable.subscribeOn(scheduler);
        }

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        joinedObservable.doOnUnsubscribe(new Action0() {
            @Override
            public void call() {
                countDownLatch.countDown();
            }
        }).subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        try {
            countDownLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);

        testSubscriber.assertValueCount(1000);
        assertEquals(ImmutableList.of(
                "1 - 1l - 1 - 1r",
                "2 - 2l - 2 - 2r",
                "3 - 3l - 3 - 3r",
                "4 - 4l - 4 - 4r",
                "5 - 5l - 5 - 5r"
        ), testSubscriber.getOnNextEvents().subList(0, 5));
        assertEquals(ImmutableList.of(
                "996 - 996l - 996 - 996r",
                "997 - 997l - 997 - 997r",
                "998 - 998l - 998 - 998r",
                "999 - 999l - 999 - 999r",
                "1000 - 1000l - 1000 - 1000r"
        ), testSubscriber.getOnNextEvents().subList(995, 1000));
    }

    @Test
    public void testJoinOneSideFaster() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<String>();

        Observable<Duple<Integer, String>> observable1 = OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.io());
        Observable<Duple<Integer, String>> observable2 = OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.io())
                .filter(OrderedZipOperatorsTest.FILTER_ODD_VALUES)
                .delay(500, TimeUnit.MILLISECONDS);
        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.JOIN_FUNCTION,
                        ZipType.LEFT
                ))
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);

        testSubscriber.assertValueCount(1000);
        assertEquals(ImmutableList.of(
                "1 - 1l",
                "2 - 2l - 2 - 2r",
                "3 - 3l",
                "4 - 4l - 4 - 4r",
                "5 - 5l"
        ), testSubscriber.getOnNextEvents().subList(0, 5));
        assertEquals(ImmutableList.of(
                "996 - 996l - 996 - 996r",
                "997 - 997l",
                "998 - 998l - 998 - 998r",
                "999 - 999l",
                "1000 - 1000l - 1000 - 1000r"
        ), testSubscriber.getOnNextEvents().subList(995, 1000));
    }

    @Test
    public void testJoinOtherSideFaster() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<String>();

        Observable<Duple<Integer, String>> observable1 = OrderedZipOperatorsTest.generateObservable(1, 1000, "l", Schedulers.io())
                .filter(OrderedZipOperatorsTest.FILTER_ODD_VALUES)
                .delay(500, TimeUnit.MILLISECONDS);
        Observable<Duple<Integer, String>> observable2 = OrderedZipOperatorsTest.generateObservable(1, 1000, "r", Schedulers.io());
        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.JOIN_FUNCTION,
                        ZipType.INNER
                ))
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);

        testSubscriber.assertValueCount(500);
        assertEquals(ImmutableList.of(
                "2 - 2l - 2 - 2r",
                "4 - 4l - 4 - 4r",
                "6 - 6l - 6 - 6r",
                "8 - 8l - 8 - 8r",
                "10 - 10l - 10 - 10r"
        ), testSubscriber.getOnNextEvents().subList(0, 5));
        assertEquals(ImmutableList.of(
                "992 - 992l - 992 - 992r",
                "994 - 994l - 994 - 994r",
                "996 - 996l - 996 - 996r",
                "998 - 998l - 998 - 998r",
                "1000 - 1000l - 1000 - 1000r"
        ), testSubscriber.getOnNextEvents().subList(495, 500));
    }

    @Test
    public void testJoinOnTwoThreadsEmptyLeftStream() throws Exception {
        // Run this repeatedly to attempt to unearth random threading issues (in case any crop up in the future)
        for (int i = 0; i < MAX_RUNS; i++) {
            TestSubscriber<String> testSubscriber = new TestSubscriber<String>();
            Observable<Duple<Integer, String>> observable1 = Observable.just(new Duple<>(1, "1")).subscribeOn(Schedulers.io());
            Observable<Duple<Integer, String>> observable2 = Observable.<Duple<Integer, String>>empty().subscribeOn(Schedulers.io());

            assertOnEmptyStreams(testSubscriber, observable1, observable2);
        }
    }

    @Test
    public void testJoinOnTwoThreadsEmptyRightStream() throws Exception {
        // Run this repeatedly to attempt to unearth random threading issues (in case any crop up in the future)
        for (int i = 0; i < MAX_RUNS; i++) {
            TestSubscriber<String> testSubscriber = new TestSubscriber<String>();
            Observable<Duple<Integer, String>> observable1 = Observable.<Duple<Integer, String>>empty().subscribeOn(Schedulers.io());
            Observable<Duple<Integer, String>> observable2 = Observable.just(new Duple<>(1, "1")).subscribeOn(Schedulers.io());

            assertOnEmptyStreams(testSubscriber, observable1, observable2);
        }
    }

    private void assertOnEmptyStreams(TestSubscriber<String> testSubscriber, Observable<Duple<Integer, String>> observable1, Observable<Duple<Integer, String>> observable2) throws InterruptedException {
        Observable<String> joinedObservable = OrderedZipOperators.zip(
                observable1,
                observable2,
                Ordering.<Integer>natural(),
                OrderedZipOperatorsTest.KEYING_FUNCTION,
                OrderedZipOperatorsTest.KEYING_FUNCTION,
                OrderedZipOperatorsTest.JOIN_FUNCTION,
                ZipType.OUTER
        );

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        joinedObservable.doOnUnsubscribe(new Action0() {
            @Override
            public void call() {
                countDownLatch.countDown();
            }
        }).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        countDownLatch.await(10, TimeUnit.SECONDS);

        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValueCount(1);
    }

}