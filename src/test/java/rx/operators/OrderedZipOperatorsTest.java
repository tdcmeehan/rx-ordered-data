package rx.operators;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import conductor.rx.ordered.flow.join.ZipType;
import org.junit.Test;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observers.TestSubscriber;
import rx.ordered.internal.util.Duple;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OrderedZipOperatorsTest {

    static final List<Duple<Integer, String>> JOIN_SIDE_1 = ImmutableList.of(
            new Duple<>(1, "one"),
            new Duple<>(2, "two"),
            new Duple<>(4, "four"),
            new Duple<>(5, "five"),
            new Duple<>(7, "seven"),
            new Duple<>(8, "eight"),
            new Duple<>(10, "ten")
    );

    static final List<Duple<Integer, String>> JOIN_SIDE_2 = ImmutableList.of(
            new Duple<>(2, "'2'"),
            new Duple<>(4, "'4'"),
            new Duple<>(6, "'6'"),
            new Duple<>(8, "'8'"),
            new Duple<>(10, "'10'")
    );

    static final Func1<Duple<Integer, String>, Integer> KEYING_FUNCTION = Duple::getFst;

    static final Func1<Duple<Integer, String>, Boolean> FILTER_ODD_VALUES = integerStringDuple -> (integerStringDuple.getFst() % 2) == 0;

    static final Func2<Duple<Integer, String>, Duple<Integer, String>, String> JOIN_FUNCTION = (integerStringDuple, integerStringDuple2) -> {
        if (integerStringDuple != null && integerStringDuple2 != null) {
            return String.format("%d - %s - %d - %s", integerStringDuple.getFst(), integerStringDuple.getSnd(), integerStringDuple2.getFst(), integerStringDuple2.getSnd());
        }
        else if (integerStringDuple == null) {
            return String.format("%d - %s", integerStringDuple2.getFst(), integerStringDuple2.getSnd());
        }
        else {
            return String.format("%d - %s", integerStringDuple.getFst(), integerStringDuple.getSnd());
        }
    };

    static final Func2<Duple<Integer, String>, Duple<Integer, String>, Duple<Integer, String>> JOIN_RETURN_LHS_FUNCTION = (integerStringDuple, integerStringDuple2) -> integerStringDuple;

    /**
     * The following are basic tests of functionality for left, inner and outer joins
     */
    @Test
    public void testFullOuterJoin() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<String>();

        Observable.from(JOIN_SIDE_1)
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(JOIN_SIDE_2),
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.OUTER
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - one",
                "2 - two - 2 - '2'",
                "4 - four - 4 - '4'",
                "5 - five",
                "6 - '6'",
                "7 - seven",
                "8 - eight - 8 - '8'",
                "10 - ten - 10 - '10'"
        );
    }

    @Test
    public void testFullOuterJoinOppositeSide() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable.from(JOIN_SIDE_2)
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(JOIN_SIDE_1),
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.OUTER
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - one",
                "2 - '2' - 2 - two",
                "4 - '4' - 4 - four",
                "5 - five",
                "6 - '6'",
                "7 - seven",
                "8 - '8' - 8 - eight",
                "10 - '10' - 10 - ten"
        );
    }

    @Test
    public void testFullOuterJoinWithBackpressure() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>(2);
        testSubscriber.requestMore(3);
        Observable.from(JOIN_SIDE_1)
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(JOIN_SIDE_2),
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.OUTER
                ))
                .subscribe(testSubscriber);

        testSubscriber.assertNoErrors();
        assertFalse(testSubscriber.isUnsubscribed());
        testSubscriber.assertNoTerminalEvent();
        testSubscriber.assertNotCompleted();

        testSubscriber.assertValueCount(5);
        testSubscriber.assertValues(
                "1 - one",
                "2 - two - 2 - '2'",
                "4 - four - 4 - '4'",
                "5 - five",
                "6 - '6'"
        );

        testSubscriber.requestMore(3);

        testSubscriber.assertValueCount(8);
        assertEquals(ImmutableList.of(
                "7 - seven",
                "8 - eight - 8 - '8'",
                "10 - ten - 10 - '10'"
        ), testSubscriber.getOnNextEvents().subList(5, 8));
    }

    @Test
    public void testLeftJoinWithInnerJoin() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable.from(JOIN_SIDE_1)
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(JOIN_SIDE_2),
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_RETURN_LHS_FUNCTION,
                        ZipType.LEFT
                ))
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(JOIN_SIDE_1),
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.INNER
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - one - 1 - one",
                "2 - two - 2 - two",
                "4 - four - 4 - four",
                "5 - five - 5 - five",
                "7 - seven - 7 - seven",
                "8 - eight - 8 - eight",
                "10 - ten - 10 - ten"
        );
    }

    @Test
    public void testLeftJoin() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable.from(JOIN_SIDE_1)
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(JOIN_SIDE_2),
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.LEFT
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - one",
                "2 - two - 2 - '2'",
                "4 - four - 4 - '4'",
                "5 - five",
                "7 - seven",
                "8 - eight - 8 - '8'",
                "10 - ten - 10 - '10'"
        );
    }

    @Test
    public void testLeftJoinOppositeSide() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable.from(JOIN_SIDE_2)
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(JOIN_SIDE_1),
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.LEFT
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "2 - '2' - 2 - two",
                "4 - '4' - 4 - four",
                "6 - '6'",
                "8 - '8' - 8 - eight",
                "10 - '10' - 10 - ten"
        );
    }

    @Test
    public void testFullInnerJoin() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable.from(JOIN_SIDE_1)
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(JOIN_SIDE_2),
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.INNER
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);

        testSubscriber.assertValueCount(4);
        testSubscriber.assertValues(
                "2 - two - 2 - '2'",
                "4 - four - 4 - '4'",
                "8 - eight - 8 - '8'",
                "10 - ten - 10 - '10'"
        );
    }

    @Test
    public void testFullInnerJoinOppositeSide() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable.from(JOIN_SIDE_2)
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(JOIN_SIDE_1),
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.INNER
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "2 - '2' - 2 - two",
                "4 - '4' - 4 - four",
                "8 - '8' - 8 - eight",
                "10 - '10' - 10 - ten"
        );
    }


    /**
     * The following are tests where one of the streams never emits an item
     */
    @Test
    public void testLeftJoinWithEmptyStream() {
        assertWithEmptyRightStream(JOIN_SIDE_1, ZipType.LEFT, FluentIterable.from(JOIN_SIDE_1).transform((input) -> JOIN_FUNCTION.call(input, null)).toArray(String.class));
        assertWithEmptyLeftStream(JOIN_SIDE_1, ZipType.LEFT);
    }

    @Test
    public void testFullOuterJoinWithEmptyStream() {
        String[] output = FluentIterable.from(JOIN_SIDE_1).transform((input) -> JOIN_FUNCTION.call(input, null)).toArray(String.class);

        assertWithEmptyRightStream(JOIN_SIDE_1, ZipType.OUTER, output);
        assertWithEmptyLeftStream(JOIN_SIDE_1, ZipType.OUTER, output);
    }

    @Test
    public void testInnerJoinWithEmptyStream() {
        assertWithEmptyRightStream(JOIN_SIDE_1, ZipType.INNER);
        assertWithEmptyLeftStream(JOIN_SIDE_1, ZipType.INNER);
    }

    /**
     * The following are tests for large data streams, which follow a slightly different path than smaller streams
     * because the entire contents of the stream don't fit in the upfront request for data and hence need to be
     * re-requested
     */
    @Test
    public void testFullOuterJoinWithLargeData() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable<Duple<Integer, String>> observable1 = generateObservable(1, 1000, "l");
        Observable<Duple<Integer, String>> observable2 = generateObservable(1, 1000, "r").filter(FILTER_ODD_VALUES);

        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.OUTER
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        assertEquals(1000, testSubscriber.getOnNextEvents().size());
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
    public void testFullOuterJoinWithLargeDataOpposite() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable<Duple<Integer, String>> observable1 = generateObservable(1, 1000, "l").filter(FILTER_ODD_VALUES);
        Observable<Duple<Integer, String>> observable2 = generateObservable(1, 1000, "r");

        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.OUTER
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        assertEquals(1000, testSubscriber.getOnNextEvents().size());
        assertEquals(ImmutableList.of(
                "1 - 1r",
                "2 - 2l - 2 - 2r",
                "3 - 3r",
                "4 - 4l - 4 - 4r",
                "5 - 5r"
        ), testSubscriber.getOnNextEvents().subList(0, 5));
        assertEquals(ImmutableList.of(
                "996 - 996l - 996 - 996r",
                "997 - 997r",
                "998 - 998l - 998 - 998r",
                "999 - 999r",
                "1000 - 1000l - 1000 - 1000r"
        ), testSubscriber.getOnNextEvents().subList(995, 1000));
    }

    @Test
    public void testLeftJoinWithLargeData() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable<Duple<Integer, String>> observable1 = generateObservable(1, 1000, "l");
        Observable<Duple<Integer, String>> observable2 = generateObservable(1, 1000, "r").filter(FILTER_ODD_VALUES);

        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.LEFT
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        assertEquals(1000, testSubscriber.getOnNextEvents().size());
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
    public void testLeftJoinWithLargeDataOpposite() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable<Duple<Integer, String>> observable1 = generateObservable(1, 1000, "l").filter(FILTER_ODD_VALUES);
        Observable<Duple<Integer, String>> observable2 = generateObservable(1, 1000, "r");

        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.LEFT
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        assertEquals(500, testSubscriber.getOnNextEvents().size());
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
    public void testInnerJoinWithLargeData() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable<Duple<Integer, String>> observable1 = generateObservable(1, 1000, "l");
        Observable<Duple<Integer, String>> observable2 = generateObservable(1, 1000, "r").filter(FILTER_ODD_VALUES);

        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.INNER
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        assertEquals(500, testSubscriber.getOnNextEvents().size());
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
    public void testInnerJoinWithLargeDataOpposite() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable<Duple<Integer, String>> observable1 = generateObservable(1, 1000, "l").filter(FILTER_ODD_VALUES);
        Observable<Duple<Integer, String>> observable2 = generateObservable(1, 1000, "r");

        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.INNER
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        assertEquals(500, testSubscriber.getOnNextEvents().size());
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
    public void testDelayedWithScheduler() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        TestScheduler testScheduler = Schedulers.test();

        Observable<Duple<Integer, String>> observable1 = generateObservable(1, 6, "l");
        Observable<Duple<Integer, String>> observable2 = Observable.concat(Observable.just(
                Observable.just(new Duple<>(1, "1r")).delay(100, TimeUnit.MILLISECONDS, testScheduler),
                Observable.just(new Duple<>(3, "3r")).delay(300, TimeUnit.MILLISECONDS, testScheduler),
                Observable.just(new Duple<>(5, "5r")).delay(600, TimeUnit.MILLISECONDS, testScheduler)
        ));

        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.LEFT
                ))
                .subscribe(testSubscriber);

        testScheduler.advanceTimeBy(101, TimeUnit.MILLISECONDS);
        assertSubscriberNotFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - 1l - 1 - 1r"
        );
        testScheduler.advanceTimeBy(301, TimeUnit.MILLISECONDS);
        assertSubscriberNotFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - 1l - 1 - 1r",
                "2 - 2l",
                "3 - 3l - 3 - 3r"
        );
        testScheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);
        assertSubscriberNotFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - 1l - 1 - 1r",
                "2 - 2l",
                "3 - 3l - 3 - 3r"
        );
        testScheduler.advanceTimeBy(600, TimeUnit.MILLISECONDS);
        assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - 1l - 1 - 1r",
                "2 - 2l",
                "3 - 3l - 3 - 3r",
                "4 - 4l",
                "5 - 5l - 5 - 5r",
                "6 - 6l"
        );
    }

    @Test
    public void testJoinWithError() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();

        Observable<Duple<Integer, String>> observable1 = generateObservable(1, 6, "l");
        Observable<Duple<Integer, String>> observable2 = Observable.concat(Observable.just(
                Observable.just(new Duple<>(1, "1r")),
                Observable.<Duple<Integer, String>>error(new IllegalStateException("Holy exception batman!")),
                Observable.just(new Duple<>(5, "5r"))
        ));

        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        ZipType.LEFT
                ))
                .subscribe(testSubscriber);

        testSubscriber.assertError(IllegalStateException.class);
        testSubscriber.assertTerminalEvent();
        testSubscriber.assertUnsubscribed();
        testSubscriber.assertValues(
                "1 - 1l - 1 - 1r"
        );
    }

    @Test
    public void testJoinWithErrorInJoinFunction() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();

        Observable<Duple<Integer, String>> observable1 = generateObservable(1, 6, "l");
        Observable<Duple<Integer, String>> observable2 = generateObservable(1, 6, "r");

        observable1
                .compose(OrderedZipOperators.zipWith(
                        observable2,
                        Ordering.<Integer>natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        new Func2<Duple<Integer,String>, Duple<Integer,String>, String>() {
                            @Override
                            public String call(Duple<Integer, String> leftSide, Duple<Integer, String> rightSide) {
                                if (leftSide != null && leftSide.getFst() == 3) {
                                    throw new IllegalStateException("Holy exception batman!");
                                }
                                return JOIN_FUNCTION.call(leftSide, rightSide);
                            }
                        },
                        ZipType.LEFT
                ))
                .subscribe(testSubscriber);

        testSubscriber.assertError(IllegalStateException.class);
        testSubscriber.assertTerminalEvent();
        testSubscriber.assertUnsubscribed();
        testSubscriber.assertValues(
                "1 - 1l - 1 - 1r",
                "2 - 2l - 2 - 2r"
        );
    }

    static Observable<Duple<Integer, String>> generateObservable(final int start, final int end, final String suffix) {
        return Observable.range(start, end)
                .map((integer) -> new Duple<>(integer, integer+suffix));
    }

    static Observable<Duple<Integer, String>> generateObservable(final int start, final int end, final String suffix, final Scheduler scheduler) {
        return Observable.range(start, end, scheduler)
                .map((integer) -> new Duple<>(integer, integer+suffix));
    }

    static void assertSubscriberFinished(final TestSubscriber testSubscriber) {
        testSubscriber.assertNoErrors();
        testSubscriber.assertUnsubscribed();
        testSubscriber.assertTerminalEvent();
    }

    static void assertSubscriberNotFinished(final TestSubscriber testSubscriber) {
        testSubscriber.assertNoErrors();
        assertFalse(testSubscriber.isUnsubscribed());
        testSubscriber.assertNoTerminalEvent();
    }

    static void assertWithEmptyRightStream(final List<Duple<Integer, String>> joinSide, final ZipType zipType, final String ... output) {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable.from(joinSide)
                .compose(OrderedZipOperators.zipWith(
                        Observable.empty(),
                        Ordering.natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        zipType
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);

        testSubscriber.assertValueCount(output.length);
        testSubscriber.assertValues(output);
    }

    static void assertWithEmptyLeftStream(final List<Duple<Integer, String>> joinSide, final ZipType zipType, final String ... output) {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable.<Duple<Integer, String>>empty()
                .compose(OrderedZipOperators.zipWith(
                        Observable.from(joinSide),
                        Ordering.natural(),
                        KEYING_FUNCTION,
                        KEYING_FUNCTION,
                        JOIN_FUNCTION,
                        zipType
                ))
                .subscribe(testSubscriber);

        assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(output);
    }
}