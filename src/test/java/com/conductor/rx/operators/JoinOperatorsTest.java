package com.conductor.rx.operators;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.conductor.rx.ordered.flow.join.JoinType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observers.TestSubscriber;
import com.conductor.rx.ordered.internal.util.Duple;

import java.util.List;

import static org.junit.Assert.assertNull;

public class JoinOperatorsTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private static final Func1<Integer, Integer> IDENTITY = integer -> integer;

    static final List<Duple<Integer, String>> JOIN_SIDE_1 = ImmutableList.of(
            new Duple<>(1, "one"),
            new Duple<>(2, "two"),
            new Duple<>(2, "2s"),
            new Duple<>(4, "four"),
            new Duple<>(4, "4s"),
            new Duple<>(5, "five")
    );

    static final List<Duple<Integer, String>> JOIN_SIDE_2 = ImmutableList.of(
            new Duple<>(1, ""),
            new Duple<>(2, "'2'"),
            new Duple<>(4, "'4'"),
            new Duple<>(4, "'4s'"),
            new Duple<>(4, "'four'"),
            new Duple<>(6, "'6'"),
            new Duple<>(8, "'8'"),
            new Duple<>(10, "'10'")
    );

    @Test
    public void testSqlInnerJoin() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<String>();

        Observable.from(JOIN_SIDE_1)
                .compose(JoinOperators.joinWith(
                        Observable.from(JOIN_SIDE_2),
                        Ordering.<Integer>natural(),
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.JOIN_FUNCTION,
                        JoinType.FULL_INNER
                ))
                .subscribe(testSubscriber);

        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - one - 1 - ",
                "2 - two - 2 - '2'",
                "2 - 2s - 2 - '2'",
                "4 - four - 4 - '4'",
                "4 - four - 4 - '4s'",
                "4 - four - 4 - 'four'",
                "4 - 4s - 4 - '4'",
                "4 - 4s - 4 - '4s'",
                "4 - 4s - 4 - 'four'"
        );
    }

    @Test
    public void testSqlFullOuterJoin() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<String>();

        Observable.from(JOIN_SIDE_1)
                .compose(JoinOperators.joinWith(
                        Observable.from(JOIN_SIDE_2),
                        Ordering.<Integer>natural(),
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.JOIN_FUNCTION,
                        JoinType.FULL_OUTER
                ))
                .subscribe(testSubscriber);

        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - one - 1 - ",
                "2 - two - 2 - '2'",
                "2 - 2s - 2 - '2'",
                "4 - four - 4 - '4'",
                "4 - four - 4 - '4s'",
                "4 - four - 4 - 'four'",
                "4 - 4s - 4 - '4'",
                "4 - 4s - 4 - '4s'",
                "4 - 4s - 4 - 'four'",
                "5 - five",
                "6 - '6'",
                "8 - '8'",
                "10 - '10'"
        );
    }

    @Test
    public void testSqlLeftJoin() throws Exception {
        TestSubscriber<String> testSubscriber = new TestSubscriber<String>();

        Observable.from(JOIN_SIDE_1)
                .compose(JoinOperators.joinWith(
                        Observable.from(JOIN_SIDE_2),
                        Ordering.<Integer>natural(),
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.KEYING_FUNCTION,
                        OrderedZipOperatorsTest.JOIN_FUNCTION,
                        JoinType.LEFT
                ))
                .subscribe(testSubscriber);

        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(
                "1 - one - 1 - ",
                "2 - two - 2 - '2'",
                "2 - 2s - 2 - '2'",
                "4 - four - 4 - '4'",
                "4 - four - 4 - '4s'",
                "4 - four - 4 - 'four'",
                "4 - 4s - 4 - '4'",
                "4 - 4s - 4 - '4s'",
                "4 - 4s - 4 - 'four'",
                "5 - five"
        );
    }

    @Test
    public void testLeftJoinWithRightSideEmpty() {
        TestSubscriber<Integer> testSubscriber = new TestSubscriber<Integer>();

        Observable.just(1, 2, 2, 3)
                .compose(JoinOperators.joinWith(
                        Observable.<Integer>empty(),
                        Ordering.<Integer>natural(),
                        IDENTITY,
                        IDENTITY,
                        new Func2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer left, Integer right) {
                                assertNull("Unexpected join", right);
                                return left;
                            }
                        },
                        JoinType.LEFT
                ))
                .subscribe(testSubscriber);

        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(1, 2, 2, 3);
    }

    @Test
    public void testOuterJoinWithRightSideEmpty() {
        TestSubscriber<Integer> testSubscriber = new TestSubscriber<Integer>();

        Observable.just(1, 2, 2, 3)
                .compose(JoinOperators.joinWith(
                        Observable.<Integer>empty(),
                        Ordering.<Integer>natural(),
                        IDENTITY,
                        IDENTITY,
                        new Func2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer left, Integer right) {
                                assertNull("Unexpected join", right);
                                return left;
                            }
                        },
                        JoinType.FULL_OUTER
                ))
                .subscribe(testSubscriber);

        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(1, 2, 2, 3);
    }

    @Test
    public void testOuterJoinWithLeftSideEmpty() {
        TestSubscriber<Integer> testSubscriber = new TestSubscriber<Integer>();

        Observable.<Integer>empty()
                .compose(JoinOperators.joinWith(
                        Observable.just(1, 2, 2, 3),
                        Ordering.<Integer>natural(),
                        IDENTITY,
                        IDENTITY,
                        new Func2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer left, Integer right) {
                                assertNull("Unexpected join", left);
                                return right;
                            }
                        },
                        JoinType.FULL_OUTER
                ))
                .subscribe(testSubscriber);

        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);
        testSubscriber.assertValues(1, 2, 2, 3);
    }

    @Test
    public void testInnerJoinWithASideEmpty() {
        TestSubscriber<Integer> testSubscriber = new TestSubscriber<Integer>();

        Observable.just(1, 2, 2, 3)
                .compose(JoinOperators.joinWith(
                        Observable.<Integer>empty(),
                        Ordering.<Integer>natural(),
                        IDENTITY,
                        IDENTITY,
                        new Func2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer integer, Integer integer2) {
                                return integer;
                            }
                        },
                        JoinType.FULL_INNER
                ))
                .subscribe(testSubscriber);

        OrderedZipOperatorsTest.assertSubscriberFinished(testSubscriber);
        testSubscriber.assertNoValues();
    }

    @Test
    public void testStaticJoin() {
        Observable<Integer> side1 = Observable.range(1, 10).filter((item) -> item % 2 == 0);
        Observable<Integer> side2 = Observable.range(1, 10);

        TestSubscriber<Integer> testSubscriber = new TestSubscriber<>();
        Observable<Integer> joinedObs =
                JoinOperators.join(side1, side2, Ordering.natural(), Integer::intValue, Integer::intValue, (intSide1, intSide2) -> intSide1, JoinType.LEFT);
        joinedObs.subscribe(testSubscriber);

        testSubscriber.assertValues(2, 4, 6, 8, 10);
    }
}