package rx.operators;


import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.ordered.internal.util.Duple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AggregationOperatorsTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testBufferByKey() throws Exception {
        int lowerBound = 1;
        int upperBound = 6;
        List<List<Integer>> output = Observable.range(lowerBound, upperBound)
        .compose(AggregationOperators.<Integer, Integer>bufferByKey((integer) -> Math.round(integer / 2)))
        .toList().toBlocking().single(); // Get the entire contents

        assertEquals(4, output.size());
        assertEquals(ImmutableList.of(1), output.get(0));
        assertEquals(ImmutableList.of(2,3), output.get(1));
        assertEquals(ImmutableList.of(4,5), output.get(2));
        assertEquals(ImmutableList.of(6), output.get(3));
    }

    @Test
    public void testBufferByKeyIsImmutable() {
        List<Integer> integers = Observable.range(1, 10).compose(AggregationOperators.<Integer, Integer>bufferByKey((integer) -> integer / 10)).toBlocking().first();
        exception.expect(UnsupportedOperationException.class);
        integers.add(1212);
    }

    @Test
    public void testBufferByKeyMutableIsNotImmutable() {
        List<Integer> integers = Observable.range(1, 10).compose(AggregationOperators.<Integer, Integer>bufferByKey((integer) -> integer / 10, ArrayList::new)).toBlocking().first();
        integers.add(1212);
    }

    @Test
    public void testAggregateListIsImmutable() {
        exception.expect(UnsupportedOperationException.class);
        String reduction = Observable.range(1, 10)
                .compose(AggregationOperators.<Integer, Integer, String>aggregate((integer) -> integer / 10, (list) -> {
                    list.remove(list.size() - 1);
                    return Joiner.on(" ").join(list);
                }))
                .toBlocking().first();
    }

    @Test
    public void testAggregateListMutableIsMutable() {
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        Observable.range(1, 3)
                .compose(AggregationOperators.<Integer, Integer, String>aggregate((integer) -> integer / 10, ArrayList::new, (list) -> {
                    list.remove(list.size() - 1);
                    return Joiner.on(" ").join(list);
                }))
                .subscribe(testSubscriber);
        testSubscriber.assertValues("1 2");
    }

    @Test
    public void testBufferByKeyReallyAggregatesTheWholeGrouping() throws InterruptedException {
        final List<Duple<Integer, Integer>> input = ImmutableList.of(
                new Duple<>(1, 2),
                new Duple<>(2, 2), new Duple<>(2, 3),
                new Duple<>(3, 2), new Duple<>(3, 3), new Duple<>(3, 4)
        );

        TestSubscriber<List<Duple<Integer, Integer>>> testSubscriber = new TestSubscriber<>();
        Observable.from(input)
                .compose(AggregationOperators.<Duple<Integer, Integer>, Integer>bufferByKey(Duple::getFst))
                .map(Lists::newArrayList)
                .subscribe(testSubscriber);

        List<List<Duple<Integer, Integer>>> output = testSubscriber.getOnNextEvents();
        assertEquals(3, output.size());
        assertEquals(input.subList(0, 1), output.get(0));
        assertEquals(input.subList(1, 3), output.get(1));
        assertEquals(input.subList(3, 6), output.get(2));

        testSubscriber.assertNoErrors();
        testSubscriber.assertTerminalEvent();
        testSubscriber.assertUnsubscribed();

    }

    @Test
    public void testAggregate() {
        final List<Duple<Integer, Integer>> input = ImmutableList.of(
                new Duple<>(1, 2),
                new Duple<>(2, 2), new Duple<>(2, 3),
                new Duple<>(3, 2), new Duple<>(3, 3), new Duple<>(3, 4)
        );

        TestSubscriber<Integer> testSubscriber = new TestSubscriber<>();
        Observable.from(input)
                .compose(AggregationOperators.aggregate(Duple::getFst, (duples) -> {
                    int sum = 0;
                    for (Duple<Integer, Integer> element : duples) {
                        sum += (element.getFst() + element.getSnd());
                    }
                    return sum;
                }))
                .subscribe(testSubscriber);

        testSubscriber.assertNoErrors();
        testSubscriber.assertTerminalEvent();
        testSubscriber.assertUnsubscribed();
        testSubscriber.assertValues(3, 9, 18);
    }

    @Test
    public void testOnErrorHandling() {
        Observable<List<Integer>> integerObservable = Observable.range(0, 5).compose(AggregationOperators.<Integer, Integer>bufferByKey((integer) -> {
            if (integer < 3) {
                return integer / 2;
            }
            throw new RuntimeException("unexpected error");
        }));
        TestSubscriber<List<Integer>> testSubscriber = new TestSubscriber<>();
        integerObservable.subscribe(testSubscriber);

        testSubscriber.assertTerminalEvent();
        testSubscriber.assertUnsubscribed();
        testSubscriber.assertValues(Arrays.asList(0, 1));
        testSubscriber.assertError(RuntimeException.class);
    }

}