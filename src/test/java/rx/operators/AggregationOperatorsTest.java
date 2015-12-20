package rx.operators;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.ordered.internal.util.Duple;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class AggregationOperatorsTest {

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

}