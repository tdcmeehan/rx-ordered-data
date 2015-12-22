rx-ordered-data
===============

[![Build Status](https://travis-ci.org/Conductor/rx-ordered-data.svg?branch=master)](https://travis-ci.org/Conductor/rx-ordered-data) [![Coverage Status](https://coveralls.io/repos/Conductor/rx-ordered-data/badge.svg?branch=master&service=github)](https://coveralls.io/github/Conductor/rx-ordered-data?branch=master)


RxJava utilities for working with pre-sorted data:

* [`AggregationOperators.bufferByKey`](http://conductor.github.io/rx-ordered-data/javadoc/rx/operators/AggregationOperators.html#bufferByKey-rx.functions.Func1-) - bounded buffering that emits the buffer when the key value has changed
* [`AggregationOperators.aggregate`](http://conductor.github.io/rx-ordered-data/javadoc/rx/operators/AggregationOperators.html#aggregate-rx.functions.Func1-rx.functions.Func1-) - bounded buffering that emits the buffer when the key value has changed, and maps this to a dedicated type
* [`OrderedZipOperators.zip`](http://conductor.github.io/rx-ordered-data/javadoc/rx/operators/OrderedZipOperators.html#zip-rx.Observable-rx.Observable-java.util.Comparator-rx.functions.Func1-rx.functions.Func1-rx.functions.Func2-conductor.rx.ordered.flow.join.ZipType-) / [`OrderedZipOperators.zipWith`](http://conductor.github.io/rx-ordered-data/javadoc/rx/operators/OrderedZipOperators.html#zipWith-rx.Observable-java.util.Comparator-rx.functions.Func1-rx.functions.Func1-rx.functions.Func2-conductor.rx.ordered.flow.join.ZipType-) - zips two streams, with an additional configuration to specify what happens when keys are missing on one side
* [`JoinOperators.join`](http://conductor.github.io/rx-ordered-data/javadoc/rx/operators/JoinOperators.html#join-rx.Observable-rx.Observable-java.util.Comparator-rx.functions.Func1-rx.functions.Func1-rx.functions.Func2-conductor.rx.ordered.flow.join.JoinType-) / [`JoinOperators.joinWith`](http://conductor.github.io/rx-ordered-data/javadoc/rx/operators/JoinOperators.html#joinWith-rx.Observable-java.util.Comparator-rx.functions.Func1-rx.functions.Func1-rx.functions.Func2-conductor.rx.ordered.flow.join.JoinType-) - provides a SQL-like join between two ordered streams
