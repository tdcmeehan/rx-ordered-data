rx-ordered-data
===============

[![Build Status](https://travis-ci.org/Conductor/rx-ordered-data.svg?branch=master)](https://travis-ci.org/Conductor/rx-ordered-data) [![Coverage Status](https://coveralls.io/repos/Conductor/rx-ordered-data/badge.svg?branch=master&service=github)](https://coveralls.io/github/Conductor/rx-ordered-data?branch=master)


RxJava utilities for working with pre-sorted data:

* `AggregationOperators.bufferByKey` - bounded buffering that emits the buffer when the key value has changed
* `AggregationOperators.aggregate` - bounded buffering that emits the buffer when the key value has changed, and maps this to a dedicated type
* `OrderedZipOperators.zip` / `OrderedZipOperators.zipWith` - zips two streams, with an additional configuration to specify what happens when keys are missing on one side
* `JoinOperators.join` / `JoinOperators.joinWith` - provides a SQL-like join between two ordered streams
