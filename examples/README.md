# Examples

This folder contains end-to-end examples of using this library.

All examples can be run directly without additional setup using `node`. E.g.:

```
node ./examples/ex1_1.js
```

## Table of contents

### Section 1: Common usage

These examples demonstrate the most common, expected usage.

[Example 1.1: Basic usage](./ex1_1.js) ([input](./data/ex1_1-in.csv), [output](./data/ex1_1-out.csv))

[Example 1.2: Replace headers in the output](./ex1_2.js) ([input](./data/ex1_2-in.csv), [output](./data/ex1_2-out.csv))

[Example 1.3: Chaining multiple transformations](./ex1_3.js) ([input](./data/ex1_3-in.csv), [output](./data/ex1_3-out.csv))

[Example 1.4: Handling an irregularly-shaped CSV file](./ex1_4.js) ([input](./data/ex1_4-in.csv), [output](./data/ex1_4-out.csv))

[Example 1.5: Duplicate input rows to the output](./ex1_5.js) ([input](./data/ex1_5-in.csv), [output](./data/ex1_5-out.csv))

[Example 1.6: Track the status of the CSV processing](./ex1_6.js) ([input](./data/ex1_6-in.csv), [output](./data/ex1_6-out.csv))

[Example 1.7: Abort a transformation](./ex1_7.js) ([input](./data/ex1_7-in.csv), [output](./data/ex1_7-out.csv))

### Section 2: Error strategies

These examples demonstrate using the `onError` option to handle transformations that may throw errors.

[Example 2.1: Basic retry strategy](./ex2_1.js) ([input](./data/ex2_1-in.csv), [output](./data/ex2_1-out.csv))

[Example 2.2: Graceful failure (placeholder value)](./ex2_2.js) ([input](./data/ex2_2-in.csv), [output](./data/ex2_2-out.csv))

[Example 2.3: Truncated binary exponential backoff (retry with graceful failure)](./ex2_3.js) ([input](./data/ex2_3-in.csv), [output](./data/ex2_3-out.csv))

### Section 3: Batch and concurrent processing

These examples demonstrate using the `maxBatchSize` and `maxConcurrent` options to process more than one CSV row at a time.

[Example 3.1: Batch processing (multi-row input) using `maxBatchSize`](./ex3_1.js) ([input](./data/ex3_1-in.csv), [output](./data/ex3_1-out.csv))

[Example 3.2: Concurrent processing (async) using `maxConcurrent`](./ex3_2.js) ([input](./data/ex3_2-in.csv), [output](./data/ex3_2-out.csv))

### Section 4: Extended API

These examples demonstrate the extended API.

[Example 4.1: Using `CSVNormalizer` and `CSVDenormalizer`](./ex4_1.js) ([input](./data/ex4_1-in.csv), [output](./data/ex4_1-out.csv))
