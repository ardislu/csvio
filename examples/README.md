# Examples

This folder contains end-to-end examples of using this library.

All examples can be run directly without additional setup using `node`. E.g.:

```
node ./examples/ex1_1.js
```

## Table of contents

### Section 1: Common usage

These examples demonstrate the most common, expected usage.

[Example 1.1: Basic usage](./ex1_1.js)

[Example 1.2: Updating headers in the output](./ex1_2.js)

[Example 1.3: Chaining multiple transformations](./ex1_3.js)

[Example 1.4: Handling an irregularly-shaped CSV file](./ex1_4.js)

[Example 1.5: Duplicate input rows to the output](./ex1_5.js)

### Section 2: Error strategies

These examples demonstrate using the `onError` option to handle transformations that may throw errors.

[Example 2.1: Basic retry strategy](./ex2_1.js)

[Example 2.2: Graceful failure (placeholder value)](./ex2_2.js)

### Section 3: Batch and concurrent processing

These examples demonstrate using the `maxBatchSize` and `maxConcurrent` options to process more than one CSV row at a time.

[Example 3.1: Batch processing (multi-row input) using `maxBatchSize`](./ex3_1.js)

[Example 3.2: Concurrent processing (async) using `maxConcurrent`](./ex3_2.js)

### Section 4: Extended API

These examples demonstrate the extended API.

[Example 4.1: Using `CSVNormalizer` and `CSVDenormalizer`](./ex4_1.js)
