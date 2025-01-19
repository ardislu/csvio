// ## Example 6: Handling a flaky transformation (basic retry strategy)
// 
// Pass a function to the `onError` option of `CSVTransformer` to catch errors thrown by the transformation
// function.
// 
// The `onError` function may be used to implement graceful failure strategies (e.g., set the output row to a
// placeholder), retry strategies (e.g., a truncated exponential backoff for flaky network requests), logging, etc.

import { createCSVReadableStream, CSVTransformer, createCSVWritableStream } from '../src/index.js';

// Will immediately retry calling `fn` a maximum of `iterations` times
function retry(row, fn, iterations) {
  while (iterations--) {
    try { return fn(row); }
    catch { }
  }
}

// Will fail 90% of the time
function flaky(row) {
  if (Math.random() < 0.9) {
    throw new Error();
  }
  const [v] = row;
  return [`${v}: pass`];
}

await createCSVReadableStream(new URL('./data/ex6-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(flaky, { onError: (row, e, fn) => retry(row, fn, 1000) }))
  .pipeTo(createCSVWritableStream(new URL('./data/ex6-out.csv', import.meta.url)));