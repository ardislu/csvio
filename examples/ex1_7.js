// ## Example 1.7: Abort a transformation
// 
// Pass an [`AbortSignal`](https://developer.mozilla.org/en-US/docs/Web/API/AbortSignal) to `.pipeTo()` to gracefully
// abort a transformation. All transformations enqueued up until the row causing the abort (including the abort row
// itself) will be saved to disk.
// 
// Aborting throws an error, so you must also add a `.catch()` to handle the abort.

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

const controller = new AbortController();
function identityWithAbort(row) {
  if (row[0] === 'abort') { // Arbitrary logic for aborting the transformation
    controller.abort();
  }
  return row;
}

await new CSVReader(new URL('./data/ex1_7-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(identityWithAbort))
  .pipeTo(new CSVWriter(new URL('./data/ex1_7-out.csv', import.meta.url)), { signal: controller.signal })
  .catch(() => {}); // No-op to catch the intentional abort for example purposes. Beware this will also catch other errors.