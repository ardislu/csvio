// ## Example 1.10: Return a ReadableStream from a transformation
// 
// If the transformation itself involves a stream, you can directly return a ReadableStream to CSVTransformer.
// This capability is useful if your transformation involves a network request; you can directly pass the
// streaming response body as the output of your transformation.

import { ReadableStream } from 'node:stream/web';
import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';
/** @import { TransformationFunction } from '../src/index.js'; */

let rowCount = 0;

/** @type {TransformationFunction} */
function stream(row) {
  return ReadableStream.from((function* () {
    yield [`${row[0]}: streamed 1`];
    yield [`${row[0]}: streamed 2`];

    // Errors produced by the ReadableStream are NOT be handled by CSVTransformer's onError, use .catch or implement
    // your own error handling this instead. Note some rows yielded immediately before the error may be dropped.
    if (rowCount === 2) {
      throw new Error();
    }

    yield [`${row[0]}: streamed 3`];
    rowCount++;
  })());
}

await new CSVReader(new URL('./data/ex1_10-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(stream))
  .pipeTo(new CSVWriter(new URL('./data/ex1_10-out.csv', import.meta.url)))
  .catch(() => { }); // No-op to catch the intentional error for example purposes. Beware this will also catch other errors.
