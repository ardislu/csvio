// ## Example 3.1: Batch processing (multi-row input) using `maxBatchSize`
// 
// Set the `maxBatchSize` option to a number greater than `1` to collect rows into batches before passing the whole
// batch to the transformation function.
// 
// This functionality is useful if you need to collect data from multiple rows before processing (e.g., performing an
// expensive network request where it is more efficient to batch multiple rows into one request).

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

let firstChunk = true;
let batchNumber = 0;
function process(batch) {
  if (firstChunk) { // Header row is always passed by itself, regardless of maxBatchSize
    firstChunk = false;
    return ['batch number', 'sum of batch', ...batch]; // Note the header row is Array<string>, NOT Array<Array<string>>
  }

  // Some logic that requires data across multiple rows
  let sum = 0;
  for (const row of batch) {
    for (const field of row) {
      sum += Number(field);
    }
  }

  batchNumber++;
  return batch.map(row => [batchNumber, sum, ...row]);
}

await new CSVReader(new URL('./data/ex3_1-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(process, { handleHeaders: true, maxBatchSize: 5 }))
  .pipeTo(new CSVWriter(new URL('./data/ex3_1-out.csv', import.meta.url)));