// ## Example 2.2: Graceful failure (placeholder value)

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

// Set a placeholder value and continue processing
function graceful(row) {
  return row.map(() => 'n/a');
}

// Will throw on every other row
let i = 0;
function flaky(row) {
  i++;
  if (i % 2 === 0) {
    throw new Error('Example error message');
  }
  return row;
}

await new CSVReader(new URL('./data/ex2_2-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(flaky, { onError: row => graceful(row) }))
  .pipeTo(new CSVWriter(new URL('./data/ex2_2-out.csv', import.meta.url)));