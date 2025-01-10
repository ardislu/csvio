// ## Example 2: Updating headers in the output
// 
// Set `includeHeaders: true` to manually handle the header row (first row).
// 
// Notes:
// - The header row is assumed to be the first row in the input CSV, and by default it goes directly to the output (skipping the user-provided transformation function).
// - If your input CSV does not have a header row, set `includeHeaders: true` and process the header row (first row) normally.
// - Otherwise, you will need distinct logic to handle the header row vs. data rows. A simple boolean variable is a good way to switch between this logic. 

import { createCSVReadableStream, createCSVTransformStream, createCSVWritableStream } from '../src/index.js';

let firstRow = true;
function sum(row) {
  if (firstRow) {
    firstRow = false;
    return [...row, 'sum'];
  }
  return [...row, Number(row[0]) + Number(row[1])];
}

await createCSVReadableStream(new URL('./data/ex2-in.csv', import.meta.url))
  .pipeThrough(createCSVTransformStream(sum, { includeHeaders: true }))
  .pipeTo(createCSVWritableStream(new URL('./data/ex2-out.csv', import.meta.url)));