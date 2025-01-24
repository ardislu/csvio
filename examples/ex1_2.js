// ## Example 1.2: Replace headers in the output
// 
// Set `handleHeaders` to an `Array<any>` to replace the header row (first row) in the output.
// 
// Notes:
// - The header row is assumed to be the first row in the input CSV, and by default it goes directly to the output (skipping the user-provided transformation function).
// - Setting `handleHeaders` to an array is the simplest way to rename headers, delete headers, or add new headers.

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

function sum(row) {
  return [...row, Number(row[0]) + Number(row[1])];
}

await new CSVReader(new URL('./data/ex1_2-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(sum, { handleHeaders: ['Column A', 'Column B', 'Sum'] }))
  .pipeTo(new CSVWriter(new URL('./data/ex1_2-out.csv', import.meta.url)));