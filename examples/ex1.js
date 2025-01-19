// ## Example 1: Basic usage
// 
// Minimal example of a user-defined function to transform a CSV.
// 
// Notes:
// - The header row (first row) of the input CSV is not passed to the transformation function, it skips through to the output CSV.
// - The input to the transformation function is already deserialized to an array, however **each field in the array is a string**.
// - Each call to the transformation function passes 1 row of the input CSV.
// - The expected output of the transformation function is another array representing 1 row of the output CSV.

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

function timesTwo(row) {
  return [Number(row[0]) * 2, Number(row[1]) * 2];
}

await new CSVReader(new URL('./data/ex1-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(timesTwo))
  .pipeTo(new CSVWriter(new URL('./data/ex1-out.csv', import.meta.url)));