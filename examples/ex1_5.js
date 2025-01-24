// ## Example 1.5: Duplicate input rows to the output
// 
// Use the spread syntax (...) to concisely duplicate input rows to the output, in both the transformation function
// and the `handleHeaders` function.
// 
// Setting `handleHeaders` to a separate function is convenient to handle its logic separately from the data rows.
// Using a transformation function instead of a hardcoded array is useful if the input row headers are unknown
// or other arbitrary logic is required to determine the output header names.

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

// It is useful to split the header transformation into a separate function because the logic for the header
// is likely different from the rest of the CSV.
function handleHeaders(headers) {
  return [...headers, 'min', 'max', 'mean']; // Duplicate input headers as the first headers in the output
}

function calculate(row) {
  const numbers = row.map(Number);
  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const mean = numbers.reduce((a, n) => a + n, 0) / numbers.length;
  return [...row, min, max, mean]; // Duplicate input rows as the first columns in the output
}

await new CSVReader(new URL('./data/ex1_5-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(calculate, { handleHeaders }))
  .pipeTo(new CSVWriter(new URL('./data/ex1_5-out.csv', import.meta.url)));