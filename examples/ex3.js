// ## Example 3: Chaining multiple transformations
// 
// Set `rawInput: true` and/or `rawOutput: true` to skip JSON deserialization (`rawInput`) and/or JSON serialization
// (`rawOutput`) when handling the transformation function input/output. These options are intended for advanced
// transformations (e.g., transformations spread across multiple functions or lower-level transformations directly
// on byte arrays).

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

// Parse the raw CSV data and transform the data into an object for convenience
function parse(row) {
  const [name, id] = row[0].split('_');
  const [first, last] = name.split('.')
  return JSON.stringify({ first, last, id }); // Manually handling serialization here, so use `rawOutput` to turn off automatic serialization
}

// Contains the logic for the transformation
function calculate(obj) {
  const { first, last, id } = JSON.parse(obj); // Manually handling deserialization here, so use `rawInput` to turn off automatic deserialization
  const name = `${last}, ${first}`;
  const paddedId = id.padStart(6, '0');
  return [name, paddedId];
}

await new CSVReader(new URL('./data/ex3-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(parse, { includeHeaders: true, rawOutput: true }))
  .pipeThrough(new CSVTransformer(calculate, { includeHeaders: true, rawInput: true }))
  .pipeTo(new CSVWriter(new URL('./data/ex3-out.csv', import.meta.url)));