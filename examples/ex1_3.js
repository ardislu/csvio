// ## Example 1.3: Chaining multiple transformations
// 
// For advanced transformations (e.g., transformations spread across multiple functions or lower-level transformations
// directly on byte arrays), manually serialize and deserialize your data (e.g., using `JSON.stringify` and `JSON.parse`).
// A raw `string` type will be "passed through" `CSVTransformer` for your transformation function to handle directly.
// 
// Set `handleHeaders` to `true` to pass the header row to the transformation function as a normal row and manually
// handle the header output processing. This setting is required if the input CSV does not have a header row.

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

// Parse the raw CSV data and transform the data into an object for convenience
function parse(row) {
  const [name, id] = row[0].split('_');
  const [first, last] = name.split('.')
  return JSON.stringify({ first, last, id }); // Manually handle serialization here
}

// Contains the logic for the transformation
function calculate(obj) {
  const { first, last, id } = JSON.parse(obj); // Manually handle deserialization here
  const name = `${last}, ${first}`;
  const paddedId = id.padStart(6, '0');
  return [name, paddedId];
}

await new CSVReader(new URL('./data/ex1_3-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(parse, { handleHeaders: true }))
  .pipeThrough(new CSVTransformer(calculate, { handleHeaders: true }))
  .pipeTo(new CSVWriter(new URL('./data/ex1_3-out.csv', import.meta.url)));