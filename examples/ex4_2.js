// ## Example 4.2: Using `CSVNormalizer` and `CSVDenormalizer` with `CSVTransformer`
// 
// Sometimes it is easier to work with an array of objects than an array of strings in a transformation function.
// `CSVNormalizer` is useful for preparing CSV row data into such an array of objects, in addition to handling
// common CSV mangling by spreadsheet programs.

import { CSVReader, CSVWriter, CSVNormalizer, CSVDenormalizer, CSVTransformer } from '../src/index.js';
/** @import { CSVNormalizerHeader } from '../src/normalization.js'; */

// A minimal declaration to tell `CSVNormalizer` to trim empty rows and columns but otherwise don't touch the data
/** @type {Array<CSVNormalizerHeader>} */
const headers = [
  { name: 'columnA' },
  { name: 'columnB' },
  { name: 'columnC' },
  { name: 'sparse1' },
  { name: 'sparse2' },
  { name: 'sparse3' }
];

function transform(row) {
  let c = 0
  for (const field of row) {
    field.displayName = `${field.displayName}_new`; // To modify column names
    field.value = field.value.toUpperCase(); // To modify field values
    if (!field.emptyField) { c++; } // The `emptyField` property is useful for managing sparse or uneven CSV data
  }
  row.push({ name: 'count', value: c }); // You can also insert new columns in a transformation
  return row;
}

await new CSVReader(new URL('./data/ex4_2-in.csv', import.meta.url))
  .pipeThrough(new CSVNormalizer(headers))
  .pipeThrough(new CSVTransformer(transform))
  .pipeThrough(new CSVDenormalizer())
  .pipeTo(new CSVWriter(new URL('./data/ex4_2-out.csv', import.meta.url)));