// ## Example 4.4: Using `CSVNormalizer.toFieldMap()`
// 
// Use the static method `CSVNormalizer.toFieldMap()` to conveniently access field values (using a subset of the familiar
// `Map` interface) while keeping the underlying `row` object in sync with updates.
// 
// The field map is convenient for straightforward reading and writing to field values. For more advanced usage, directly
// interface with the `row` object.

import { CSVReader, CSVWriter, CSVNormalizer, CSVDenormalizer, CSVTransformer } from '../src/index.js';
/** @import { CSVNormalizerHeader, CSVNormalizerField } from '../src/index.js'; */

/** @type {Array<CSVNormalizerHeader>} */
const headers = [
  { name: 'a' },
  { name: 'b' },
  { name: 'c' }
];

/**
 * @param {Array<CSVNormalizerField>} row 
 * @returns {Array<CSVNormalizerField>}
 */
function transform(row) {
  const map = CSVNormalizer.toFieldMap(row);
  const a = map.get('a'); // Use the familiar `Map` interface
  const b = map.get('b');
  const c = map.get('c');
  map.set('a', `intercepted: ${a}`); // The underlying object in `row` is also updated
  map.set('b', `intercepted: ${b}`);
  map.set('c', `intercepted: ${c}`);
  map.set('d', 'new column: 444'); // New names are automatically pushed as new field
  return row; // Return the original input row, which stays in sync with changes made to the field map
}

await new CSVReader(new URL('./data/ex4_4-in.csv', import.meta.url))
  .pipeThrough(new CSVNormalizer(headers))
  .pipeThrough(new CSVTransformer(transform))
  .pipeThrough(new CSVDenormalizer())
  .pipeTo(new CSVWriter(new URL('./data/ex4_4-out.csv', import.meta.url)));
