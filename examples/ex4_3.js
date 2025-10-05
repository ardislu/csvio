// ## Example 4.3: Using `CSVNormalizer.toObject()`
// 
// Use the static method `CSVNormalizer.toObject()` if you just want to read the values of `CSVNormalizerField`.
// For more advanced usage, such as keeping the underlying `row` object in sync with changes to the value, see
// `CSVNormalizer.toFieldMap()`.

import { CSVReader, CSVWriter, CSVNormalizer, CSVDenormalizer, CSVTransformer } from '../src/index.js';
/** @import { CSVNormalizerHeader, CSVNormalizerField } from '../src/index.js'; */

/** @type {Array<CSVNormalizerHeader>} */
const headers = [
  { name: 'a' },
  { name: 'b' },
  { name: 'c' },
  { name: '🏴‍☠️' },
  { name: '😂' }
];

/**
 * @param {Array<CSVNormalizerField>} row 
 * @returns {Array<CSVNormalizerField>}
 */
function transform(row) {
  const obj = CSVNormalizer.toObject(row);
  const { a, b, c } = obj; // Destructuring works
  const pirate = obj['🏴‍☠️']; // Access by field name works
  const laughing = obj['😂'];
  const out = [ // Manually construct a new output row
    { name: 'a', value: `intercepted: ${a}` },
    { name: 'b', value: `intercepted: ${b}` },
    { name: 'c', value: `intercepted: ${c}` },
    { name: '🏴‍☠️', value: `intercepted: ${pirate}` },
    { name: '😂', value: `intercepted: ${laughing}` },
  ];
  return out;
}

await new CSVReader(new URL('./data/ex4_3-in.csv', import.meta.url))
  .pipeThrough(new CSVNormalizer(headers))
  .pipeThrough(new CSVTransformer(transform))
  .pipeThrough(new CSVDenormalizer())
  .pipeTo(new CSVWriter(new URL('./data/ex4_3-out.csv', import.meta.url)));
