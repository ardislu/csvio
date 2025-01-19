// ## Example 4: Using `CSVNormalizer` and `CSVDenormalizer`
// 
// These functions fix common CSV data mangling caused by Excel. In `ex4-in.csv`:
// - The "Index" column has been mangled into Excel's "Accounting" format
// - The "Phone Number" column has been mangled into scientific notation
// - The "Join Timestamp" column has been mangled into a number
// - There are trailing empty rows after deleting excess data from the CSV
// `CSVNormalizer` performs transformations to un-mangle the above using only a declaration of the header names and data types.
// `CSVDenormalizer` converts the data stream back into a CSV row.

import { CSVReader, CSVWriter, CSVNormalizer, CSVDenormalizer } from '../src/index.js';

// The order of items in `headers` is significant, it determines the order of the output CSV columns.
// If an input column is not provided in `headers`, the column is removed from the output CSV.
const headers = [
  {
    name: 'index', // MUST match the input CSV column name in camelCase
    type: 'number',
    displayName: 'Index' // `displayName` can also be used to rename the column in the output CSV
  },
  {
    name: 'phoneNumber',
    type: 'bigint',
    displayName: 'Phone Number'
  },
  {
    name: 'joinTimestamp',
    type: 'date',
    displayName: 'Join Timestamp'
  }
];

await new CSVReader(new URL('./data/ex4-in.csv', import.meta.url))
  .pipeThrough(new CSVNormalizer(headers))
  .pipeThrough(new CSVDenormalizer())
  .pipeTo(new CSVWriter(new URL('./data/ex4-out.csv', import.meta.url)));