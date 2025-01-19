// ## Example 5: Handling an irregularly-shaped CSV file
// 
// `CSVReader` can parse CSVs that have rows with a mismatching number of columns, for example CSVs
// with informational file headers. This information can be handled as required in the transformation function.
//
// Return `null` in the transformation function to consume an input row without emitting an output row.

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

let i = 0;
let report, id, date;
function parse(row) {
  // Special logic for the first six rows of the CSV
  switch (i++) {
    case 0: report = row[0]; return null;
    case 1: id = row[0].split(' ')[1]; return null;
    case 2: date = row[0]; return null;
    case 3: return null;
    case 4: return null;
    case 5: return ['Report', 'ID', 'Date', 'Name', 'Value_1', 'Value_2'];
  }

  // Normal logic for the rest of the CSV
  return [report, id, date, ...row];
}

await new CSVReader(new URL('./data/ex5-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(parse, { includeHeaders: true }))
  .pipeTo(new CSVWriter(new URL('./data/ex5-out.csv', import.meta.url)));