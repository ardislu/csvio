// ## Example 1.6: Track the status of the CSV processing
// 
// Use the `status` property of CSVWriter to track the status of the output CSV file writing. This property
// updates as the writer receives data, so it may be used in a live progress tracker.

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

const writer = new CSVWriter(new URL('./data/ex1_6-out.csv', import.meta.url));

// For long-running processing, it may be useful to use an interval-based progress tracker as
// demonstrated below:
// const id = setInterval(() => {
//   const { name, elapsed, rows } = writer.status; 
//   const message = `${name} | ${elapsed.toFixed()}ms | ${rows} rows written`;
//   console.log(message);
// }, 1000); // Will log ongoing progress approximately every second

await new CSVReader(new URL('./data/ex1_6-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(r => r))
  .pipeTo(writer);

const { name, rows, elapsed } = writer.status;
const message = `${name} | ${elapsed.toFixed()}ms | ${rows} rows written`;
console.log(message);

// If using interval, remember to clear it after completion:
// clearInterval(id);