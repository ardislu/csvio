// ## Example 3.2: Concurrent processing (async) using `maxConcurrent`
// 
// Set the `maxConcurrent` option to a number greater than `1` to process rows concurrently. The transformation function
// will be automatically turned into a promise if it isn't already async.
// 
// Note that execution is blocked until all promises in a concurrent group settle. So if there is one transformation that
// takes very long, all row processing is blocked on the 1 transformation even if all others in the group have already
// resolved.

import { setTimeout } from 'node:timers/promises';
import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

// Returns how many 100ms increments have elapsed since this program began
const start = performance.now();
function tick() {
  const ms = performance.now() - start;
  return Math.floor(ms / 100);
}

async function concurrent(row) {
  await setTimeout(100); // Simulate some slow task, e.g. a network request
  return [tick(), ...row];
}

await new CSVReader(new URL('./data/ex3_2-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(concurrent, { handleHeaders: h => ['tick number', ...h], maxConcurrent: 5 }))
  .pipeTo(new CSVWriter(new URL('./data/ex3_2-out.csv', import.meta.url)));