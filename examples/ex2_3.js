// ## Example 2.3: Truncated binary exponential backoff (retry with graceful failure)

import { setTimeout } from 'node:timers/promises';
import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

// Simple implementation of exponential backoff with graceful failure at a given termination point
// https://en.wikipedia.org/wiki/Exponential_backoff
async function backoff(row, fn, truncate = 4) {
  for (let n = 0; n < truncate; n++) {
    const duration = (2 ** n) * 1000; // 1s, 2s, 4s, 8s, 16s, ...
    const jitter = Math.random() * 1000;

    // Reducing sleep by 1000x to avoid slowing down tests, comment out to use real time backoff
    const timeout = (duration + jitter) / 1000;
    await setTimeout(timeout);

    try { return await fn(row); }
    catch { }
  }
  return row.map(() => 'n/a'); // If tried `truncate` number of times, set row to "n/a" and proceed
}

// Each row will fail the number of times indicated in the mapping
// All rows should be retried until passed, except for "b" which gracefully fails to "n/a"
const fails = {
  a: 1,
  b: 8,
  c: 4,
  d: 0
};
function willFail(row) {
  if (fails[row[0]]--) {
    throw new Error();
  }
  return row;
}

await new CSVReader(new URL('./data/ex2_3-in.csv', import.meta.url))
  .pipeThrough(new CSVTransformer(willFail, { onError: (row, e, fn) => backoff(row, fn, 4) }))
  .pipeTo(new CSVWriter(new URL('./data/ex2_3-out.csv', import.meta.url)));