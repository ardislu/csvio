import { suite, test, before, after } from 'node:test';
import { ok, deepStrictEqual } from 'node:assert/strict';
import { unlink, stat } from 'node:fs/promises';
import { memoryUsage } from 'node:process';
import { setTimeout } from 'node:timers/promises';

import { createRandomCSV, createTempFile } from './utils.js';
import { CSVReader, CSVTransformer, CSVWriter } from '../src/core.js';

/** Maximum variance allowed for memory leak testing. */
const MEMORY_THRESHOLD = 10 * 1024 * 1024; // 10 MB, or 10% of the CSV file size

/** Snapshot of memory usage before any tests have begun. */
const MEMORY_BEFORE = memoryUsage().heapUsed;

/**
 * Helper function that will block until garbage collection is performed.
 * 
 * **WARNING:** By design, JavaScript garbage collection is unpredictable and may never happen (i.e., this
 * function may never resolve). Use this function with caution.
 */
async function garbageCollection() {
  const { promise, resolve } = Promise.withResolvers();
  const registry = new FinalizationRegistry(resolve);
  (() => registry.register({}, ''))();

  // Theoretically, you can just do:
  // global.gc?.();
  // await promise;
  // However, in testing, `promise` never resolves. So, the infinite loop with `setTimeout` is required.
  let collected = false;
  promise.then(() => collected = true);

  global.gc?.();
  while (!collected) {
    await setTimeout(0);
  }
}

suite('large: 100000 row x 100 column CSV (~100 MB)', { concurrency: 1 }, async () => {
  /** @type {string} */
  let temp;
  before(async () => { temp = await createTempFile() });
  after(async () => await unlink(temp));

  test('CSVWriter writes file', async () => {
    await createRandomCSV(100000, 100, 1).pipeTo(new CSVWriter(temp));
    const { size } = await stat(temp, { bigint: true });
    ok(size > 100_000_000n);
  });

  test('CSVReader reads file', async () => {
    const reader = new CSVReader(temp);
    let rowCount = 0;
    for await (const row of reader) {
      if (rowCount === 0) {
        deepStrictEqual(row[0], '1717370683'); // A1
        deepStrictEqual(row[49], '1159187275'); // AX1
        deepStrictEqual(row[99], '398856332'); // CV1
      }
      else if (rowCount === 49_999) {
        deepStrictEqual(row[0], '1950971124'); // A50000
        deepStrictEqual(row[49], '1212575531'); // AX50000
        deepStrictEqual(row[99], '135758415'); // CV50000
      }
      else if (rowCount === 99_999) {
        deepStrictEqual(row[0], '737583361'); // A100000
        deepStrictEqual(row[49], '793036957'); // AX100000
        deepStrictEqual(row[99], '1923608001'); // CV100000
      }
      rowCount++;
    }
  });

  test('CSVTransformer transforms file', async () => {
    const reader = new CSVReader(temp)
      .pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1), { handleHeaders: true }));
    let rowCount = 0;
    for await (const row of reader) {
      if (rowCount === 0) {
        deepStrictEqual(row[0], 1717370684);
        deepStrictEqual(row[49], 1159187276);
        deepStrictEqual(row[99], 398856333);
      }
      else if (rowCount === 49_999) {
        deepStrictEqual(row[0], 1950971125);
        deepStrictEqual(row[49], 1212575532);
        deepStrictEqual(row[99], 135758416);
      }
      else if (rowCount === 99_999) {
        deepStrictEqual(row[0], 737583362);
        deepStrictEqual(row[49], 793036958);
        deepStrictEqual(row[99], 1923608002);
      }
      rowCount++;
    }
  });
});

suite('large: 100 row x 100000 column CSV (~100 MB)', { concurrency: 1 }, async () => {
  /** @type {string} */
  let temp;
  before(async () => { temp = await createTempFile() });
  after(async () => await unlink(temp));

  test('CSVWriter writes file', async () => {
    await createRandomCSV(100, 100000, 1).pipeTo(new CSVWriter(temp));
    const { size } = await stat(temp, { bigint: true });
    ok(size > 100_000_000n);
  });

  test('CSVReader reads file', async () => {
    const reader = new CSVReader(temp);
    let rowCount = 0;
    for await (const row of reader) {
      if (rowCount === 0) {
        deepStrictEqual(row[0], '1717370683');
        deepStrictEqual(row[49_999], '689610881');
        deepStrictEqual(row[99_999], '183082010');
      }
      else if (rowCount === 49) {
        deepStrictEqual(row[0], '1521260985');
        deepStrictEqual(row[49_999], '1397216552');
        deepStrictEqual(row[99_999], '817708515');
      }
      else if (rowCount === 99) {
        deepStrictEqual(row[0], '1749947568');
        deepStrictEqual(row[49_999], '2119460449');
        deepStrictEqual(row[99_999], '127114681');
      }
      rowCount++;
    }
  });

  test('CSVTransformer transforms file', async () => {
    const reader = new CSVReader(temp)
      .pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1), { handleHeaders: true }));
    let rowCount = 0;
    for await (const row of reader) {
      if (rowCount === 0) {
        deepStrictEqual(row[0], 1717370684);
        deepStrictEqual(row[49_999], 689610882);
        deepStrictEqual(row[99_999], 183082011);
      }
      else if (rowCount === 49) {
        deepStrictEqual(row[0], 1521260986);
        deepStrictEqual(row[49_999], 1397216553);
        deepStrictEqual(row[99_999], 817708516);
      }
      else if (rowCount === 99) {
        deepStrictEqual(row[0], 1749947569);
        deepStrictEqual(row[49_999], 2119460450);
        deepStrictEqual(row[99_999], 127114682);
      }
      rowCount++;
    }
  });
});

test('large: memory is released after test suites finish, within acceptable threshold', { concurrency: 1 }, async () => {
  await garbageCollection();
  const memoryAfter = memoryUsage().heapUsed;
  ok(memoryAfter - MEMORY_BEFORE < MEMORY_THRESHOLD);
});
