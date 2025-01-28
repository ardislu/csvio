import { suite, test, after } from 'node:test';
import { ok, deepStrictEqual } from 'node:assert/strict';
import { unlink, stat } from 'node:fs/promises';

import { createRandomCSV, createTempFile } from './utils.js';
import { CSVReader, CSVTransformer, CSVWriter } from '../src/core.js';

suite('large: 100000 row x 100 column CSV (~100 MB)', { concurrency: 1 }, async () => {
  const temp = await createTempFile();
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
      const parsed = JSON.parse(row);
      if (rowCount === 0) {
        deepStrictEqual(parsed[0], '1717370683'); // A1
        deepStrictEqual(parsed[49], '1159187275'); // AX1
        deepStrictEqual(parsed[99], '398856332'); // CV1
      }
      else if (rowCount === 49_999) {
        deepStrictEqual(parsed[0], '1950971124'); // A50000
        deepStrictEqual(parsed[49], '1212575531'); // AX50000
        deepStrictEqual(parsed[99], '135758415'); // CV50000
      }
      else if (rowCount === 99_999) {
        deepStrictEqual(parsed[0], '737583361'); // A100000
        deepStrictEqual(parsed[49], '793036957'); // AX100000
        deepStrictEqual(parsed[99], '1923608001'); // CV100000
      }
      rowCount++;
    }
  });

  test('CSVTransformer transforms file', async () => {
    const reader = new CSVReader(temp)
      .pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1), { handleHeaders: true }));
    let rowCount = 0;
    for await (const row of reader) {
      const parsed = JSON.parse(row);
      if (rowCount === 0) {
        deepStrictEqual(parsed[0], 1717370684);
        deepStrictEqual(parsed[49], 1159187276);
        deepStrictEqual(parsed[99], 398856333);
      }
      else if (rowCount === 49_999) {
        deepStrictEqual(parsed[0], 1950971125);
        deepStrictEqual(parsed[49], 1212575532);
        deepStrictEqual(parsed[99], 135758416);
      }
      else if (rowCount === 99_999) {
        deepStrictEqual(parsed[0], 737583362);
        deepStrictEqual(parsed[49], 793036958);
        deepStrictEqual(parsed[99], 1923608002);
      }
      rowCount++;
    }
  });
});

suite('large: 100 row x 100000 column CSV (~100 MB)', { concurrency: 1 }, async () => {
  const temp = await createTempFile();
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
      const parsed = JSON.parse(row);
      if (rowCount === 0) {
        deepStrictEqual(parsed[0], '1717370683');
        deepStrictEqual(parsed[49_999], '689610881');
        deepStrictEqual(parsed[99_999], '183082010');
      }
      else if (rowCount === 49) {
        deepStrictEqual(parsed[0], '1521260985');
        deepStrictEqual(parsed[49_999], '1397216552');
        deepStrictEqual(parsed[99_999], '817708515');
      }
      else if (rowCount === 99) {
        deepStrictEqual(parsed[0], '1749947568');
        deepStrictEqual(parsed[49_999], '2119460449');
        deepStrictEqual(parsed[99_999], '127114681');
      }
      rowCount++;
    }
  });

  test('CSVTransformer transforms file', async () => {
    const reader = new CSVReader(temp)
      .pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1), { handleHeaders: true }));
    let rowCount = 0;
    for await (const row of reader) {
      const parsed = JSON.parse(row);
      if (rowCount === 0) {
        deepStrictEqual(parsed[0], 1717370684);
        deepStrictEqual(parsed[49_999], 689610882);
        deepStrictEqual(parsed[99_999], 183082011);
      }
      else if (rowCount === 49) {
        deepStrictEqual(parsed[0], 1521260986);
        deepStrictEqual(parsed[49_999], 1397216553);
        deepStrictEqual(parsed[99_999], 817708516);
      }
      else if (rowCount === 99) {
        deepStrictEqual(parsed[0], 1749947569);
        deepStrictEqual(parsed[49_999], 2119460450);
        deepStrictEqual(parsed[99_999], 127114682);
      }
      rowCount++;
    }
  });
});