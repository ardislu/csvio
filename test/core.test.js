import { suite, test } from 'node:test';
import { ok, deepStrictEqual, throws, rejects } from 'node:assert/strict';
import { unlink } from 'node:fs/promises';
import { normalize, basename, resolve } from 'node:path';
import { setTimeout } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import { ReadableStream } from 'node:stream/web';

import { csvStreamEqualWritable, csvStreamNotEqualWritable, createCSVMockStream, createTempFile } from './utils.js';
import { parsePathLike, createFileStream, CSVReader, CSVTransformer, CSVWriter } from '../src/core.js';

suite('CSVWriter.arrayToCSVString', { concurrency: true }, () => {
  const vectors = [
    { name: 'converts simple row', input: ['abc', '123'], output: 'abc,123\r\n' },
    { name: 'converts row with commas', input: ['a,bc', '12,3'], output: '"a,bc","12,3"\r\n' },
    { name: 'converts row with double quotes', input: ['a"bc', '12"3'], output: '"a""bc","12""3"\r\n' },
    { name: 'converts row with newlines', input: ['a\nbc', '12\r\n3'], output: '"a\nbc","12\r\n3"\r\n' },
    { name: 'converts row with mixed escaped characters', input: ['a,"\nbc', '12,"\r\n3'], output: '"a,""\nbc","12,""\r\n3"\r\n' },
    { name: 'converts single value (no commas) row', input: ['abc'], output: 'abc\r\n' },
    { name: 'converts empty row', input: [''], output: '\r\n' },
    { name: 'converts empty row with multiple fields', input: ['', '', '', ''], output: ',,,\r\n' }
  ];
  for (const { name, input, output } of vectors) {
    test(name, { concurrency: true }, () => {
      deepStrictEqual(CSVWriter.arrayToCSVString(input), output);
    });
  }
});

suite('parsePathLike', { concurrency: true }, () => {
  const vectors = [
    { name: 'parses string relative path', input: './folder/example.txt', output: './folder/example.txt' },
    { name: 'parses string absolute path', input: '/absolute/path/to/file.txt', output: '/absolute/path/to/file.txt' },
    { name: 'parses TypedArray relative path', input: new TextEncoder().encode('./folder/example.txt'), output: './folder/example.txt' },
    { name: 'parses TypedArray absolute path', input: new TextEncoder().encode('/absolute/path/to/file.txt'), output: '/absolute/path/to/file.txt' },
    { name: 'parses Buffer relative path', input: new TextEncoder().encode('./folder/example.txt').buffer, output: './folder/example.txt' },
    { name: 'parses Buffer absolute path', input: new TextEncoder().encode('/absolute/path/to/file.txt').buffer, output: '/absolute/path/to/file.txt' },
    { name: 'parses URL relative path', input: pathToFileURL('./folder/example.txt'), output: resolve('./folder/example.txt') }, // Must use resolve because URLs automatically resolve to absolute path
    { name: 'parses URL absolute path', input: pathToFileURL('/absolute/path/to/file.txt'), output: resolve('/absolute/path/to/file.txt') },
    { name: 'parses URL with percent-encoding', input: pathToFileURL('./folder/example%20chars.txt'), output: resolve('./folder/example%20chars.txt') },
    { name: 'parses empty string', input: '', output: '' },
    { name: 'throws TypeError for unsupported type (number)', input: 12345, error: TypeError },
    { name: 'throws TypeError for unsupported type (object)', input: { path: './example.txt' }, error: TypeError },
    { name: 'throws TypeError for unsupported type (null)', input: null, error: TypeError },
    { name: 'throws TypeError for unsupported URL schema', input: new URL('http://example.com/example.txt'), error: TypeError },
  ];
  for (const { name, input, output, error } of vectors) {
    test(name, { concurrency: true }, () => {
      if (error !== undefined) {
        // @ts-expect-error
        throws(() => parsePathLike(input), error);
      }
      else {
        deepStrictEqual(parsePathLike(input), normalize(output));
      }
    });
  }
  test('parses SharedArrayBuffer relative path', { concurrency: true }, () => {
    const str = './folder/example.txt';
    const view = new Uint8Array(new SharedArrayBuffer(str.length));
    new TextEncoder().encodeInto(str, view);
    ok(view.buffer instanceof SharedArrayBuffer);
    deepStrictEqual(parsePathLike(view.buffer), normalize(str));
  })
  test('parses SharedArrayBuffer absolute path', { concurrency: true }, () => {
    const str = '/absolute/path/to/file.txt';
    const view = new Uint8Array(new SharedArrayBuffer(str.length));
    new TextEncoder().encodeInto(str, view);
    ok(view.buffer instanceof SharedArrayBuffer);
    deepStrictEqual(parsePathLike(view.buffer), normalize(str));
  })
});

suite('createFileStream', { concurrency: true }, () => {
  test('can cancel stream before reading', { concurrency: true }, async () => {
    const path = new URL('./data/simple.csv', import.meta.url);
    const s = createFileStream(path);
    await s.cancel();
    const c = await s.getReader({ mode: 'byob' }).read(new Uint8Array(1)).then(({ value }) => value);
    deepStrictEqual(c?.[0], undefined); // Should be empty array because the stream was canceled
  });
  test('can cancel on reader before reading', { concurrency: true }, async () => {
    const path = new URL('./data/simple.csv', import.meta.url);
    const s = createFileStream(path);
    const r = s.getReader({ mode: 'byob' });
    await r.cancel();
    r.releaseLock();
    rejects(r.read(new Uint8Array(1)));
  });
  test('can read then cancel on stream', { concurrency: true }, async () => {
    const path = new URL('./data/simple.csv', import.meta.url);
    const s = createFileStream(path);
    const r = s.getReader({ mode: 'byob' });
    const c = await r.read(new Uint8Array(1)).then(({ value }) => value);
    r.releaseLock();
    await s.cancel();
    rejects(r.read(new Uint8Array(1)));
    deepStrictEqual(c?.[0], 99);
  });
  test('can read then cancel on reader', { concurrency: true }, async () => {
    const path = new URL('./data/simple.csv', import.meta.url);
    const s = createFileStream(path);
    const r = s.getReader({ mode: 'byob' });
    const c = await r.read(new Uint8Array(1)).then(({ value }) => value);
    await r.cancel();
    r.releaseLock();
    rejects(r.read(new Uint8Array(1)));
    deepStrictEqual(c?.[0], 99);
  });
  test('can re-acquire lock', { concurrency: true }, async () => {
    const path = new URL('./data/simple.csv', import.meta.url);
    const s = createFileStream(path);

    const r = s.getReader({ mode: 'byob' });
    const c = await r.read(new Uint8Array(1)).then(({ value }) => value);
    r.releaseLock();

    const r2 = s.getReader({ mode: 'byob' });
    const c2 = await r2.read(new Uint8Array(1)).then(({ value }) => value);
    r2.releaseLock();

    await s.cancel();

    rejects(r.read(new Uint8Array(1)));
    rejects(r2.read(new Uint8Array(1)));
    deepStrictEqual(c?.[0], 99);
    deepStrictEqual(c2?.[0], 111);
  });
});

suite('CSVReader', { concurrency: true }, () => {
  const vectors = [
    {
      name: 'parses simple CSV',
      input: './data/simple.csv',
      output: [
        ['column1', 'column2', 'column3'],
        ['abc', 'def', 'ghi'],
        ['123', '456', '789'],
        ['aaa', 'bbb', 'ccc']
      ]
    },
    {
      name: 'parses escaping CSV',
      input: './data/escaping.csv',
      output: [
        ['name', 'value'],
        ['Three spaces', '   '],
        ['Three commas', ',,,'],
        ['Three newlines', '\r\n\r\n\r\n'],
        ['Three unescaped double quotes', 'a"""'],
        ['Three escaped double quotes', '"""'],
        ['Unescaped double quotes around delimiter 1 "', 'a'],
        ['Unescaped double quotes around delimiter 2 ""', 'a'],
        ['Unescaped double quotes around delimiter 3 """', 'a'],
        ['Unescaped double quotes around delimiter 4', ' "'],
        ['Unescaped double quotes around delimiter 5', ' ""'],
        ['Unescaped double quotes around delimiter 6', ' """'],
        ['Spaces before and after a value', '   abc   '],
        ['Spaces before and after three unescaped double quotes', '   """   '],
        ['Spaces trailing escaped double quotes', 'abc   '],
        ['Characters trailing escaped double quotes', 'abc def'],
        ['Unescaped double quotes trailing escaped double quotes', 'abc " def 123 """ 456'],
        ['Unescaped "double quotes" trailing escaped double quotes', 'abc """ def 123 """ 456'],
        ['Unicode test 1', '你好'],
        ['Unicode test 2', '😂👌👍'],
        ['Unicode test 3', '🏴‍☠️👨‍👩‍👧‍👦'],
        ['Mixed', ',\r\n",\r\n",\r\n"']
      ]
    },
    {
      name: 'parses escaping edges CSV',
      input: './data/escaping-edges.csv',
      output: [
        [',,,', '"""'],
        ['"""', ',,,'],
        ['a"b""', 'a"b""'],
        ['a"b""', 'a"b""']
      ]
    },
    {
      name: 'parses sparse CSV',
      input: './data/sparse.csv',
      output: [
        ['column1', 'column2', 'column3'],
        [''],
        ['', ''],
        ['', '', ''],
        ['', '', '', ''],
        ['', '', '', '', ''],
        ['aaa', 'bbb', 'ccc'],
        ['111', '222', '333', '444', '555', '666', '777', '888'],
        ['', 'hhh', 'iii'],
        ['ggg', '', 'iii'],
        ['ggg', 'hhh', ''],
        ['', 'hhh', ''],
        ['ggg', '', ''],
        ['', '', 'iii'],
        ['', '', ''],
        ['', '', ''],
        ['', '', ''],
        [''],
        ['']
      ]
    },
    {
      name: 'parses line feed end-of-line CSV',
      input: './data/lf-eol.csv',
      output: [
        ['column1', 'column2'],
        ['a', 'b'],
        ['c', 'd'],
        ['1', '2'],
        ['3', '4']
      ]
    },
    {
      name: 'parses byte order mark CSV',
      input: './data/bom.csv',
      output: [
        ['column1', 'column2'],
        ['ab', 'cd'],
        ['12', '34']
      ]
    },
    {
      name: 'parses UTF-16 LE CSV',
      input: './data/utf-16le.csv',
      options: { encoding: 'utf-16le' },
      output: [
        ['column1', 'column2'],
        ['ab', 'cd'],
        ['12', '34']
      ]
    },
    {
      name: 'parses UTF-16 BE CSV',
      input: './data/utf-16be.csv',
      options: { encoding: 'utf-16be' },
      output: [
        ['column1', 'column2'],
        ['ab', 'cd'],
        ['12', '34']
      ]
    },
    {
      name: 'parses single value (no commas or newlines) CSV',
      input: './data/single-value.csv',
      output: [
        ['abc']
      ]
    },
    {
      name: 'parses single column (no commas) CSV',
      input: './data/single-column.csv',
      output: [
        ['abc'],
        ['def'],
        ['ghi']
      ]
    },
    {
      name: 'parses single row (no newlines) CSV',
      input: './data/single-row.csv',
      output: [
        ['abc', 'def', 'ghi']
      ]
    },
    {
      name: 'parses blank CSV',
      input: './data/blank.csv',
      output: [
        ['']
      ],
      negativeOutput: [
        ['a']
      ]
    }
  ];
  for (const { name, input, options = {}, output, negativeOutput } of vectors) {
    test(name, { concurrency: true }, async () => {
      const path = new URL(input, import.meta.url); // Makes the path relative to this file, not the command's execution directory
      await new CSVReader(path, options).pipeTo(csvStreamEqualWritable(output));
      if (negativeOutput !== undefined) {
        await new CSVReader(path, options).pipeTo(csvStreamNotEqualWritable(negativeOutput));
      }
    });
  }
  test('can cancel', { concurrency: true }, async () => {
    const path = new URL('./data/simple.csv', import.meta.url);
    const s = new CSVReader(path);
    await s.cancel();
    const c = await s.getReader().read().then(({ value }) => value);
    deepStrictEqual(c, undefined);
  });
});

suite('CSVTransformer', { concurrency: true }, () => {
  test('passes through CSV', { concurrency: true }, async () => {
    const csv = [
      ['columnA', 'columnB'],
      ['a', 'b']
    ];
    await createCSVMockStream(csv)
      .pipeThrough(new CSVTransformer(r => r))
      .pipeTo(csvStreamEqualWritable(csv));
  });
  test('passes through raw input', { concurrency: true }, async () => {
    await ReadableStream.from((function* () {
      yield '["headerA","headerB"]';
      yield '["111","222"]';
    })())
      .pipeThrough(new CSVTransformer(r => r, { handleHeaders: true, rawInput: true }))
      .pipeTo(csvStreamEqualWritable([
        ['headerA', 'headerB'],
        ['111', '222']
      ]));
  });
  test('passes through raw output', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['a', 'b']
    ])
      .pipeThrough(new CSVTransformer(() => '["abc", "def"]', { handleHeaders: true, rawOutput: true }))
      .pipeTo(csvStreamEqualWritable([
        ['abc', 'def'],
        ['abc', 'def']
      ]));
  });
  test('handles ArrayLike outputs', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['a', 'b']
    ])
      .pipeThrough(new CSVTransformer(() => ({ length: 2, 0: 'a2', 1: 'b2' })))
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a2', 'b2']
      ]));
  });
  test('consumes input row without output row when null is returned', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['should be deleted', 'should be deleted'],
      ['a', 'b'],
      ['should be deleted', 'should be deleted'],
      ['a', 'b'],
      ['should be deleted', 'should be deleted'],
      ['a', 'b'],
      ['should be deleted', 'should be deleted']
    ])
      .pipeThrough(new CSVTransformer(r => r.length % 2 ? r : null))
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a', 'b'],
        ['a', 'b'],
        ['a', 'b'],
      ]));
  });
  test('passes through header row when handleHeaders is true', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['a', 'b'],
      ['1', '2']
    ])
      .pipeThrough(new CSVTransformer(r => r.map(f => `${f}-modified`), { handleHeaders: true }))
      .pipeTo(csvStreamEqualWritable([
        ['a-modified', 'b-modified'],
        ['1-modified', '2-modified']
      ]));
  });
  test('replaces header row when handleHeaders is Array<any>', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['a', 'b'],
      ['1', '2']
    ])
      .pipeThrough(new CSVTransformer(r => r, { handleHeaders: ['aaa', 'bbb'] }))
      .pipeTo(csvStreamEqualWritable([
        ['aaa', 'bbb'],
        ['1', '2']
      ]));
  });
  test('processes header row when handleHeaders is TransformationFunction', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['a', 'b'],
      ['1', '2']
    ])
      .pipeThrough(new CSVTransformer(r => r, { handleHeaders: h => [...h, 'new header'] }))
      .pipeTo(csvStreamEqualWritable([
        ['a', 'b', 'new header'],
        ['1', '2']
      ]));
  });
  test('deletes header row when handleHeaders returns null', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['a', 'b'],
      ['1', '2']
    ])
      .pipeThrough(new CSVTransformer(r => r, { handleHeaders: () => null }))
      .pipeTo(csvStreamEqualWritable([
        ['1', '2']
      ]));
  });
  test('ignores handleHeaders when row is an array of objects (normalized)', { concurrency: true }, async () => {
    await createCSVMockStream([
      [{ value: 'a1' }, { value: 'b1' }],
      [{ value: 'a2' }, { value: 'b2' }],
    ])
      .pipeThrough(new CSVTransformer(r => r.map(f => f.value), { handleHeaders: ['updatedA', 'updatedB'] }))
      .pipeTo(csvStreamEqualWritable([
        ['a1', 'b1'],
        ['a2', 'b2']
      ]));
  });
  test('enqueues multiple rows when Array<Array<any>> is returned', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['a', 'b']
    ])
      .pipeThrough(new CSVTransformer(r => ([r, r, r, r])))
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a', 'b'],
        ['a', 'b'],
        ['a', 'b'],
        ['a', 'b']
      ]));
  });
  test('passes errors to onError function', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA'],
      ['1'],
      ['2']
    ])
      .pipeThrough(new CSVTransformer(() => { throw new Error(); }, { onError: r => [`${r[0]}: caught`] }))
      .pipeTo(csvStreamEqualWritable([
        ['columnA'],
        ['1: caught'],
        ['2: caught']
      ]));
  });
  test('passes handleHeaders to onError if handleHeaders is a function', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA'],
      ['1'],
      ['2']
    ])
      .pipeThrough(new CSVTransformer(r => r, { handleHeaders: () => { throw new Error() }, onError: r => [`${r[0]}: caught`] }))
      .pipeTo(csvStreamEqualWritable([
        ['columnA: caught'],
        ['1'],
        ['2']
      ]));
  });
  test('bubbles up errors re-raised by onError function', { concurrency: true }, async () => {
    await rejects(
      createCSVMockStream([
        ['columnA'],
        ['1']
      ])
        .pipeThrough(new CSVTransformer(() => { throw new Error('outer'); }, { onError: () => { throw new Error('inner') } }))
        .pipeTo(csvStreamEqualWritable([
          ['columnA'],
          ['1']
        ])),
      { name: 'Error', message: 'inner' }
    );
  });
  test('raises new error if transformation function rejects with non-error type', { concurrency: true }, async () => {
    await rejects(
      createCSVMockStream([
        ['columnA'],
        ['1']
      ])
        .pipeThrough(new CSVTransformer(() => Promise.reject('Not an error type')))
        .pipeTo(csvStreamEqualWritable([
          ['columnA'],
          ['1']
        ])),
      { name: 'Error', message: 'Not an error type' }
    );
  });
  test('creates batch of 1 when maxBatchSize is equal to 1', { concurrency: true }, async (t) => {
    const fn = t.mock.fn(b => b.map(r => r.map(f => Number(f) * 2)));
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['1', '2'],
      ['3', '4'],
      ['5', '6']
    ])
      .pipeThrough(new CSVTransformer(fn, { maxBatchSize: 1 }))
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        [2, 4],
        [6, 8],
        [10, 12]
      ]));
    deepStrictEqual(fn.mock.callCount(), 3);
  });
  test('creates one batch when maxBatchSize is greater than CSV row length', { concurrency: true }, async (t) => {
    const fn = t.mock.fn(b => b.map(r => r.map(f => Number(f) * 2)));
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['1', '2'],
      ['3', '4'],
      ['5', '6']
    ])
      .pipeThrough(new CSVTransformer(fn, { maxBatchSize: 100 }))
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        [2, 4],
        [6, 8],
        [10, 12]
      ]));
    deepStrictEqual(fn.mock.callCount(), 1);
  });
  test('creates one batch when maxBatchSize is equal to CSV row length', { concurrency: true }, async (t) => {
    const fn = t.mock.fn(b => b.map(r => r.map(f => Number(f) * 2)));
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['1', '2'],
      ['3', '4'],
      ['5', '6']
    ])
      .pipeThrough(new CSVTransformer(fn, { maxBatchSize: 3 }))
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        [2, 4],
        [6, 8],
        [10, 12]
      ]));
    deepStrictEqual(fn.mock.callCount(), 1);
  });
  test('creates multiple batches when maxBatchSize is less than CSV row length', { concurrency: true }, async (t) => {
    const fn = t.mock.fn(b => b.map(r => r.map(f => Number(f) * 2)));
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['1', '2'],
      ['3', '4'],
      ['5', '6']
    ])
      .pipeThrough(new CSVTransformer(fn, { maxBatchSize: 2 }))
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        [2, 4],
        [6, 8],
        [10, 12]
      ]));
    deepStrictEqual(fn.mock.callCount(), 2);
  });
});

suite('CSVWriter', { concurrency: true }, () => {
  const vectors = [
    {
      name: 'writes simple CSV',
      csv: [
        ['column1', 'column2', 'column3'],
        ['abc', 'def', 'ghi'],
        ['123', '456', '789'],
        ['aaa', 'bbb', 'ccc']
      ]
    },
    {
      name: 'writes escaping CSV',
      csv: [
        ['name', 'value'],
        ['Three spaces', '   '],
        ['Three commas', ',,,'],
        ['Three newlines', '\r\n\r\n\r\n'],
        ['Three unescaped double quotes', 'a"""'],
        ['Three escaped double quotes', '"""'],
        ['Unescaped double quotes around delimiter 1 "', 'a'],
        ['Unescaped double quotes around delimiter 2 ""', 'a'],
        ['Unescaped double quotes around delimiter 3 """', 'a'],
        ['Unescaped double quotes around delimiter 4', ' "'],
        ['Unescaped double quotes around delimiter 5', ' ""'],
        ['Unescaped double quotes around delimiter 6', ' """'],
        ['Spaces before and after a value', '   abc   '],
        ['Spaces before and after three unescaped double quotes', '   """   '],
        ['Spaces trailing escaped double quotes', 'abc   '],
        ['Characters trailing escaped double quotes', 'abc def'],
        ['Unescaped double quotes trailing escaped double quotes', 'abc " def 123 """ 456'],
        ['Unescaped "double quotes" trailing escaped double quotes', 'abc """ def 123 """ 456'],
        ['Unicode test 1', '你好'],
        ['Unicode test 2', '😂👌👍'],
        ['Unicode test 3', '🏴‍☠️👨‍👩‍👧‍👦'],
        ['Mixed', ',\r\n",\r\n",\r\n"']
      ]
    },
    {
      name: 'writes escaping edges CSV',
      csv: [
        [',,,', '"""'],
        ['"""', ',,,'],
        ['a"b""', 'a"b""'],
        ['a"b""', 'a"b""']
      ]
    },
    {
      name: 'writes sparse CSV',
      csv: [
        ['column1', 'column2', 'column3'],
        [''],
        ['', ''],
        ['', '', ''],
        ['', '', '', ''],
        ['', '', '', '', ''],
        ['aaa', 'bbb', 'ccc'],
        ['111', '222', '333', '444', '555', '666', '777', '888'],
        ['', 'hhh', 'iii'],
        ['ggg', '', 'iii'],
        ['ggg', 'hhh', ''],
        ['', 'hhh', ''],
        ['ggg', '', ''],
        ['', '', 'iii'],
        ['', '', ''],
        ['', '', ''],
        ['', '', ''],
        [''],
        ['']
      ]
    },
    {
      name: 'writes single value (no commas or newlines) CSV',
      csv: [
        ['abc']
      ]
    },
    {
      name: 'writes single column (no commas) CSV',
      csv: [
        ['abc'],
        ['def'],
        ['ghi']
      ]
    },
    {
      name: 'writes single row (no newlines) CSV',
      csv: [
        ['abc', 'def', 'ghi']
      ]
    },
    {
      name: 'writes blank CSV',
      csv: [
        ['']
      ]
    }
  ];
  for (const { name, csv } of vectors) {
    test(name, { concurrency: true }, async (t) => {
      const temp = await createTempFile();
      t.after(async () => await unlink(temp));
      await createCSVMockStream(csv).pipeTo(new CSVWriter(temp));
      await new CSVReader(temp).pipeTo(csvStreamEqualWritable(csv));
    });
  }
  test('writes raw CSV string', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    await createCSVMockStream([['']])
      .pipeThrough(new CSVTransformer(() => 'row 1,a\r\nrow 2,b\r\nrow 3,c', { handleHeaders: true, rawOutput: true }))
      .pipeTo(new CSVWriter(temp));
    await new CSVReader(temp).pipeTo(
      csvStreamEqualWritable([
        ['row 1', 'a'],
        ['row 2', 'b'],
        ['row 3', 'c']
      ]));
  });
  test('flushes progress to disk on uncaught error', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const writer = new CSVWriter(temp);
    await rejects(async () => {
      await ReadableStream.from((function* () {
        yield '["header1","header2"]';
        yield '["1","1"]';
        throw new Error();
      })()).pipeTo(writer)
    });
    deepStrictEqual(writer.status.done, true); // Confirm cleanup actions
    await new CSVReader(temp).pipeTo( // Confirm flush to disk
      csvStreamEqualWritable([
        ['header1', 'header2'],
        ['1', '1'],
      ]));
  });
  test('can close', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const stream = new CSVWriter(temp);
    const writer = stream.getWriter();
    await writer.write('a');
    writer.releaseLock();
    await stream.close();
    await rejects(writer.write('b'));
    await new CSVReader(temp).pipeTo(csvStreamEqualWritable([['a']]));
  });
  test('can abort', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const stream = new CSVWriter(temp);
    const writer = stream.getWriter();
    await writer.write('a');
    writer.releaseLock();
    await stream.abort();
    await rejects(writer.write('b'));
    await new CSVReader(temp).pipeTo(csvStreamEqualWritable([['a']]));
  });
});

suite('CSVWriter status', { concurrency: true }, () => {
  test('records file name', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const writer = new CSVWriter(temp);
    await createCSVMockStream([['']]).pipeTo(writer);
    deepStrictEqual(writer.status.name, basename(temp));
  });
  test('records elapsed time', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const writer = new CSVWriter(temp);
    await createCSVMockStream([['']]).pipeTo(writer);
    ok(writer.status.elapsed > 0);
  });
  test('elapsed time changes while writing', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const writer = new CSVWriter(temp);
    const elapsedArray = [];
    await createCSVMockStream([[''], [''], [''], ['']])
      .pipeThrough(new CSVTransformer(async r => {
        const { elapsed } = writer.status;
        elapsedArray.push(elapsed);
        await setTimeout(5);
        return r;
      }, { handleHeaders: true }))
      .pipeTo(writer);
    const { elapsed } = writer.status;
    elapsedArray.push(elapsed);
    const uniqueElapsed = new Set(elapsedArray);
    deepStrictEqual(elapsedArray.length, 5);
    deepStrictEqual(elapsedArray.length, uniqueElapsed.size);
  });
  test('elapsed time does not change after finishing writing', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const writer = new CSVWriter(temp);
    await createCSVMockStream([['']]).pipeTo(writer);
    const { elapsed: elapsed1 } = writer.status;
    await setTimeout(5);
    const { elapsed: elapsed2 } = writer.status;
    deepStrictEqual(elapsed1, elapsed2);
  });
  test('records rows written', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const writer = new CSVWriter(temp);
    await createCSVMockStream([[''], [''], ['']]).pipeTo(writer);
    deepStrictEqual(writer.status.rows, 3);
  });
  test('records done status', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const writer = new CSVWriter(temp);
    await createCSVMockStream([['']]).pipeTo(writer);
    ok(writer.status.done);
  });
  test('counts raw CSV string as one row', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const writer = new CSVWriter(temp);
    await createCSVMockStream([['']])
      .pipeThrough(new CSVTransformer(() => 'row 1,a\r\nrow 2,b\r\nrow 3,c', { handleHeaders: true, rawOutput: true }))
      .pipeTo(writer);
    deepStrictEqual(writer.status.rows, 1);
    await new CSVReader(temp)
      .pipeTo(csvStreamEqualWritable([
        ['row 1', 'a'],
        ['row 2', 'b'],
        ['row 3', 'c']
      ]));
  });
});