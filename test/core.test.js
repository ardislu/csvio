import { suite, test } from 'node:test';
import { deepStrictEqual } from 'node:assert/strict';
import { unlink } from 'node:fs/promises';

import { csvStreamEqual, csvStreamEqualWritable, createCSVMockStream, createTempFile } from './utils.js';
import { arrayToCSVString, createCSVReadableStream, createCSVTransformStream, createCSVWritableStream } from '../src/core.js';

suite('arrayToCSVString', { concurrency: true }, () => {
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
      deepStrictEqual(arrayToCSVString(input), output);
    });
  }
});

suite('createCSVReadableStream', { concurrency: true }, () => {
  test('parses simple CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/simple.csv');
    await csvStreamEqual(stream, [
      ['column1', 'column2', 'column3'],
      ['abc', 'def', 'ghi'],
      ['123', '456', '789'],
      ['aaa', 'bbb', 'ccc']
    ]);
  });
  test('parses escaping CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/escaping.csv');
    await csvStreamEqual(stream, [
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
    ]);
  });
  test('parses escaping edges CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/escaping-edges.csv');
    await csvStreamEqual(stream, [
      [',,,', '"""'],
      ['"""', ',,,'],
      ['a"b""', 'a"b""'],
      ['a"b""', 'a"b""']
    ]);
  });
  test('parses sparse CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/sparse.csv');
    await csvStreamEqual(stream, [
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
    ]);
  });
  test('parses line feed end-of-line CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/lf-eol.csv');
    await csvStreamEqual(stream, [
      ['column1', 'column2'],
      ['a', 'b'],
      ['c', 'd'],
      ['1', '2'],
      ['3', '4']
    ]);
  });
  test('parses byte order mark CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/bom.csv');
    await csvStreamEqual(stream, [
      ['column1', 'column2'],
      ['ab', 'cd'],
      ['12', '34']
    ]);
  });
  test('parses single value (no commas or newlines) CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/single-value.csv');
    await csvStreamEqual(stream, [
      ['abc']
    ]);
  });
  test('parses single column (no commas) CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/single-column.csv');
    await csvStreamEqual(stream, [
      ['abc'],
      ['def'],
      ['ghi']
    ]);
  });
  test('parses single row (no newlines) CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/single-row.csv');
    await csvStreamEqual(stream, [
      ['abc', 'def', 'ghi']
    ]);
  });
  test('parses blank CSV', { concurrency: true }, async () => {
    const stream = createCSVReadableStream('./test/data/blank.csv');
    await csvStreamEqual(stream, [
      ['']
    ]);
  });
});

suite('createCSVTransformStream', { concurrency: true }, () => {
  test('transforms data in place', { concurrency: true }, async () => {
    function timesTwo(row) {
      return [Number(row[0]) * 2, Number(row[1]) * 2];
    }
    const stream = createCSVMockStream([
      ['columnA', 'columnB'],
      ['1', '1'],
      ['100', '100'],
      ['223423', '455947'],
      ['348553', '692708'],
      ['536368', '676147']
    ]).pipeThrough(createCSVTransformStream(timesTwo));
    await csvStreamEqual(stream, [
      ['columnA', 'columnB'],
      [2, 2],
      [200, 200],
      [446846, 911894],
      [697106, 1385416],
      [1072736, 1352294]
    ]);
  });
  test('can add new column', { concurrency: true }, async () => {
    let firstRow = true;
    function sum(row) {
      if (firstRow) {
        firstRow = false;
        return [...row, 'sum'];
      }
      return [...row, Number(row[0]) + Number(row[1])];
    }
    const stream = createCSVMockStream([
      ['columnA', 'columnB'],
      ['1', '1'],
      ['100', '100'],
      ['223423', '455947'],
      ['348553', '692708'],
      ['536368', '676147']
    ]).pipeThrough(createCSVTransformStream(sum, { includeHeaders: true }));
    await csvStreamEqual(stream, [
      ['columnA', 'columnB', 'sum'],
      ['1', '1', 2],
      ['100', '100', 200],
      ['223423', '455947', 679370],
      ['348553', '692708', 1041261],
      ['536368', '676147', 1212515]
    ]);
  });
});

suite('createCSVWritableStream', { concurrency: true }, () => {
  test('writes simple CSV', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const csv = [
      ['column1', 'column2', 'column3'],
      ['abc', 'def', 'ghi'],
      ['123', '456', '789'],
      ['aaa', 'bbb', 'ccc']
    ];
    await createCSVMockStream(csv).pipeTo(createCSVWritableStream(temp));
    await createCSVReadableStream(temp).pipeTo(csvStreamEqualWritable(csv));
  });
  test('writes escaping CSV', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const csv = [
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
    ];
    await createCSVMockStream(csv).pipeTo(createCSVWritableStream(temp));
    await createCSVReadableStream(temp).pipeTo(csvStreamEqualWritable(csv));
  });
  test('writes escaping edges CSV', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const csv = [
      [',,,', '"""'],
      ['"""', ',,,'],
      ['a"b""', 'a"b""'],
      ['a"b""', 'a"b""']
    ];
    await createCSVMockStream(csv).pipeTo(createCSVWritableStream(temp));
    await createCSVReadableStream(temp).pipeTo(csvStreamEqualWritable(csv));
  });
  test('writes sparse CSV', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const csv = [
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
    ];
    await createCSVMockStream(csv).pipeTo(createCSVWritableStream(temp));
    await createCSVReadableStream(temp).pipeTo(csvStreamEqualWritable(csv));
  });
  test('writes single value (no commas or newlines)', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const csv = [
      ['abc']
    ];
    await createCSVMockStream(csv).pipeTo(createCSVWritableStream(temp));
    await createCSVReadableStream(temp).pipeTo(csvStreamEqualWritable(csv));
  });
  test('writes single row (no newlines) CSV', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const csv = [
      ['abc', 'def', 'ghi']
    ];
    await createCSVMockStream(csv).pipeTo(createCSVWritableStream(temp));
    await createCSVReadableStream(temp).pipeTo(csvStreamEqualWritable(csv));
  });
  test('writes blank CSV', { concurrency: true }, async (t) => {
    const temp = await createTempFile();
    t.after(async () => await unlink(temp));
    const csv = [
      ['']
    ];
    await createCSVMockStream(csv).pipeTo(createCSVWritableStream(temp));
    await createCSVReadableStream(temp).pipeTo(csvStreamEqualWritable(csv));
  });
});