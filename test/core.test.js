import { suite, test } from 'node:test';
import { deepStrictEqual, throws } from 'node:assert/strict';
import { unlink } from 'node:fs/promises';
import { normalize, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import { csvStreamEqualWritable, csvStreamNotEqualWritable, createCSVMockStream, createTempFile } from './utils.js';
import { arrayToCSVString, parsePathLike, createCSVReadableStream, createCSVTransformStream, createCSVWritableStream } from '../src/core.js';

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

suite('parsePathLike', { concurrency: true }, () => {
  const vectors = [
    { name: 'parses string relative path', input: './folder/example.txt', output: './folder/example.txt' },
    { name: 'parses string absolute path', input: '/absolute/path/to/file.txt', output: '/absolute/path/to/file.txt' },
    { name: 'parses Buffer relative path', input: new TextEncoder().encode('./folder/example.txt'), output: './folder/example.txt' },
    { name: 'parses Buffer absolute path', input: new TextEncoder().encode('/absolute/path/to/file.txt'), output: '/absolute/path/to/file.txt' },
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
        throws(() => parsePathLike(input), error);
      }
      else {
        deepStrictEqual(parsePathLike(input), normalize(output));
      }
    });
  }
});

suite('createCSVReadableStream', { concurrency: true }, () => {
  const vectors = [
    {
      name: 'parses simple CSV',
      input: './test/data/simple.csv',
      output: [
        ['column1', 'column2', 'column3'],
        ['abc', 'def', 'ghi'],
        ['123', '456', '789'],
        ['aaa', 'bbb', 'ccc']
      ]
    },
    {
      name: 'parses escaping CSV',
      input: './test/data/escaping.csv',
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
      input: './test/data/escaping-edges.csv',
      output: [
        [',,,', '"""'],
        ['"""', ',,,'],
        ['a"b""', 'a"b""'],
        ['a"b""', 'a"b""']
      ]
    },
    {
      name: 'parses sparse CSV',
      input: './test/data/sparse.csv',
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
      input: './test/data/lf-eol.csv',
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
      input: './test/data/bom.csv',
      output: [
        ['column1', 'column2'],
        ['ab', 'cd'],
        ['12', '34']
      ]
    },
    {
      name: 'parses single value (no commas or newlines) CSV',
      input: './test/data/single-value.csv',
      output: [
        ['abc']
      ]
    },
    {
      name: 'parses single column (no commas) CSV',
      input: './test/data/single-column.csv',
      output: [
        ['abc'],
        ['def'],
        ['ghi']
      ]
    },
    {
      name: 'parses single row (no newlines) CSV',
      input: './test/data/single-row.csv',
      output: [
        ['abc', 'def', 'ghi']
      ]
    },
    {
      name: 'parses blank CSV',
      input: './test/data/blank.csv',
      output: [
        ['']
      ],
      negativeOutput: [
        ['a']
      ]
    }
  ];
  for (const { name, input, output, negativeOutput } of vectors) {
    test(name, { concurrency: true }, async () => {
      await createCSVReadableStream(input).pipeTo(csvStreamEqualWritable(output));
      if (negativeOutput !== undefined) {
        await createCSVReadableStream(input).pipeTo(csvStreamNotEqualWritable(negativeOutput));
      }
    });
  }
});

suite('createCSVTransformStream', { concurrency: true }, () => {
  test('passes through CSV', { concurrency: true }, async () => {
    const csv = [
      ['columnA', 'columnB'],
      ['a', 'b']
    ];
    await createCSVMockStream(csv)
      .pipeThrough(createCSVTransformStream(r => r))
      .pipeTo(csvStreamEqualWritable(csv));
  });
  test('passes through raw output', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['a', 'b']
    ])
      .pipeThrough(createCSVTransformStream(() => '["abc", "def"]', { includeHeaders: true, rawOutput: true }))
      .pipeTo(csvStreamEqualWritable([
        ['abc', 'def'],
        ['abc', 'def']
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
      .pipeThrough(createCSVTransformStream(r => r.length % 2 ? r : null))
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a', 'b'],
        ['a', 'b'],
        ['a', 'b'],
      ]));
  });
  test('enqueues multiple rows when Array<Array<any>> is returned', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['a', 'b']
    ])
      .pipeThrough(createCSVTransformStream(r => ([r, r, r, r])))
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a', 'b'],
        ['a', 'b'],
        ['a', 'b'],
        ['a', 'b']
      ]));
  });
});

suite('createCSVWritableStream', { concurrency: true }, () => {
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
      await createCSVMockStream(csv).pipeTo(createCSVWritableStream(temp));
      await createCSVReadableStream(temp).pipeTo(csvStreamEqualWritable(csv));
    });
  }
});