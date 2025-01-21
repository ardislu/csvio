import { suite, test } from 'node:test';
import { deepStrictEqual } from 'node:assert/strict';

import { csvStreamEqual, csvStreamEqualWritable, createCSVMockStream } from './utils.js';
import { CSVReader } from '../src/core.js';
import { toCamelCase, expandScientificNotation, CSVNormalizer, CSVDenormalizer } from '../src/normalization.js';

suite('toCamelCase', { concurrency: true }, () => {
  const vectors = [
    { name: 'does not modify lowercase word', input: 'example', output: 'example' },
    { name: 'does not modify camelCase', input: 'alreadyInCamelCase', output: 'alreadyInCamelCase' },
    { name: 'does not modify camelCase (with acronyms)', input: 'withAcronymsABCNotDEFCapitalizedGHI', output: 'withAcronymsABCNotDEFCapitalizedGHI' },
    { name: 'converts single word with capital first letter', input: 'Example', output: 'example' },
    { name: 'converts TitleCase', input: 'ExampleOne', output: 'exampleOne' },
    { name: 'converts space', input: 'example one', output: 'exampleOne' },
    { name: 'converts multiple spaces', input: 'example two with more words', output: 'exampleTwoWithMoreWords' },
    { name: 'converts hyphen', input: 'example-one', output: 'exampleOne' },
    { name: 'converts multiple hyphens', input: 'example-two-with-more-words', output: 'exampleTwoWithMoreWords' },
    { name: 'converts underscore', input: 'example_one', output: 'exampleOne' },
    { name: 'converts multiple underscores', input: 'example_two_with_more_words', output: 'exampleTwoWithMoreWords' },
    { name: 'converts mixed casing', input: 'ExAmPlE oNe', output: 'exampleOne' },
    { name: 'converts mixed casing (multiple words)', input: 'eXaMpLe TwO-wItH_mOrE wOrDs', output: 'exampleTwoWithMoreWords' },
    { name: 'converts excess whitespace', input: '   exampleOne  \r\n', output: 'exampleOne' },
    { name: 'converts excess whitespace (multiple words)', input: ' \r\n\r\n    example    Two\rWith More   \r\nWords  \r\n', output: 'exampleTwoWithMoreWords' }
  ];
  for (const { name, input, output } of vectors) {
    test(name, { concurrency: true }, () => {
      deepStrictEqual(toCamelCase(input), output);
    });
  }
});

suite('expandScientificNotation', { concurrency: true }, () => {
  const vectors = [
    { name: 'returns null for string integer', input: '123', output: null },
    { name: 'returns null for string BigInt', input: '123n', output: null },
    { name: 'returns null for string number', input: '123.456', output: null },
    { name: 'returns null for string non-numeric', input: 'abc', output: null },
    { name: 'returns null for number', input: 123, output: null },
    { name: 'returns null for BigInt', input: 123n, output: null },
    { name: 'returns null for array', input: [], output: null },
    { name: 'returns null for object', input: {}, output: null },
    { name: 'returns expanded form for positive exponent', input: '1E18', output: '1000000000000000000' },
    { name: 'returns expanded form for positive exponent (with plus)', input: '1E+18', output: '1000000000000000000' },
    { name: 'returns expanded form for negative mantissa', input: '-1E18', output: '-1000000000000000000' },
    { name: 'returns expanded form for negative mantissa (with plus)', input: '-1E+18', output: '-1000000000000000000' },
    { name: 'returns expanded form with exponent equal to 1', input: '1E1', output: '10' },
    { name: 'returns expanded form with decimals smaller than exponent', input: '1.1E2', output: '110' },
    { name: 'returns expanded form with decimals equal to exponent', input: '1.11E2', output: '111' },
    { name: 'returns expanded form with decimals greater than exponent', input: '1.111E2', output: '111.1' },
    { name: 'truncates when decimals greater than exponent by 1', input: '1.111E2', truncate: true, output: '111' },
    { name: 'truncates when decimals greater than exponent by 2', input: '1.1111E2', truncate: true, output: '111' },
  ];
  for (const { name, input, truncate = false, output } of vectors) {
    test(name, { concurrency: true }, () => {
      deepStrictEqual(expandScientificNotation(input, truncate), output);
    });
  }
});

suite('CSVNormalizer.fixExcelNumber', { concurrency: true }, () => {
  const vectors = [
    { name: 'parses integer', input: '123', output: 123 },
    { name: 'parses number', input: '123.456', output: 123.456 },
    { name: 'fixes date mangling (1/1/1900)', input: '1/1/1900', output: 1 },
    { name: 'fixes date mangling (1/2/1900)', input: '1/2/1900', output: 2 },
    { name: 'fixes date mangling (2/28/1900)', input: '2/28/1900', output: 59 },
    { name: 'fixes date mangling (2/29/1900)', input: '2/29/1900', output: 60 },
    { name: 'fixes date mangling (3/1/1900)', input: '3/1/1900', output: 61 },
    { name: 'fixes date mangling (12/31/1969)', input: '12/31/1969', output: 25568 },
    { name: 'fixes date mangling (1/1/1970)', input: '1/1/1970', output: 25569 },
    { name: 'fixes date mangling (1/2/1970)', input: '1/2/1970', output: 25570 },
    { name: 'fixes accounting mangling integer', input: ' $123.00 ', output: 123 },
    { name: 'fixes accounting mangling number', input: ' $123.45 ', output: 123.45 },
    { name: 'fixes accounting mangling with commas', input: ' $10,000,000.00 ', output: 10000000 },
    { name: 'passes through non-number value', input: 'abc', output: 'abc' },
    { name: 'passes through blank value', input: '', output: '' },
  ];
  for (const { name, input, output } of vectors) {
    test(name, { concurrency: true }, () => {
      deepStrictEqual(CSVNormalizer.fixExcelNumber(input), output);
    });
  }
});

suite('CSVNormalizer.fixExcelBigInt', { concurrency: true }, () => {
  const vectors = [
    { name: 'parses BigInt', input: '123456789123456789123456789', output: '123456789123456789123456789' },
    { name: 'fixes scientific notation mangling', input: '1E+18', output: '1000000000000000000' },
    { name: 'fixes scientific notation mangling with decimal', input: '1.123456789123456789E18', output: '1123456789123456789' },
    { name: 'fixes scientific notation mangling with decimal (truncated)', input: '1.1234567891234567891234E18', output: '1123456789123456789' },
    { name: 'fixes accounting mangling integer', input: ' $123.00 ', output: '123' },
    { name: 'fixes accounting mangling number (truncated)', input: ' $123.45 ', output: '123' },
    { name: 'fixes accounting mangling BigInt with commas', input: '  $1,123,123,123,123,123,123.12  ', output: '1123123123123123123' },
    { name: 'passes through non-BigInt values', input: 'abc', output: 'abc' },
    { name: 'passes through blank value', input: '', output: '' },
  ];
  for (const { name, input, output } of vectors) {
    test(name, { concurrency: true }, () => {
      deepStrictEqual(CSVNormalizer.fixExcelBigInt(input), output);
    });
  }
});

suite('CSVNormalizer.fixExcelDate', { concurrency: true }, () => {
  const vectors = [
    { name: 'parses date', input: '12/31/24', output: '12/31/24' },
    { name: 'fixes number mangling (1)', input: '1', output: '1900-01-01T00:00:00.000Z' },
    { name: 'fixes number mangling (2)', input: '2', output: '1900-01-02T00:00:00.000Z' },
    { name: 'fixes number mangling (59)', input: '59', output: '1900-02-28T00:00:00.000Z' },
    { name: 'fixes number mangling (60)', input: '60', output: '1900-02-29T00:00:00.000Z' },
    { name: 'fixes number mangling (61)', input: '61', output: '1900-03-01T00:00:00.000Z' },
    { name: 'fixes number mangling (25568)', input: '25568', output: '1969-12-31T00:00:00.000Z' },
    { name: 'fixes number mangling (25569)', input: '25569', output: '1970-01-01T00:00:00.000Z' },
    { name: 'fixes number mangling (25570)', input: '25570', output: '1970-01-02T00:00:00.000Z' },
    { name: 'fixes number mangling (43915.31372)', input: '43915.31372', output: '2020-03-25T07:31:45.407Z' },
    { name: 'fixes number mangling (44067.63)', input: '44067.63', output: '2020-08-24T15:07:11.999Z' },
    { name: 'fixes number mangling (44309.63502)', input: '44309.63502', output: '2021-04-23T15:14:25.728Z' },
    { name: 'passes through non-date values', input: 'abc', output: 'abc', raw: true },
    { name: 'passes through blank value', input: '', output: '', raw: true },
  ];
  for (const { name, input, output, raw = false } of vectors) {
    test(name, { concurrency: true }, () => {
      if (raw) {
        deepStrictEqual(CSVNormalizer.fixExcelDate(input), output);
      }
      else {
        deepStrictEqual(CSVNormalizer.fixExcelDate(input), new Date(output));
      }
    });
  }
});

suite('CSVNormalizer', { concurrency: true }, () => {
  test('can remove extra columns', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['extra1', 'columnA', 'extra2', 'extra3', 'columnB'],
      ['123', 'a', '123', '123', 'b']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA', type: 'string' },
        { name: 'columnB', type: 'string' }
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a', 'b']
      ]));
  });
  test('can reorder columns', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnC', 'columnA', 'columnB'],
      ['c', 'a', 'b']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA', type: 'string' },
        { name: 'columnB', type: 'string' },
        { name: 'columnC', type: 'string' }
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB', 'columnC'],
        ['a', 'b', 'c']
      ]));
  });
  test('can rename columns', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['a', 'b']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA', type: 'string', displayName: 'Column A' },
        { name: 'columnB', type: 'string', displayName: 'Column B' }
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['Column A', 'Column B'],
        ['a', 'b']
      ]));
  });
  test('can remove empty rows', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['', ''],
      ['a', 'b'],
      ['', ''],
      ['', ''],
      ['', '']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA', type: 'string' },
        { name: 'columnB', type: 'string' }
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a', 'b']
      ]));
  });
  test('can ignore incorrect data types', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['a', 'b']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA', type: 'abc' },
        { name: 'columnB', type: 'def' }
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a', 'b']
      ]));
  });
});

suite('CSVNormalizer and CSVDenormalizer end-to-end', { concurrency: true }, () => {
  test('can normalize', { concurrency: true }, async () => {
    const headers = [
      { name: 'stringCol', displayName: 'String Column', type: 'string', defaultValue: 'N/A' },
      { name: 'numberCol', displayName: 'Number Column', type: 'number' },
      { name: 'bigintCol', displayName: 'BigInt Column', type: 'bigint' },
      { name: 'dateCol', displayName: 'Date Column', type: 'date' }
    ]
    const stream = new CSVReader('./test/data/normalization.csv')
      .pipeThrough(new CSVNormalizer(headers))
      .pipeThrough(new CSVDenormalizer());
    await csvStreamEqual(stream, [
      ['String Column', 'Number Column', 'BigInt Column', 'Date Column'],
      ['abc 👨‍👩‍👧‍👦', 123456789.123456, '1000000000000000000', '2024-01-01T00:00:00.000Z'],
      [', " 🏴‍☠️', 1000, '-1234567890000000000000000', '2024-06-01T00:00:00.000Z'],
      ['N/A', 123100, '1000000000000000000', '2024-12-31T08:00:00.000Z']
    ]);
  });
});