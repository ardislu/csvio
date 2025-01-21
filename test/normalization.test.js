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
  test('parses numbers', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelNumber('123'), 123);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('123.456'), 123.456);
  });
  test('fixes numbers mangled to dates', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelNumber('1/1/1900'), 1);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('1/2/1900'), 2);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('2/28/1900'), 59);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('2/29/1900'), 60);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('3/1/1900'), 61);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('12/31/1969'), 25568);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('1/1/1970'), 25569);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('1/2/1970'), 25570);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('12/31/2023'), 45291);
    deepStrictEqual(CSVNormalizer.fixExcelNumber('1/1/2024'), 45292);
  });
  test('fixes numbers mangled to accounting format', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelNumber(' $123.00 '), 123);
    deepStrictEqual(CSVNormalizer.fixExcelNumber(' $123.45 '), 123.45);
    deepStrictEqual(CSVNormalizer.fixExcelNumber(' $10,000,000.00 '), 10000000);
  });
  test('passes through non-number values', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelNumber('abc'), 'abc');
  });
  test('passes through blank value', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelNumber(''), '');
  });
});

suite('CSVNormalizer.fixExcelBigInt', { concurrency: true }, () => {
  test('parses BigInt', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelBigInt('123'), '123');
    deepStrictEqual(CSVNormalizer.fixExcelBigInt('123456'), '123456');
    deepStrictEqual(CSVNormalizer.fixExcelBigInt('123456789123456789123456789'), '123456789123456789123456789');
  });
  test('fixes BigInt mangled to scientific notation', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelBigInt('1E+18'), '1000000000000000000');
    deepStrictEqual(CSVNormalizer.fixExcelBigInt('1.123456789123456789E18'), '1123456789123456789');
    deepStrictEqual(CSVNormalizer.fixExcelBigInt('1.1234567891234567891234E18'), '1123456789123456789');
  });
  test('fixes BigInt mangled to accounting format', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelBigInt(' $123.00 '), '123');
    deepStrictEqual(CSVNormalizer.fixExcelBigInt(' $123.45 '), '123');
    deepStrictEqual(CSVNormalizer.fixExcelBigInt('  $1,123,123,123,123,123,123.12  '), '1123123123123123123');
  });
  test('passes through non-BigInt values', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelBigInt('abc'), 'abc');
  });
  test('passes through blank value', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelBigInt(''), '');
  });
});

suite('CSVNormalizer.fixExcelDate', { concurrency: true }, () => {
  test('parses dates', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelDate('12/31/24').valueOf(), new Date('12/31/24').valueOf());
  });
  test('fixes dates mangled to numbers', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelDate('1').valueOf(), new Date('1900-01-01T00:00:00.000Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('2').valueOf(), new Date('1900-01-02T00:00:00.000Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('59').valueOf(), new Date('1900-02-28T00:00:00.000Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('60').valueOf(), new Date('1900-02-29T00:00:00.000Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('61').valueOf(), new Date('1900-03-01T00:00:00.000Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('25568').valueOf(), new Date('1969-12-31T00:00:00.000Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('25569').valueOf(), new Date('1970-01-01T00:00:00.000Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('25570').valueOf(), new Date('1970-01-02T00:00:00.000Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('45291').valueOf(), new Date('2023-12-31T00:00:00.000Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('45292').valueOf(), new Date('2024-01-01T00:00:00.000Z').valueOf());
  });
  test('fixes date times mangled to numbers', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelDate('43915.31372').valueOf(), new Date('2020-03-25T07:31:45.407Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('44067.63').valueOf(), new Date('2020-08-24T15:07:11.999Z').valueOf());
    deepStrictEqual(CSVNormalizer.fixExcelDate('44309.63502').valueOf(), new Date('2021-04-23T15:14:25.728Z').valueOf());
  });
  test('passes through non-date values', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelDate('abc'), 'abc');
  });
  test('passes through blank value', { concurrency: true }, () => {
    deepStrictEqual(CSVNormalizer.fixExcelDate(''), '');
  });
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
      ['',''],
      ['a', 'b'],
      ['',''],
      ['',''],
      ['','']
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