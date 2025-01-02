import { suite, test } from 'node:test';
import { deepStrictEqual } from 'node:assert/strict';

import { csvStreamEqual, csvStreamEqualWritable, createCSVMockStream } from './utils.js';
import { createCSVReadableStream } from '../src/core.js';
import { toCamelCase, expandScientificNotation, fixExcelNumber, fixExcelBigInt, fixExcelDate, createCSVNormalizationStream, createCSVDenormalizationStream } from '../src/normalization.js';

suite('toCamelCase', { concurrency: true }, () => {
  test('does not modify strings already in camelCase', { concurrency: true }, () => {
    const test1 = 'example';
    const test2 = 'alreadyInCamelCase';
    const test3 = 'withAcronymsABCNotDEFCapitalizedGHI';
    deepStrictEqual(toCamelCase(test1), test1);
    deepStrictEqual(toCamelCase(test2), test2);
    deepStrictEqual(toCamelCase(test3), test3);
  });
  test('converts single word with capital first letter', { concurrency: true }, () => {
    deepStrictEqual(toCamelCase('Example'), 'example');
    deepStrictEqual(toCamelCase('ExampleOne'), 'exampleOne');
  });
  test('converts strings with spaces', { concurrency: true }, () => {
    deepStrictEqual(toCamelCase('example one'), 'exampleOne');
    deepStrictEqual(toCamelCase('example two with more words'), 'exampleTwoWithMoreWords');
  });
  test('converts strings with hyphens', { concurrency: true }, () => {
    deepStrictEqual(toCamelCase('example-one'), 'exampleOne');
    deepStrictEqual(toCamelCase('example-two-with-more-words'), 'exampleTwoWithMoreWords');
  });
  test('converts strings with underscores', { concurrency: true }, () => {
    deepStrictEqual(toCamelCase('example_one'), 'exampleOne');
    deepStrictEqual(toCamelCase('example_two_with_more_words'), 'exampleTwoWithMoreWords');
  });
  test('converts strings with mixed casing', { concurrency: true }, () => {
    deepStrictEqual(toCamelCase('ExAmPlE oNe'), 'exampleOne');
    deepStrictEqual(toCamelCase('eXaMpLe TwO-wItH_mOrE wOrDs'), 'exampleTwoWithMoreWords');
  });
  test('converts strings with excess whitespace', { concurrency: true }, () => {
    deepStrictEqual(toCamelCase('   exampleOne  \r\n'), 'exampleOne');
    deepStrictEqual(toCamelCase(' \r\n\r\n    example    Two\rWith More   \r\nWords  \r\n'), 'exampleTwoWithMoreWords');
  });
});

suite('expandScientificNotation', { concurrency: true }, () => {
  test('returns null for numbers not in scientific notation', { concurrency: true }, () => {
    deepStrictEqual(expandScientificNotation('123'), null);
    deepStrictEqual(expandScientificNotation('123n'), null);
    deepStrictEqual(expandScientificNotation('123.456'), null);
  });
  test('returns null for values that are not strings of numbers', { concurrency: true }, () => {
    deepStrictEqual(expandScientificNotation('abc'), null);
    deepStrictEqual(expandScientificNotation(123), null);
    deepStrictEqual(expandScientificNotation(123n), null);
    deepStrictEqual(expandScientificNotation([]), null);
    deepStrictEqual(expandScientificNotation({}), null);
  });
  test('returns expanded form for positive exponents', { concurrency: true }, () => {
    deepStrictEqual(expandScientificNotation('1E2'), '100');
    deepStrictEqual(expandScientificNotation('1E10'), '10000000000');
    deepStrictEqual(expandScientificNotation('1E18'), '1000000000000000000');
    deepStrictEqual(expandScientificNotation('1E+18'), '1000000000000000000');
  });
  test('returns expanded form for negative mantissas', { concurrency: true }, () => {
    deepStrictEqual(expandScientificNotation('-1E2'), '-100');
    deepStrictEqual(expandScientificNotation('-1E10'), '-10000000000');
    deepStrictEqual(expandScientificNotation('-1E18'), '-1000000000000000000');
    deepStrictEqual(expandScientificNotation('-1E+18'), '-1000000000000000000');
  });
  test('returns expanded form for mantissas with decimals', { concurrency: true }, () => {
    deepStrictEqual(expandScientificNotation('1.1E2'), '110');
    deepStrictEqual(expandScientificNotation('1.11E2'), '111');
    deepStrictEqual(expandScientificNotation('1.111E2'), '111.1');
    deepStrictEqual(expandScientificNotation('1.12345E10'), '11234500000');
    deepStrictEqual(expandScientificNotation('1.123456789123456789E18'), '1123456789123456789');
    deepStrictEqual(expandScientificNotation('1.1234567891234567891234E18'), '1123456789123456789.1234');
  });
  test('truncates decimals correctly', { concurrency: true }, () => {
    deepStrictEqual(expandScientificNotation('1.1E2', true), '110');
    deepStrictEqual(expandScientificNotation('1.11E2', true), '111');
    deepStrictEqual(expandScientificNotation('1.111E2', true), '111');
    deepStrictEqual(expandScientificNotation('1.1111E2', true), '111');
    deepStrictEqual(expandScientificNotation('1.11111E2', true), '111');
    deepStrictEqual(expandScientificNotation('1.111111111E10', true), '11111111110');
    deepStrictEqual(expandScientificNotation('1.1111111111E10', true), '11111111111');
    deepStrictEqual(expandScientificNotation('1.11111111111E10', true), '11111111111');
    deepStrictEqual(expandScientificNotation('1.111111111111E10', true), '11111111111');
  });
});

suite('fixExcelNumber', { concurrency: true }, () => {
  test('parses numbers', { concurrency: true }, () => {
    deepStrictEqual(fixExcelNumber('123'), 123);
    deepStrictEqual(fixExcelNumber('123.456'), 123.456);
  });
  test('fixes numbers mangled to dates', { concurrency: true }, () => {
    deepStrictEqual(fixExcelNumber('1/1/1900'), 1);
    deepStrictEqual(fixExcelNumber('1/2/1900'), 2);
    deepStrictEqual(fixExcelNumber('2/28/1900'), 59);
    deepStrictEqual(fixExcelNumber('2/29/1900'), 60);
    deepStrictEqual(fixExcelNumber('3/1/1900'), 61);
    deepStrictEqual(fixExcelNumber('12/31/1969'), 25568);
    deepStrictEqual(fixExcelNumber('1/1/1970'), 25569);
    deepStrictEqual(fixExcelNumber('1/2/1970'), 25570);
    deepStrictEqual(fixExcelNumber('12/31/2023'), 45291);
    deepStrictEqual(fixExcelNumber('1/1/2024'), 45292);
  });
  test('fixes numbers mangled to accounting format', { concurrency: true }, () => {
    deepStrictEqual(fixExcelNumber(' $123.00 '), 123);
    deepStrictEqual(fixExcelNumber(' $123.45 '), 123.45);
    deepStrictEqual(fixExcelNumber(' $10,000,000.00 '), 10000000);
  });
  test('passes through non-number values', { concurrency: true }, () => {
    deepStrictEqual(fixExcelNumber('abc'), 'abc');
  });
  test('passes through blank value', { concurrency: true }, () => {
    deepStrictEqual(fixExcelNumber(''), '');
  });
});

suite('fixExcelBigInt', { concurrency: true }, () => {
  test('parses BigInt', { concurrency: true }, () => {
    deepStrictEqual(fixExcelBigInt('123'), '123');
    deepStrictEqual(fixExcelBigInt('123456'), '123456');
    deepStrictEqual(fixExcelBigInt('123456789123456789123456789'), '123456789123456789123456789');
  });
  test('fixes BigInt mangled to scientific notation', { concurrency: true }, () => {
    deepStrictEqual(fixExcelBigInt('1E+18'), '1000000000000000000');
    deepStrictEqual(fixExcelBigInt('1.123456789123456789E18'), '1123456789123456789');
    deepStrictEqual(fixExcelBigInt('1.1234567891234567891234E18'), '1123456789123456789');
  });
  test('fixes BigInt mangled to accounting format', { concurrency: true }, () => {
    deepStrictEqual(fixExcelBigInt(' $123.00 '), '123');
    deepStrictEqual(fixExcelBigInt(' $123.45 '), '123');
    deepStrictEqual(fixExcelBigInt('  $1,123,123,123,123,123,123.12  '), '1123123123123123123');
  });
  test('passes through non-BigInt values', { concurrency: true }, () => {
    deepStrictEqual(fixExcelBigInt('abc'), 'abc');
  });
  test('passes through blank value', { concurrency: true }, () => {
    deepStrictEqual(fixExcelBigInt(''), '');
  });
});

suite('fixExcelDate', { concurrency: true }, () => {
  test('parses dates', { concurrency: true }, () => {
    deepStrictEqual(fixExcelDate('12/31/24').valueOf(), new Date('12/31/24').valueOf());
  });
  test('fixes dates mangled to numbers', { concurrency: true }, () => {
    deepStrictEqual(fixExcelDate('1').valueOf(), new Date('1900-01-01T00:00:00.000Z').valueOf());
    deepStrictEqual(fixExcelDate('2').valueOf(), new Date('1900-01-02T00:00:00.000Z').valueOf());
    deepStrictEqual(fixExcelDate('59').valueOf(), new Date('1900-02-28T00:00:00.000Z').valueOf());
    deepStrictEqual(fixExcelDate('60').valueOf(), new Date('1900-02-29T00:00:00.000Z').valueOf());
    deepStrictEqual(fixExcelDate('61').valueOf(), new Date('1900-03-01T00:00:00.000Z').valueOf());
    deepStrictEqual(fixExcelDate('25568').valueOf(), new Date('1969-12-31T00:00:00.000Z').valueOf());
    deepStrictEqual(fixExcelDate('25569').valueOf(), new Date('1970-01-01T00:00:00.000Z').valueOf());
    deepStrictEqual(fixExcelDate('25570').valueOf(), new Date('1970-01-02T00:00:00.000Z').valueOf());
    deepStrictEqual(fixExcelDate('45291').valueOf(), new Date('2023-12-31T00:00:00.000Z').valueOf());
    deepStrictEqual(fixExcelDate('45292').valueOf(), new Date('2024-01-01T00:00:00.000Z').valueOf());
  });
  test('fixes date times mangled to numbers', { concurrency: true }, () => {
    deepStrictEqual(fixExcelDate('43915.31372').valueOf(), new Date('2020-03-25T07:31:45.407Z').valueOf());
    deepStrictEqual(fixExcelDate('44067.63').valueOf(), new Date('2020-08-24T15:07:11.999Z').valueOf());
    deepStrictEqual(fixExcelDate('44309.63502').valueOf(), new Date('2021-04-23T15:14:25.728Z').valueOf());
  });
  test('passes through non-date values', { concurrency: true }, () => {
    deepStrictEqual(fixExcelDate('abc'), 'abc');
  });
  test('passes through blank value', { concurrency: true }, () => {
    deepStrictEqual(fixExcelDate(''), '');
  });
});

suite('createCSVNormalizationStream', { concurrency: true }, () => {
  test('can remove extra columns', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['extra1', 'columnA', 'extra2', 'extra3', 'columnB'],
      ['123', 'a', '123', '123', 'b']
    ])
      .pipeThrough(createCSVNormalizationStream([
        { name: 'columnA', type: 'string' },
        { name: 'columnB', type: 'string' }
      ]))
      .pipeThrough(createCSVDenormalizationStream())
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
      .pipeThrough(createCSVNormalizationStream([
        { name: 'columnA', type: 'string' },
        { name: 'columnB', type: 'string' },
        { name: 'columnC', type: 'string' }
      ]))
      .pipeThrough(createCSVDenormalizationStream())
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
      .pipeThrough(createCSVNormalizationStream([
        { name: 'columnA', type: 'string', displayName: 'Column A' },
        { name: 'columnB', type: 'string', displayName: 'Column B' }
      ]))
      .pipeThrough(createCSVDenormalizationStream())
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
      .pipeThrough(createCSVNormalizationStream([
        { name: 'columnA', type: 'string' },
        { name: 'columnB', type: 'string' }
      ]))
      .pipeThrough(createCSVDenormalizationStream())
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
      .pipeThrough(createCSVNormalizationStream([
        { name: 'columnA', type: 'abc' },
        { name: 'columnB', type: 'def' }
      ]))
      .pipeThrough(createCSVDenormalizationStream())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a', 'b']
      ]));
  });
});

suite('createCSVNormalizationStream and createCSVDenormalizationStream end-to-end', { concurrency: true }, () => {
  test('can normalize', { concurrency: true }, async () => {
    const headers = [
      { name: 'stringCol', displayName: 'String Column', type: 'string', defaultValue: 'N/A' },
      { name: 'numberCol', displayName: 'Number Column', type: 'number' },
      { name: 'bigintCol', displayName: 'BigInt Column', type: 'bigint' },
      { name: 'dateCol', displayName: 'Date Column', type: 'date' }
    ]
    const stream = createCSVReadableStream('./test/data/normalization.csv')
      .pipeThrough(createCSVNormalizationStream(headers))
      .pipeThrough(createCSVDenormalizationStream());
    await csvStreamEqual(stream, [
      ['String Column', 'Number Column', 'BigInt Column', 'Date Column'],
      ['abc 👨‍👩‍👧‍👦', 123456789.123456, '1000000000000000000', '2024-01-01T00:00:00.000Z'],
      [', " 🏴‍☠️', 1000, '-1234567890000000000000000', '2024-06-01T00:00:00.000Z'],
      ['N/A', 123100, '1000000000000000000', '2024-12-31T08:00:00.000Z']
    ]);
  });
});