import { suite, test } from 'node:test';
import { ok, deepStrictEqual, throws } from 'node:assert/strict';

import { csvStreamEqualWritable, createCSVMockStream, assertConsole } from './utils.js';
import { CSVReader, CSVTransformer } from '../src/core.js';
import { getDecimalSeparator, toCamelCase, expandScientificNotation, CSVNormalizer, CSVDenormalizer } from '../src/normalization.js';
/** @import { CSVNormalizerHeader } from '../src/normalization.js'; */

suite('getDecimalSeparator', { concurrency: true }, () => {
  test('returns a value if no locale is specified', { concurrency: true }, () => {
    const s = getDecimalSeparator();
    ok(typeof s === 'string');
    ok(s.length === 1);
  });
  test('returns "." for locale identifier "en-US"', { concurrency: true }, () => {
    const s = getDecimalSeparator('en-US');
    deepStrictEqual(s, '.');
  });
  test('returns "," for locale identifier "de-DE"', { concurrency: true }, () => {
    const s = getDecimalSeparator('de-DE');
    deepStrictEqual(s, ',');
  });
  test('returns "." for `Intl.Locale` object set to "en-US"', { concurrency: true }, () => {
    const s = getDecimalSeparator(new Intl.Locale('en-US'));
    deepStrictEqual(s, '.');
  });
  test('returns "," for `Intl.Locale` object set to "de-DE"', { concurrency: true }, () => {
    const s = getDecimalSeparator(new Intl.Locale('de-DE'));
    deepStrictEqual(s, ',');
  });
});

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
    { name: 'converts excess whitespace (multiple words)', input: ' \r\n\r\n    example    Two\rWith More   \r\nWords  \r\n', output: 'exampleTwoWithMoreWords' },
    { name: 'handles empty string', input: '', output: '' },
    { name: 'handles whitespace-only string', input: '  \r\n', output: '' }
  ];
  for (const { name, input, output } of vectors) {
    test(name, { concurrency: true }, () => {
      deepStrictEqual(toCamelCase(input), output);
    });
  }
});

suite('expandScientificNotation', { concurrency: true }, () => {
  const vectors = [
    { name: 'throws SyntaxError for string integer', input: '123', error: SyntaxError },
    { name: 'throws SyntaxError for string BigInt', input: '123n', error: SyntaxError },
    { name: 'throws SyntaxError for string number', input: '123.456', error: SyntaxError },
    { name: 'throws SyntaxError for string non-numeric', input: 'abc', error: SyntaxError },
    { name: 'throws TypeError for number', input: 123, error: TypeError },
    { name: 'throws TypeError for BigInt', input: 123n, error: TypeError },
    { name: 'throws TypeError for array', input: [], error: TypeError },
    { name: 'throws TypeError for object', input: {}, error: TypeError },
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
  for (const { name, input, truncate = false, output, error } of vectors) {
    test(name, { concurrency: true }, () => {
      if (error !== undefined) {
        // @ts-expect-error
        throws(() => expandScientificNotation(input, truncate), error);
      }
      else {
        deepStrictEqual(expandScientificNotation(input, truncate), output);
      }
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
    { name: 'passes through non-number value', input: 'abc', output: 'abc', consoleCounts: { warn: 1 } },
    { name: 'passes through blank value', input: '', output: '' },
  ];
  for (const { name, input, output, consoleCounts } of vectors) {
    test(name, { concurrency: true }, (t) => {
      if (consoleCounts !== undefined) {
        assertConsole(t, consoleCounts);
      }
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
    { name: 'passes through non-BigInt values', input: 'abc', output: 'abc', consoleCounts: { warn: 1 } },
    { name: 'passes through blank value', input: '', output: '' },
  ];
  for (const { name, input, output, consoleCounts } of vectors) {
    test(name, { concurrency: true }, (t) => {
      if (consoleCounts !== undefined) {
        assertConsole(t, consoleCounts);
      }
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
    { name: 'passes through non-date values', input: 'abc', output: 'abc', raw: true, consoleCounts: { warn: 1 } },
    { name: 'passes through blank value', input: '', output: '', raw: true },
  ];
  for (const { name, input, output, raw = false, consoleCounts } of vectors) {
    test(name, { concurrency: true }, (t) => {
      if (consoleCounts !== undefined) {
        assertConsole(t, consoleCounts);
      }

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
  test('can remove extra columns in CSV', { concurrency: true }, async () => {
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
  test('can remove extra columns in headers input', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['a', 'b']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA', type: 'string' },
        { name: 'columnB', type: 'string' },
        { name: 'extra1', type: 'string' },
        { name: 'extra2', type: 'string' },
        { name: 'extra3', type: 'string' }
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
  test('removes columns that have the same normalized name (last column prevails)', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'column A', 'column-A'],
      ['a1', 'a2', 'a3']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA' }
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA'],
        ['a3']
      ]));
  });
  test('can match by literal column names', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'column A', 'column-A'],
      ['a1', 'a2', 'a3']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA' },
        { name: 'column A' },
        { name: 'column-A' }
      ], { useLiteralNames: true }))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'column A', 'column-A'],
        ['a1', 'a2', 'a3']
      ]));
  });
  test('removes columns that have the same literal name (last column prevails)', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnA', 'columnA'],
      ['a1', 'a2', 'a3']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA' }
      ], { useLiteralNames: true }))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA'],
        ['a3']
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
  test('passes through empty rows when passthroughEmptyRows is true', { concurrency: true }, async () => {
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
      ], { passthroughEmptyRows: true }))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['', ''],
        ['a', 'b'],
        ['', ''],
        ['', ''],
        ['', '']
      ]));
  });
  test('can remove empty columns', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', '', 'columnB', '', ''],
      ['a', '', 'b', '', '']
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
  test('handles sparse CSV', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['sparse1', 'sparse2', 'sparse3'],
      ['s1', '', ''],
      ['', 's2', ''],
      ['', '', 's3']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'sparse1', type: 'string' },
        { name: 'sparse2', type: 'string' },
        { name: 'sparse3', type: 'string' },
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['sparse1', 'sparse2', 'sparse3'],
        ['s1', '', ''],
        ['', 's2', ''],
        ['', '', 's3']
      ]));
  });
  test('handles uneven CSV', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['common', 'maybe1', 'maybe2', 'maybe3'],
      ['c'],
      ['c', 'm'],
      ['c', 'm', 'm'],
      ['c', 'm', 'm', 'm']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'common', type: 'string' },
        { name: 'maybe1', type: 'string' },
        { name: 'maybe2', type: 'string' },
        { name: 'maybe3', type: 'string' },
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['common', 'maybe1', 'maybe2', 'maybe3'],
        ['c', '', '', ''],
        ['c', 'm', '', ''],
        ['c', 'm', 'm', ''],
        ['c', 'm', 'm', 'm'],
      ]));
  });
  test('defaults to "string" type', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['1E9', '44309']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA' },
        { name: 'columnB' }
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['1E9', '44309']
      ]));
  });
  test('can pass through numbers', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['123', '456']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA', type: 'number' },
        { name: 'columnB', type: 'number' }
      ], { typeCastOnly: true }))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        [123, 456]
      ]));
  });
  test('can pass through BigInt', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['123', '456']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA', type: 'bigint' },
        { name: 'columnB', type: 'bigint' }
      ], { typeCastOnly: true }))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['123', '456']
      ]));
  });
  test('can pass through date', { concurrency: true }, async () => {
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['2025-02-10', '2025-02-09']
    ])
      .pipeThrough(new CSVNormalizer([
        { name: 'columnA', type: 'date' },
        { name: 'columnB', type: 'date' }
      ], { typeCastOnly: true }))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['2025-02-10T00:00:00.000Z', '2025-02-09T00:00:00.000Z']
      ]));
  });
  test('can ignore incorrect data types', { concurrency: true }, async (t) => {
    assertConsole(t, { warn: 2 });
    await createCSVMockStream([
      ['columnA', 'columnB'],
      ['a', 'b']
    ])
      .pipeThrough(new CSVNormalizer([
        // @ts-expect-error
        { name: 'columnA', type: 'abc' },
        // @ts-expect-error
        { name: 'columnB', type: 'def' }
      ]))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA', 'columnB'],
        ['a', 'b']
      ]));
  });
});

suite('CSVDenormalizer', { concurrency: true }, () => {
  test('can process normalized rows', { concurrency: true }, async () => {
    await createCSVMockStream([
      [{ displayName: 'Column A', value: 'a1' }, { displayName: 'Column B', value: 'b1' }],
      [{ displayName: 'Column A', value: 'a2' }, { displayName: 'Column B', value: 'b2' }]
    ])
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['Column A', 'Column B'],
        ['a1', 'b1'],
        ['a2', 'b2']
      ]));
  });
  test('falls back on name if displayName is undefined', { concurrency: true }, async () => {
    await createCSVMockStream([
      [{ name: 'Column A', value: 'a1' }, { displayName: 'Column B', value: 'b1' }],
      [{ displayName: 'Column A', value: 'a2' }, { name: 'Column B', value: 'b2' }]
    ])
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['Column A', 'Column B'],
        ['a1', 'b1'],
        ['a2', 'b2']
      ]));
  });
});

suite('CSVNormalizer and CSVDenormalizer end-to-end', { concurrency: true }, () => {
  test('can normalize', { concurrency: true }, async () => {
    /** @type {Array<CSVNormalizerHeader>} */
    const headers = [
      { name: 'stringCol', displayName: 'String Column', type: 'string', defaultValue: 'N/A' },
      { name: 'numberCol', displayName: 'Number Column', type: 'number' },
      { name: 'bigintCol', displayName: 'BigInt Column', type: 'bigint' },
      { name: 'dateCol', displayName: 'Date Column', type: 'date' }
    ]
    await new CSVReader('./test/data/normalization.csv')
      .pipeThrough(new CSVNormalizer(headers))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['String Column', 'Number Column', 'BigInt Column', 'Date Column'],
        ['abc 👨‍👩‍👧‍👦', 123456789.123456, '1000000000000000000', '2024-01-01T00:00:00.000Z'],
        [', " 🏴‍☠️', 1000, '-1234567890000000000000000', '2024-06-01T00:00:00.000Z'],
        ['N/A', 123100, '1000000000000000000', '2024-12-31T08:00:00.000Z']
      ]));
  });
  test('can normalize with transformation', { concurrency: true }, async () => {
    /** @type {Array<CSVNormalizerHeader>} */
    const headers = [
      { name: 'columnA' },
      { name: 'columnB' }
    ]
    await createCSVMockStream([
      ['columnA', '', 'columnB', '', ''],
      ['a', '', 'b', '', ''],
      ['', '', '', '', ''],
      ['', '', '', '', '']
    ])
      .pipeThrough(new CSVNormalizer(headers))
      .pipeThrough(new CSVTransformer(r => r.map(f => ({ name: `${f.name} updated`, value: `${f.value} updated` }))))
      .pipeThrough(new CSVDenormalizer())
      .pipeTo(csvStreamEqualWritable([
        ['columnA updated', 'columnB updated'],
        ['a updated', 'b updated']
      ]));
  });
});