# CSV I/O

CSV I/O (`csvio`) is a Node.js library for processing CSV files.

Code repository mirrors: [GitHub](https://github.com/ardislu/csvio), [Codeberg](https://codeberg.org/ardislu/csvio)

## Install

```
npm i @ardislu/csvio
```

(OPTIONAL) If you are using TypeScript or you are type-checking JavaScript, also install `@types/node`:

```
npm i -D @types/node
```

## Example

Assume `./in.csv` looks like:

Code    | Quantity | Price
------- | -------- | -----
US-1234 | 1        | 12.00
US-5678 | 3        | 8.35
CA-8765 | 2        | 4.11
CA-4321 | 4        | 3.43

Run:

```javascript
import { CSVReader, CSVTransformer, CSVWriter } from '@ardislu/csvio';

function calculate(row) { // Input is a simple array representing one CSV row
  const [country, id] = row[0].split('-');
  const total = (Number(row[1]) * Number(row[2])).toFixed(2);
  return [country, id, total]; // Output is also a simple array
}

// Specify output headers with a simple array
const headers = ['Country', 'ID', 'Total'];

// Use web standard Streams API
await new CSVReader('./in.csv') // Automatically handles path-like input
  .pipeThrough(new CSVTransformer(calculate, { handleHeaders: headers }))
  .pipeTo(new CSVWriter('./out.csv'));
```

A new `./out.csv` file will be created:

Country | ID   | Total
------- | ---- | -----
US      | 1234 | 12.00
US      | 5678 | 25.05
CA      | 8765 | 8.22
CA      | 4321 | 13.72

See the [`examples`](./examples) folder for more end-to-end examples.

## Why?

A common workflow is:

1. Read an existing input CSV file.
2. Perform some processing using the data in the input CSV file.
3. Write data to an output CSV file.

Most people are interested in coding the processing logic for step (2) and less interested in coding the boilerplate for steps (1) and (3).

CSV I/O provides the minimum code to accomplish **this specific workflow**. This library is *not* intended for any other workflow (e.g., parsing CSV files with custom delimiters, converting between CSV and other formats, generating CSV data). For those workflows, see ["Other CSV libraries"](#other-csv-libraries).

## Features

- No dependencies other than the Node.js standard library.
- Reads and writes [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt) compliant CSV files.
- Optimized for common CSV processing requirements (e.g., concurrent processing, batching, error handling).
- Single `core.js` file contains *all* core API code.
- Prioritizes [WinterTC (TC55)](https://wintercg.org/) [Minimum Common Web Platform APIs](https://min-common-api.proposal.wintercg.org/) (e.g., the [Web Streams API](https://streams.spec.whatwg.org/) over [Node.js Streams](https://nodejs.org/api/stream.html)).

## Core API

The core API is contained in `src/core.js` and exposes three classes to read, transform, and write CSV files.

> [!TIP]
> Although these classes *may* be used by themselves (as demonstrated below), they are **intended to be used together**. See [`examples`](./examples) for more practical usage demonstrations.

### `CSVReader`

A `ReadableStream` where each chunk is one row from a local CSV file.

```javascript
import { CSVReader } from '@ardislu/csvio';

const readableStream = new CSVReader('./examples/data/ex1_1-in.csv');

for await (const row of readableStream) {
  console.log(row);
}
// ["columnA","columnB"]
// ["1","1"]
// ["100","100"]
// ["223423","455947"]
// ["348553","692708"]
// ["536368","676147"]
```

### `CSVTransformer`

A `TransformStream` that will apply a given function to each row in a streamed CSV.

```javascript
import { CSVTransformer } from '@ardislu/csvio';

function edit(row) {
  return [`${row[0]}: transformed!`];
}

const transformStream = new CSVTransformer(edit);

const rows = ReadableStream.from((function* () {
  yield '["header"]';
  yield '["1"]';
  yield '["2"]';
  yield '["3"]';
})()).pipeThrough(transformStream);

for await (const row of rows) {
  console.log(row);
}
// ["header"]
// ["1: transformed!"]
// ["2: transformed!"]
// ["3: transformed!"]
```

### `CSVWriter`

A `WritableStream` to save CSV row data to disk.

```javascript
import { CSVWriter } from '@ardislu/csvio';

const writableStream = new CSVWriter('./example.csv');

await ReadableStream.from((function* () {
  yield '["header1","header2"]';
  yield '["1","1"]';
  yield '["2","2"]';
  yield '["3","3"]';
})()).pipeTo(writableStream);
// A new ./example.csv file is created with the CSV data
```

## Extended API

In addition to the core API, optional utilities are provided to address common CSV transformation tasks.

### `CSVNormalizer` and `CSVDenormalizer`

`TransformStream`s to parse CSVs that have been mangled by spreadsheet programs such as Excel.

## Tests

Example and test CSVs use `CRLF` (`\r\n`) as the record delimiter, as suggested by [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).

If your IDE is configured to automatically convert `CRLF` line endings to `LF`, you must undo this conversion for all the CSV files inside `/test/data` and `/examples/data`.

Run primary tests:

```plaintext
npm test
```

Run coverage report for primary tests:

```plaintext
npm run test:coverage
```

Run `large.test.js` tests (writing, reading, and transforming ~100 MB CSV files). These tests take ~30s to complete, so they are separated from the primary tests:

```plaintext
npm run test:large
```

## Other CSV libraries

Here are more comprehensive CSV libraries that may be better for your use case.

- [Papa Parse](https://github.com/mholt/PapaParse)
- [node-csv](https://github.com/adaltas/node-csv)
- [fast-csv](https://github.com/C2FO/fast-csv)
- [csv-parser](https://github.com/mafintosh/csv-parser)
