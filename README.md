# Streamed CSV

Streamed CSV is a Node.js library to read, transform, and write CSV files using the [Web Streams API](https://developer.mozilla.org/en-US/docs/Web/API/Streams_API).

## Example

Assume `./data/example-in.csv` looks like:

```plaintext
columnA,columnB
1,1
100,100
223423,455947
348553,692708
536368,676147
```

Run:

```javascript
import {
  createCSVReadableStream,
  createCSVTransformStream,
  createCSVWritableStream
} from './src/index.js';

function timesTwo(row) {
  return [Number(row[0]) * 2, Number(row[1]) * 2];
}

createCSVReadableStream('./data/example-in.csv')
  .pipeThrough(createCSVTransformStream(timesTwo))
  .pipeTo(createCSVWritableStream('./data/example-out.csv'));
```

A new `./data/example-out.csv` file will be created:

```plaintext
columnA,columnB
2,2
200,200
446846,911894
697106,1385416
1072736,1352294
```

See the [`examples`](./examples) folder for more end-to-end examples.

## Why?

A common workflow is:

1. Read an existing CSV file row by row.
2. Perform some transformation using the data in each row.
3. Write the transformed data to another CSV file.

Most people are interested in coding the transformation logic for step (2), and much less interested in coding the error-prone boilerplate code for steps (1) and (3).

Streamed CSV provides the bare minimum functions to accomplish **this specific workflow**. This library is *not* intended for any other workflow (e.g., parsing CSV files with custom delimiters, converting between CSV and other formats, performing data analytics on CSV data). For those workflows, see ["Other CSV libraries"](#other-csv-libraries).

## Features

- No dependencies other than the Node.js standard library
- Reads and writes [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt) compliant CSV files
- Single `core.js` file contains *all* core API code
- Prioritizes Web Platform APIs (e.g., the [Web Streams API](https://developer.mozilla.org/en-US/docs/Web/API/Streams_API) over [Node.js Streams](https://nodejs.org/api/stream.html))

## Core API

The core API is contained in `src/core.js` and exposes three functions to read, transform, and write CSV files.

### `createCSVReadableStream`

Create a `ReadableStream` from a local CSV file where each CSV row is one chunk.

```javascript
import { createCSVReadableStream } from './src/index.js';

const readableStream = createCSVReadableStream('./data/example-in.csv');
```

### `createCSVTransformStream`

Create a `TransformStream` to apply a given function to each row in a streamed CSV.

```javascript
import { createCSVTransformStream } from './src/index.js';

function timesTwo(row) {
  return [Number(row[0]) * 2, Number(row[1]) * 2];
}

const transformStream = createCSVTransformStream(timesTwo);
```

### `createCSVWritableStream`

Create a `WritableStream` to save CSV row data to disk.

```javascript
import { createCSVWritableStream } from './src/index.js';

const writableStream = createCSVWritableStream('./data/example-out.csv');
```

## Extended API

In addition to the core API, optional utilities are provided to address common CSV transformation tasks.

### `createCSVNormalizationStream` and `createCSVDenormalizationStream`

`TransformStream`s to parse CSVs that have been mangled by spreadsheet programs such as Excel.

## Tests

Example and test CSVs use `CRLF` (`\r\n`) as the record delimiter, as suggested by [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).

If your IDE is configured to automatically convert `CRLF` line endings to `LF`, you must undo this conversion for all the CSV files inside `/test/data` and `/examples/data`.

Run all tests:

```plaintext
npm test
```

## Other CSV libraries

Here are more comprehensive CSV libraries that may be better for your use case.

- [Papa Parse](https://github.com/mholt/PapaParse)
- [node-csv](https://github.com/adaltas/node-csv)
- [fast-csv](https://github.com/C2FO/fast-csv)
- [csv-parser](https://github.com/mafintosh/csv-parser)
