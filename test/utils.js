import { deepStrictEqual, notStrictEqual } from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { mkdtemp, open } from 'node:fs/promises';
import { normalize } from 'node:path';

/**
 * Log an entire stream along with the field count for each row and the total row count.
 * 
 * @param {ReadableStream} stream A stream of CSV data.
 * @param {string} [id] Optional label to include in the log.
 */
export async function csvStreamLog(stream, id) {
  let out = '============================================================\n';
  out += `\x1b[45m${id ?? ''}\x1b[0m`;
  out += '\n------------------------------------------------------------\n';
  let totalRows = 0;
  for await (const value of stream) {
    const row = JSON.parse(value);
    const rowNoNewlines = row.map(f => f.replaceAll('\n', '\\n').replaceAll('\r', '\\r'));
    out += `${row.length} || ${rowNoNewlines.join(' | ')}\n`;
    totalRows++;
  }
  out += '------------------------------------------------------------\n';
  out += `Total row count: ${totalRows}\n`;
  out += '============================================================';
  console.log(out);
}

/**
 * Compare each field in a CSV stream with a given array, assuming each chunk of the stream is a
 * serialized `Array<any>` representing a row of a CSV. 
 * 
 * @param {ReadableStream} stream A stream of CSV data.
 * @param {Array<Array<any>>} csv A 2-D array representing the CSV file, where each inner array
 * is a row of the CSV.
 */
export async function csvStreamEqual(stream, csv) {
  let i = 0;
  for await (const value of stream) {
    const row = JSON.parse(value);
    for (let j = 0; j < csv[i].length; j++) {
      deepStrictEqual(row[j], csv[i][j]);
    }
    i++;
  }
}

/**
 * A `WritableStream` version of `csvStreamEqual`. Compares each field in a CSV stream with a given
 * array, assuming each chunk of the stream is a serialized `Array<any>` representing a row of a CSV.
 * 
 * @param {Array<Array<any>>} csv A 2-D array representing the CSV file, where each inner array
 * is a row of the CSV.
 * @returns {WritableStream} A `WritableStream` sink of a CSV stream that will be compared against the given array.
 */
export function csvStreamEqualWritable(csv) {
  let i = 0;
  return new WritableStream({
    write(chunk) {
      const row = JSON.parse(chunk);
      for (let j = 0; j < csv[i].length; j++) {
        deepStrictEqual(row[j], csv[i][j]);
      }
      i++;
    }
  });
}

/**
 * Negative version of `csvStreamEqual`. Tests that the given stream is NOT equal to the given CSV.
 * 
 * @param {ReadableStream} stream A stream of CSV data.
 * @param {Array<Array<any>>} csv A 2-D array representing the CSV file, where each inner array
 */
export async function csvStreamNotEqual(stream, csv) {
  let i = 0;
  for await (const value of stream) {
    const row = JSON.parse(value);
    for (let j = 0; j < csv[i].length; j++) {
      notStrictEqual(row[j], csv[i][j]);
    }
    i++;
  }
}

/**
 * Negative version of `csvStreamEqualWritable`. Tests that the piped stream is NOT equal to the given CSV.
 * 
 * @param {Array<Array<string>>} csv A 2-D array representing the CSV file, where each inner array
 * is a row of the CSV.
 * @returns {WritableStream} A `WritableStream` sink of a CSV stream that will be compared against the given array.
 */
export function csvStreamNotEqualWritable(csv) {
  let i = 0;
  return new WritableStream({
    write(chunk) {
      const row = JSON.parse(chunk);
      for (let j = 0; j < csv[i].length; j++) {
        notStrictEqual(row[j], csv[i][j]);
      }
      i++;
    }
  });
}

/**
 * Simulate a stream of CSV data for test mocking purposes.
 * 
 * @param {Array<Array<string>>} data A 2-D array representing the CSV file to mock, where each inner array
 * is a row of the CSV.
 * @returns {ReadableStream} A `ReadableStream` where each row is emitted as a separate chunk.
 */
export function createCSVMockStream(data) {
  const chunks = (function* () {
    for (const chunk of data) {
      yield chunk;
    }
  })();
  return new ReadableStream({
    pull(controller) {
      const { value, done } = chunks.next();
      if (done) {
        controller.close();
      }
      else {
        controller.enqueue(JSON.stringify(value));
      }
    }
  });
}

/**
 * Create a new temporary folder for testing purposes.
 * 
 * @returns {Promise<string>} A `Promise<string>` for the full path to a newly-created temporary folder.
 */
export async function createTempFolder() {
  const path = normalize(`${tmpdir()}/csvio_test_`);
  return mkdtemp(path);
}

/**
 * Create a new empty temporary file for testing purposes.
 * 
 * @returns {Promise<string>} A `Promise<string>` for the full path to a newly-created temporary file.
 */
export async function createTempFile() {
  const path = normalize(`${tmpdir()}/csvio_test_${crypto.randomUUID()}.tmp`);
  await open(path, 'a+');
  return path;
}

/**
 * Simple, small, and fast pseudorandom number generator to deterministically generate large amounts of mock test data.
 * 
 * API is intended to follow [`crypto.getRandomValues()`](https://developer.mozilla.org/en-US/docs/Web/API/Crypto/getRandomValues).
 * 
 * Hash function source:
 * - https://burtleburtle.net/bob/hash/integer.html
 * - https://web.archive.org/web/20090408063205/http://www.cris.com/~Ttwang/tech/inthash.htm
 * @param {ArrayBufferView<ArrayBufferLike>} typedArray An integer-based `TypedArray` with a byte length that is a multiple of 4.
 * All elements in the array will be overwritten with random numbers.
 * @param {Number} seed A number to initiate the pseudorandom hash function.
 * @returns {ArrayBufferView<ArrayBufferLike>} The same array passed as `typedArray` but with its contents replaced with pseudorandom
 * numbers. Note that `typedArray` is modified in-place and no copy is made.
 */
function getPseudoRandomValues(typedArray, seed) {
  function hash(n) {
    n = n ^ (n >> 4);
    n = (n ^ 0xdeadbeef) + (n << 5);
    n = n ^ (n >> 11);
    return n;
  }

  const array = new Uint32Array(typedArray.buffer);
  let h = hash(seed);
  for (let i = 0; i < array.length; i++) {
    h = hash(h);
    array[i] = h;
  }
  return typedArray;
}

/**
 * Create a stream of pseudorandom CSV data for test mocking purposes.
 * 
 * @param {Number} rows The number of rows the CSV stream will return.
 * @param {Number} columns The number of columns in each CSV row.
 * @param {Number} seed The seed used to deterministically generate the pseudorandom values in the CSV stream.
 * @returns {ReadableStream} A `ReadableStream` where each row is emitted as a separate chunk.
 */
export function createRandomCSV(rows, columns, seed) {
  const chunks = (function* () {
    while (rows) {
      const data = new Uint32Array(columns);
      getPseudoRandomValues(data, seed);
      seed = data[columns - 1];
      rows--;
      yield Array.from(data);
    }
  })();
  return new ReadableStream({
    pull(controller) {
      const { value, done } = chunks.next();
      if (done) {
        controller.close();
      }
      else {
        controller.enqueue(JSON.stringify(value));
      }
    }
  });
}