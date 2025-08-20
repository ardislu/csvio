import { deepStrictEqual, ok } from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { mkdtemp, open } from 'node:fs/promises';
import { normalize } from 'node:path';
import { ReadableStream } from 'node:stream/web';
/** @import { TestContext } from 'node:test'; */
/** @import { PathLike } from 'node:fs'; */
/** @import { FileHandle } from 'node:fs/promises'; */

import { utils } from '../src/errorStrategies.js';

/**
 * A `WritableStream` sink that compares each field in a CSV stream with a given array, assuming each chunk of the
 * stream is a serialized `Array<any>` representing a row of a CSV.
 * 
 * @param {Array<Array<any>>} csv A 2-D array representing the CSV file, where each inner array
 * is a row of the CSV.
 * @returns {WritableStream} A `WritableStream` sink of a CSV stream that will be compared against the given array.
 */
export function csvStreamEqualWritable(csv) {
  let i = 0;
  return new WritableStream({
    write(chunk) {
      deepStrictEqual(chunk.length, csv[i].length);
      for (let j = 0; j < csv[i].length; j++) {
        deepStrictEqual(chunk[j], csv[i][j]);
      }
      i++;
    },
    close() {
      deepStrictEqual(i, csv.length);
    }
  });
}

/**
 * @typedef AssertConsoleCounts Object containing the total number of times `console.log`, `console.info`, `console.warn`,
 * and `console.error` are expected to be called.
 * @property {number} [log=0] The number of times `console.log` is expected to be called.
 * @property {number} [info=0] The number of times `console.info` is expected to be called.
 * @property {number} [warn=0] The number of times `console.warn` is expected to be called.
 * @property {number} [error=0] The number of times `console.error` is expected to be called.
 */

/**
 * Silences console functions during a test and asserts a given number of calls were made during the test. 
 * 
 * **Important:** Console will only be silenced AFTER this function is called, and only until the test ends. Call this function
 * early in the test to avoid unexpected console calls.
 * 
 * @param {TestContext} context A Node.js `TestContext` for a test case.
 * @param {AssertConsoleCounts} [expected] Object containing counts for the total number of times console functions are expected
 * to be called.
 */
export function assertConsole(context, expected = {}) {
  expected.log ??= 0;
  expected.info ??= 0;
  expected.warn ??= 0;
  expected.error ??= 0;
  const actual = {
    log: 0,
    info: 0,
    warn: 0,
    error: 0
  }

  context.mock.method(console, 'log', () => actual.log++);
  context.mock.method(console, 'info', () => actual.info++);
  context.mock.method(console, 'warn', () => actual.warn++);
  context.mock.method(console, 'error', () => actual.error++);

  context.after(() => {
    deepStrictEqual(actual.log, expected.log, `expected ${expected.log} console.log calls, got ${actual.log}`);
    deepStrictEqual(actual.info, expected.info, `expected ${expected.info} console.info calls, got ${actual.info}`);
    deepStrictEqual(actual.warn, expected.warn, `expected ${expected.warn} console.warn calls, got ${actual.warn}`);
    deepStrictEqual(actual.error, expected.error, `expected ${expected.error} console.error calls, got ${actual.error}`);
  });
}

/**
 * @typedef AssertSleepDurations Object containing the minimum and maximum bounds of the total timeout duration.
 * @property {number} min The minimum total timeout duration that is expected.
 * @property {number} max The maximum total timeout duration that is expected.
 */

/**
 * Progresses asynchronous (`node:timers/promises`) `setTimeout` calls instantly and asserts the total duration passed
 * to all `setTimeout` calls are within given expected bounds.
 * 
 * **Important**: The built-in `setTimeout` can **NOT** be mocked directly. As a workaround, use the `utils.sleep`
 * wrapper function from `errorStrategies.js`.
 * 
 * BAD:
 * ```javascript
 * await setTimeout(1000);
 * ```
 * 
 * GOOD:
 * ```javascript
 * await utils.sleep(1000);
 * ```
 * 
 * @param {TestContext} context A Node.js `TestContext` for a test case.
 * @param {AssertSleepDurations} expected Object containing the minimum and maximum bounds of the total sleep
 * duration.
 */
export function assertSleep(context, expected) {
  let actual = 0;
  context.mock.method(utils, 'sleep', duration => actual += duration);
  context.after(() => {
    ok(actual >= expected.min, `expected minimum ${expected.min} total sleep duration, got ${actual}`);
    ok(actual <= expected.max, `expected maximum ${expected.max} total sleep duration, got ${actual}`);
  });
}

/**
 * Simulate a stream of CSV data for test mocking purposes.
 * 
 * @param {Array<Array<string>>|Array<Array<Object>>} data A 2-D array representing the CSV file to mock, where
 * each inner array is a row of the CSV.
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
        controller.enqueue(value.slice());
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
  const handle = await open(path, 'a+');
  await handle.close();
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
 * @param {number} seed A number to initiate the pseudorandom hash function.
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
 * @param {number} rows The number of rows the CSV stream will return.
 * @param {number} columns The number of columns in each CSV row.
 * @param {number} seed The seed used to deterministically generate the pseudorandom values in the CSV stream.
 * @returns {ReadableStream} A `ReadableStream` where each row is emitted as a separate chunk.
 */
export function createRandomCSV(rows, columns, seed) {
  const chunks = (function* () {
    while (rows--) {
      const data = new Uint32Array(columns);
      getPseudoRandomValues(data, seed);
      seed = data[columns - 1];
      yield Array.from(data);
    }
  })();
  return ReadableStream.from(chunks);
}

/**
 * Creates a writable file stream for a local file.
 * 
 * @see {@link https://streams.spec.whatwg.org/#example-ws-backpressure}
 * @param {PathLike} path A `string`, `Buffer`, or `URL` representing a path to a local file.
 * @returns {WritableStream<Uint8Array<ArrayBuffer>>}
 */
export function createWritableFileStream(path) {
  /** @type {FileHandle} */
  let handle;

  return new WritableStream({
    async start() {
      handle = await open(path, 'w');
    },
    async write(chunk) {
      await handle.write(chunk, 0, chunk.length);
    },
    async close() {
      await handle.close();
    },
    async abort() {
      await handle.close();
    }
  });
}

/**
 * Sets the "OS" (filesystem) header byte of a given gzip file to `255` ("Unknown"). This function
 * is useful to strip a gzip file of platform-specific metadata.
 * 
 * @param {PathLike} path A `string`, `Buffer`, or `URL` representing a path to a local file.
 */
export async function setGzipOSByteToUnknown(path) {
  // https://en.wikipedia.org/wiki/Gzip#File_structure
  const offset = 9;
  const byte = new Uint8Array([255]);
  const file = await open(path, 'r+');
  await file.write(byte, 0, 1, offset);
  await file.close();
}
