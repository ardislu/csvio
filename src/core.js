import { createReadStream } from 'node:fs';
import { mkdir, open } from 'node:fs/promises';
import { dirname, basename, normalize } from 'node:path';
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
/** @import { PathLike } from 'node:fs'; */

/**
 * Parse and normalize a `PathLike` object into a string.
 * 
 * @param {PathLike} path A `string`, `Buffer`, or `URL` representing a path to a local file.
 * @returns {string} The normalized file path in string form.
 * @throws {TypeError} If `path` is not a `string`, `Buffer`, or `URL`, or it is a `URL` that is not using the `file` schema.
 */
export function parsePathLike(path) {
  if (typeof path === 'string') {
    return normalize(path);
  }
  else if (ArrayBuffer.isView(path) || path instanceof ArrayBuffer) {
    return normalize(new TextDecoder().decode(path));
  }
  else if (path instanceof URL) {
    return fileURLToPath(path);
  }
  else {
    throw new TypeError(`Expected a string, Buffer, or URL, but received ${typeof path}.`);
  }
}

// https://github.com/DefinitelyTyped/DefinitelyTyped/blob/e9296eba83c97aa1fc5d2689ad0539da9427977f/types/node/buffer.d.ts#L246-L258
/** @typedef {"ascii"|"utf8"|"utf-8"|"utf16le"|"utf-16le"|"ucs2"|"ucs-2"|"base64"|"base64url"|"latin1"|"binary"|"hex"} BufferEncoding */

/**
 * Options to configure `CSVReader`.
 * @typedef {Object} CSVReaderOptions
 * @property {BufferEncoding} [encoding='utf-8'] The character encoding of the input CSV file. Can be any value accepted by
 * Node.js's [`Buffer`](https://nodejs.org/api/buffer.html#buffers-and-character-encodings). The default value is `utf-8`.
 */

/**
 * A simple streaming parser for CSV files.
 * 
 * Uses Node.js's [`fs.createReadStream()`](https://nodejs.org/api/fs.html#fscreatereadstreampath-options) to read a local
 * CSV file then parses the file contents and returns the data through a [Streams API](https://developer.mozilla.org/en-US/docs/Web/API/Streams_API)
 * `ReadableStream`.
 * 
 * Each chunk is one row of the CSV file, in the form of a string that may be deserialized using `JSON.parse()`.
 * @extends ReadableStream
 */
export class CSVReader extends ReadableStream {
  #row = [];
  #field = '';
  #lastChar = null;
  #escapeMode = false;
  #justExitedEscapeMode = false;

  /**
   * @param {PathLike} path A `string`, `Buffer`, or `URL` representing a path to a local CSV file.
   * @param {CSVReaderOptions} options Object containing flags to configure the reader.
   */
  constructor(path, options = {}) {
    const { encoding = 'utf-8' } = options;
    const readStream = createReadStream(path, { encoding });
    const readableStream = Readable.toWeb(readStream).pipeThrough(new TransformStream({
      transform: (chunk, controller) => this.#transform(chunk, controller),
      flush: (controller) => this.#flush(controller)
    }));
    const reader = readableStream.getReader();
    super({
      async pull(controller) {
        const { done, value } = await reader.read();
        if (done) {
          controller.close();
        }
        else {
          controller.enqueue(value);
        }
      }
    });
  }

  #transform(chunk, controller) {
    for (const char of chunk) {
      /* Escape mode logic */
      if (this.#escapeMode) {
        if (char === '"') { // Exit escape mode and do not add char to field
          this.#escapeMode = false;
          this.#justExitedEscapeMode = true;
        }
        else { // Add the literal char because in escape mode
          this.#justExitedEscapeMode = false;
          this.#field += char;
        }
      }
      /* Normal mode logic */
      else {
        if (char === '"') {
          if (this.#justExitedEscapeMode) { // Re-enter escape mode because the exit char was escaped, and add char to field
            this.#escapeMode = true;
            this.#field += char;
          }
          else if (this.#lastChar === ',' || this.#lastChar === '\n' || this.#lastChar === null) { // Enter escape mode and do not add char to field
            this.#escapeMode = true;
          }
          else { // No special cases match, add the literal char
            this.#field += char;
          }
        }
        else if (char === '\r' || char === '\uFEFF') { } // Ignore CR (newline logic is handled by LF) and byte order mark
        else if (char === ',') { // Terminate the field and do not add char to field
          this.#row.push(this.#field);
          this.#field = '';
        }
        else if (char === '\n') { // Terminate the field and row and do not add char to field
          this.#row.push(this.#field);
          this.#field = '';
          controller.enqueue(JSON.stringify(this.#row));
          this.#row.length = 0;
        }
        else { // No special cases match, add the literal char
          this.#field += char;
        }
        this.#justExitedEscapeMode = false;
      }
      this.#lastChar = char;
    }
  }

  #flush(controller) {
    if (this.#field !== '' || this.#row.length !== 0) { // CSV terminated without a trailing CRLF, leaving a row in the queue
      this.#row.push(this.#field); // Last field is always un-pushed if CSV terminated with nothing
      controller.enqueue(JSON.stringify(this.#row));
    }
    else if (this.#lastChar === null) { // CSV is blank
      controller.enqueue(JSON.stringify(['']));
    }
  }
}

/**
 * A row or rows of input CSV data.
 * @typedef {Array<Array<string>>|Array<string>|string} TransformationInput
 */

/**
 * A row or rows of output CSV data, or `null` to skip a row.
 * @typedef {Array<Array<any>>|Array<any>|string|null} TransformationOutput
 */

/**
 * A function to process a row or rows of CSV data from `CSVReader`.
 * @callback TransformationFunction
 * @param {TransformationInput} row A row or rows of input CSV data before transformation.
 * @returns {TransformationOutput|Promise<TransformationOutput>} A row or rows of output CSV data after transformation, or `null` to skip a row.
 */

/**
 * A function to handle errors thrown by a transformation function.
 * @callback TransformationErrorFunction
 * @param {TransformationInput} row The row passed to the transformation function which threw the error.
 * @param {Error} error The error thrown by the transformation function.
 * @param {TransformationFunction} fn The transformation function itself. This argument can be used to retry a transformation.
 * @returns {TransformationOutput|Promise<TransformationOutput>} A row or rows of output CSV data, or `null` to skip this row.
 */

/**
 * Options to configure `CSVTransformer`.
 * @typedef {Object} CSVTransformerOptions
 * @property {boolean|Array<any>|TransformationFunction} [handleHeaders=false] A `boolean`, `Array<any>`, or `TransformationFunction` to process
 * the header row (first row) of the CSV. If `false`, the header row will pass through the stream. If `true`, the header row will be passed to
 * `fn()`. If `Array<any>`, the header row will be replaced with the `Array<any>`. If `TransformationFunction`, the header row will be passed
 * to the `TransformationFunction`. The default value is `false`.
 * 
 * NOTE: If the CSV has no header row, set `handleHeaders` to `true` and process the first row normally in `fn()`.
 * @property {boolean} [rawInput=false] Set to `true` to send the raw CSV row to `fn()` as a `string`. Otherwise, the CSV row will be deserialized
 * using `JSON.parse()` before being sent to `fn()`. The default value is `false`.
 * @property {boolean} [rawOutput=false] Set to `true` to send the raw return value of `fn()` to the next stream. Otherwise, the return value of
 * `fn()` will be serialized using `JSON.stringify()` before being sent to the next stream. The default value is `false`.
 * @property {null|TransformationErrorFunction} [onError=null] Set to a function to catch errors thrown by the transformation function. The
 * input data, the error that was thrown, and the transformation function itself will be passed to the `onError` function. The default value
 * is `null` (errors will not be caught).
 * @property {number} [maxBatchSize=1] The maximum number of rows that will be passed to the transformation function per function call (greedy).
 * The default value is `1`.
 * @property {number} [maxConcurrent=1] The maximum concurrent executions of the transformation function (greedy). The transformation function
 * will be automatically turned into a promise if it isn't already async. Execution is blocked until all promises in a concurrent group settle
 * (i.e., if one transformation in a group is hanging, further rows will NOT be processed even if all other transformations in the group are
 * resolved). The default value is `1`.
 */

/**
 * A `TransformStream` to process CSV data.
 * @extends TransformStream
 */
export class CSVTransformer extends TransformStream {
  #fn;
  #options;
  #concurrent = [];
  #batch = [];
  #firstChunk = true;

  /**
   * @param {TransformationFunction} fn A function to process a row or rows of CSV data.
   * 
   * If `options.rawInput` is `false` (default), the input will be a `Array<string>` representing a CSV row. If `options.rawInput`
   * is `true`, the input will be a JSON `string` representing a CSV row.
   * 
   * If `options.rawOutput` is `false` (default), the expected output is a `Array<any>` which will be serialized with `JSON.stringify()`
   * and enqueued, or a `Array<Array<any>>` which will have each of its sub-arrays serialized and enqueued separately. If
   * `options.rawOutput` is `true`, the raw output will be sent to the next stream unmodified.
   * 
   * If `options.maxBatchSize` is a number greater than `1`, the input will represent multiple CSV rows as an `Array<Array<string>>`
   * (if `options.rawInput` is `false`) or a `string` (if `options.rawInput` is `true`). The input will still be a 2-D array even
   * if there is only 1 row in the batch (except for the header row). The header row is always processed by itself and passed as a
   * `Array<string>`, regardless of the `options.maxBatchSize` value.
   * 
   * If `options.maxConcurrent` is a number greater than `1`, multiple instances of the transformation function will be executed
   * concurrently in groups up to the size of `options.maxConcurrent`.
   * 
   * Return `null` to consume an input row without emitting an output row.
   * @param {CSVTransformerOptions} options Object containing flags to configure the stream logic.
   */
  constructor(fn, options = {}) {
    super({
      transform: (chunk, controller) => this.#transform(chunk, controller),
      flush: (controller) => this.#flush(controller),
    });

    options.handleHeaders ??= false;
    options.rawInput ??= false;
    options.rawOutput ??= false;
    options.onError ??= null;
    options.maxBatchSize ??= 1;
    options.maxConcurrent ??= 1;

    this.#fn = fn;
    this.#options = options;
  }

  async #wrappedHeaderFn(row) {
    const { onError, handleHeaders } = this.#options;
    if (typeof handleHeaders === 'boolean') {
      return handleHeaders ? await this.#wrappedFn(row) : row;
    }
    if (Array.isArray(handleHeaders)) {
      return handleHeaders;
    }
    if (onError === null) { return await handleHeaders(row); }
    try { return await handleHeaders(row); }
    catch (e) { return await onError(row, e, handleHeaders); }
  }

  async #wrappedFn(row) {
    const { onError } = this.#options;
    if (onError === null) { return await this.#fn(row); }
    try { return await this.#fn(row); }
    catch (e) { return await onError(row, e, this.#fn); }
  }

  #enqueueRow(row, controller) {
    if (row === null || row === undefined) { // Input row is consumed without emitting any output row
      return;
    }
    else if (this.#options.rawOutput || typeof row === 'string') {
      controller.enqueue(row);
    }
    else if (Array.isArray(row) && Array.isArray(row[0])) { // Multiple rows returned, enqueue each row separately
      row.forEach(r => controller.enqueue(JSON.stringify(r)));
    }
    else {
      controller.enqueue(JSON.stringify(row));
    }
  }

  async #enqueueConcurrent(controller) {
    const results = await Promise.allSettled(this.#concurrent);
    this.#concurrent.length = 0;
    for (const r of results) {
      const { status, value, reason } = r;
      if (status === 'rejected') {
        if (reason instanceof Error) {
          throw reason;
        }
        else {
          throw new Error(reason);
        }
      }
      else {
        this.#enqueueRow(value, controller);
      }
    }
  }

  async #transform(chunk, controller) {
    const { rawInput, maxBatchSize, maxConcurrent } = this.#options;
    const row = rawInput ? chunk : JSON.parse(chunk);

    if (this.#firstChunk) {
      this.#firstChunk = false;
      const out = await this.#wrappedHeaderFn(row);
      this.#enqueueRow(out, controller);
      return;
    }

    if (maxBatchSize > 1) {
      this.#batch.push(row);
      if (this.#batch.length === maxBatchSize) {
        this.#concurrent.push(this.#wrappedFn(this.#batch));
        this.#batch.length = 0;
      }
    }
    else {
      this.#concurrent.push(this.#wrappedFn(row));
    }

    if (this.#concurrent.length === maxConcurrent) {
      await this.#enqueueConcurrent(controller);
    }
  }

  async #flush(controller) {
    if (this.#batch.length > 0) {
      this.#concurrent.push(this.#wrappedFn(this.#batch));
    }
    if (this.#concurrent.length > 0) {
      await this.#enqueueConcurrent(controller);
    }
  }
}

/**
 * Write a streamed CSV file to disk.
 * 
 * A simple wrapper around Node.js's [`fs.writeFile`](https://nodejs.org/api/fs.html#fspromiseswritefilefile-data-options) to write
 * streamed data to a file. If the input data is JSON, the data is converted to a string representing a RFC 4180 CSV record. If the
 * data is not JSON (e.g., if it is already converted to a CSV string), the data is written to the CSV file directly.
 * @extends WritableStream
 */
export class CSVWriter extends WritableStream {
  #status;

  /**
   * @param {PathLike} path A `string`, `Buffer`, or `URL` representing a path to a local CSV file destination. If the file already
   * exists, its **data will be overwritten**.
   */
  constructor(path) {
    const fullPath = parsePathLike(path);
    const dir = dirname(fullPath);
    const status = {
      name: basename(fullPath),
      start: performance.now(),
      elapsed: 0,
      rows: 0,
      done: false
    };
    let handle;
    super({
      async start() {
        await mkdir(dir, { recursive: true });
        handle = await open(fullPath, 'w');
      },
      async write(chunk) {
        let data;
        try {
          const parsed = JSON.parse(chunk);
          data = CSVWriter.arrayToCSVString(parsed);
          status.rows += parsed.length;
        }
        catch {
          data = chunk;
          status.rows++;
        }
        await handle.write(data);
      },
      async close() {
        await handle.close();
        status.elapsed = performance.now() - status.start;
        status.done = true;
      },
      async abort() {
        await this.close();
      }
    });

    this.#status = status;
  }

  /**
   * @typedef CSVWriterStatus The current status of the CSVWriter.
   * @property {string} name The name of the output CSV file that this writer is writing to.
   * @property {number} elapsed The number of milliseconds that have elapsed between this writer's creation and the output CSV file
   * handle closing, or until now if the writer is still writing. Measured using `performance.now()`.
   * @property {number} rows The estimated number of CSV rows that have been written. If the data passed to the writer is
   * a string, the string is counted as one CSV row (i.e., raw string data is NOT parsed again). Includes the header row (first row).
   * @property {boolean} done If `true`, the writer is finished writing to the output CSV file and the file handle is closed.
   */

  /** @type {CSVWriterStatus} */
  get status() {
    if (!this.#status.done) {
      this.#status.elapsed = performance.now() - this.#status.start;
    }
    return {
      name: this.#status.name,
      elapsed: this.#status.elapsed,
      rows: this.#status.rows,
      done: this.#status.done
    }
  }

  /**
  * Convert a JavaScript array into a CSV string.
  * 
  * @param {Array<string>} arr An array of strings representing one row of a CSV file.
  * @returns {string} A string representing one row of a CSV file, with character escaping and line endings compliant with
  * [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).
  */
  static arrayToCSVString(arr) {
    let out = '';
    for (const field of arr) {
      if (/[,"\r\n]/g.test(field)) {
        out += `"${field.replaceAll('"', '""')}",`;
      }
      else {
        out += `${field},`;
      }
    }
    return `${out.slice(0, -1)}\r\n`;
  }
}
