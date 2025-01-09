import { createReadStream } from 'node:fs';
import { mkdir, open } from 'node:fs/promises';
import { dirname, normalize } from 'node:path';
import { Readable } from 'node:stream';
/** @import { PathLike } from 'node:fs' */

/**
 * Convert a JavaScript array into a CSV string.
 * 
 * @param {Array<string>} arr An array of strings representing one row of a CSV file.
 * @returns {string} A string representing one row of a CSV file, with character escaping and line endings compliant with
 * [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).
 */
export function arrayToCSVString(arr) {
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

/**
 * A simple streaming parser for CSV files.
 * 
 * Uses Node.js's [`fs.createReadStream()`](https://nodejs.org/api/fs.html#fscreatereadstreampath-options) to read a local
 * CSV file then parses the file contents and returns the data through a [Streams API](https://developer.mozilla.org/en-US/docs/Web/API/Streams_API)
 * `ReadableStream`.
 * @param {PathLike} path A `string`, `Buffer`, or `URL` representing a path to a local CSV file.
 * @returns {ReadableStream<string>} A `ReadableStream` where each chunk is one row of the CSV file, in the form of a string
 * that may be deserialized using `JSON.parse()`.
 */
export function createCSVReadableStream(path) {
  const reader = createReadStream(path, { encoding: 'utf-8' });
  const stream = Readable.toWeb(reader);

  const row = [];
  let field = '';
  let lastChar = null;
  let escapeMode = false;
  let justExitedEscapeMode = false;
  return stream.pipeThrough(new TransformStream({
    transform(chunk, controller) {
      for (const char of chunk) {
        /* Escape mode logic */
        if (escapeMode) {
          if (char === '"') { // Exit escape mode and do not add char to field
            escapeMode = false;
            justExitedEscapeMode = true;
          }
          else { // Add the literal char because in escape mode
            justExitedEscapeMode = false;
            field += char;
          }
        }
        /* Normal mode logic */
        else {
          if (char === '"') {
            if (justExitedEscapeMode) { // Re-enter escape mode because the exit char was escaped, and add char to field
              escapeMode = true;
              field += char;
            }
            else if (lastChar === ',' || lastChar === '\n' || lastChar === null) { // Enter escape mode and do not add char to field
              escapeMode = true;
            }
            else { // No special cases match, add the literal char
              field += char;
            }
          }
          else if (char === '\r' || char === '\uFEFF') { } // Ignore CR (newline logic is handled by LF) and byte order mark
          else if (char === ',') { // Terminate the field and do not add char to field
            row.push(field);
            field = '';
          }
          else if (char === '\n') { // Terminate the field and row and do not add char to field
            row.push(field);
            field = '';
            controller.enqueue(JSON.stringify(row));
            row.length = 0;
          }
          else { // No special cases match, add the literal char
            field += char;
          }
          justExitedEscapeMode = false;
        }
        lastChar = char;
      }
    },
    flush(controller) {
      if (field !== '' || row.length !== 0) { // CSV terminated without a trailing CRLF, leaving a row in the queue
        row.push(field); // Last field is always un-pushed if CSV terminated with nothing
        controller.enqueue(JSON.stringify(row));
      }
      else if (lastChar === null) { // CSV is blank
        controller.enqueue(JSON.stringify(['']));
      }
    }
  }));
}

/**
 * Options to configure `createCSVTransformStream`.
 * @typedef {Object} createCSVTransformStreamOptions
 * @property {boolean} [includeHeaders=false] Set to `true` to pass the header row (assumed to be the first row of the CSV) to `fn()`. Otherwise,
 * the header row will flow through to the next stream without going through `fn()`. The default value is `false`.
 * 
 * NOTE: If the CSV has no header row, set `includeHeaders` to `true` and process the first row normally in `fn()`.
 * @property {boolean} [rawInput=false] Set to `true` to send the raw CSV row to `fn()` as a `string`. Otherwise, the CSV row will be deserialized
 * using `JSON.parse()` before being sent to `fn()`. The default value is `false`.
 * @property {boolean} [rawOutput=false] Set to `true` to send the raw return value of `fn()` to the next stream. Otherwise, the return value of
 * `fn()` will be serialized using `JSON.stringify()` before being sent to the next stream. The default value is `false`.
 */

/**
 * Create a new [TransformStream](https://developer.mozilla.org/en-US/docs/Web/API/TransformStream) to process CSV data.
 * 
 * @param {function(Array<string>|string):Array<any>|string|null} fn A function to process a row of CSV data from `createCSVReadableStream`.
 * 
 * If `options.rawInput` is `false` (default), the input will be a `Array<string>` representing the CSV row. If `options.rawInput` is `true`,
 * the input will be a JSON `string` representing the CSV row.
 * 
 * If `options.rawOutput` is `false` (default), the expected output is a `Array<string>` which will be serialized with `JSON.stringify()`. If
 * `options.rawOutput` is `true`, the raw output will be sent the next stream unmodified.
 * 
 * Return `null` to consume an input row without emitting an output row.
 * @param {createCSVTransformStreamOptions} options Object containing flags to configure the stream logic.
 * @returns {TransformStream} A `TransformStream` where each chunk is one row of the CSV file, after transformations applied by `fn()`.
 */
export function createCSVTransformStream(fn, options = {}) {
  options.includeHeaders ??= false;
  options.rawInput ??= false;
  options.rawOutput ??= false;

  let firstChunk = true;
  return new TransformStream({
    async transform(chunk, controller) {
      const row = options.rawInput ? chunk : JSON.parse(chunk);
      let out;
      if (firstChunk) {
        firstChunk = false;
        out = options.includeHeaders ? await fn(row) : row;
      }
      else {
        out = await fn(row);
      }

      if (out === null || out === undefined) { // Input row is consumed without emitting any output row
        return;
      }

      if (options.rawOutput || typeof out === 'string') {
        controller.enqueue(out);
      }
      else {
        controller.enqueue(JSON.stringify(out));
      }
    }
  });
}

/**
 * Write a streamed CSV file to disk.
 * 
 * A simple wrapper around Node.js's [`fs.writeFile`](https://nodejs.org/api/fs.html#fspromiseswritefilefile-data-options) to write
 * streamed data to a file. If the input data is JSON, the data is converted to a string representing a RFC 4180 CSV record. If the
 * data is not JSON (e.g., if it is already converted to a CSV string), the data is written to the CSV file directly.
 * @param {string} path A `string` representing a path to a local CSV file destination. If the file already exists, its **data
 * will be overwritten**.
 * @returns {WritableStream} A `WritableStream` to write data to a file on disk.
 */
export function createCSVWritableStream(path) {
  const fullPath = normalize(path);
  const dir = dirname(fullPath);
  let handle;

  return new WritableStream({
    async start() {
      await mkdir(dir, { recursive: true });
      handle = await open(fullPath, 'w');
    },
    async write(chunk) {
      let data;
      try {
        data = arrayToCSVString(JSON.parse(chunk));
      }
      catch {
        data = chunk;
      }
      await handle.write(data);
    },
    async close() {
      await handle.close();
    }
  });
}
