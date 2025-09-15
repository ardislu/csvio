/** Manual type overrides */

// TypeScript does not support setting a return type on a class constructor.
// Workaround:
// - Instead of a class, use a const and inherit an interface (the const and the interface can use the same name for convenience)
// - Declare the constructors as overloaded `new` functions
// - Put static methods in the constant definition
// - Put instance methods in the interface definition
// See https://github.com/microsoft/TypeScript/issues/27594
// Workaround source: https://github.com/microsoft/TypeScript/issues/27594#issuecomment-566836640

import { ReadableStream, WritableStream, TransformStream } from 'node:stream/web';

interface CSVReader {}

/**
 * A simple streaming parser for CSV files.
 *
 * Each chunk is one row of the CSV file as an `Array<string>`.
 */
export declare const CSVReader: {
  new(path: PathLike, options?: CSVReaderOptions): CSVReader & ReadableStream<Array<string>>;
  new(path?: null): CSVReader & TransformStream<string, Array<string>>;
}

interface CSVWriter {
  status: CSVWriterStatus;
}

/**
 * Write a streamed CSV file to disk.
 *
 * A simple wrapper around Node.js's [`fs.writeFile`](https://nodejs.org/api/fs.html#fspromiseswritefilefile-data-options) to write
 * streamed data to a file. If the input data is JSON, the data is converted to a string representing a RFC 4180 CSV record. If the
 * data is not JSON (e.g., if it is already converted to a CSV string), the data is written to the CSV file directly.
 */
export declare const CSVWriter: {
  /**
  * Convert a JavaScript array into a CSV string.
  *
  * @param {Array<string>} arr An array of strings representing one row of a CSV file.
  * @returns {string} A string representing one row of a CSV file, with character escaping and line endings compliant with
  * [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).
  */
  arrayToCSVString(arr: Array<string>): string;
  new(path: PathLike): CSVWriter & WritableStream<any>;
  new(path?: null): CSVWriter & TransformStream<Array<string>, string>;
}
