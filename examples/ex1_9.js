// ## Example 1.9: Read and write a `.csv.gz` file
// 
// If your input or output files are not direct CSV files, you can create CSVReader and CSVWriter as
// TransformStream instances by passing nothing to the constructors. This usage is convenient for
// interoperating with other Web APIs, such as the Compression Streams API to read and write `.gz` files.
// 
// Note there is no need to save the CSVs to intermediate temporary working files, streams allow you to
// directly process the `.gz` files.

import { createWriteStream } from 'node:fs';
import { Writable } from 'node:stream';
import { DecompressionStream, TextDecoderStream, CompressionStream } from 'node:stream/web';

import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';
/** @import { TransformationFunction } from '../src/index.js'; */

import { createFileStream } from '../src/core.js';
import { setGzipOSByteToUnknown } from '../test/utils.js';

/** @type {TransformationFunction} */
function timesTwo(row) {
  return [Number(row[0]) * 2, Number(row[1]) * 2];
}

await createFileStream(new URL('./data/ex1_9-in.csv.gz', import.meta.url))
  .pipeThrough(new DecompressionStream('gzip'))
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new CSVReader())
  .pipeThrough(new CSVTransformer(timesTwo))
  .pipeThrough(new CSVWriter()) // No need to use TextEncoderStream on this end, CompressionStream can handle text.
  .pipeThrough(new CompressionStream('gzip'))
  .pipeTo(Writable.toWeb(createWriteStream(new URL('./data/ex1_9-out.csv.gz', import.meta.url))));

// Overriding platform-specific metadata on the `.gz` file so this test is consistent across platforms. This step is
// NOT necessary to actually use the `.gz` file in normal situations.
await setGzipOSByteToUnknown(new URL('./data/ex1_9-out.csv.gz', import.meta.url));
