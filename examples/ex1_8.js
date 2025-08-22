// ## Example 1.8: Read from `stdin` and write to `stdout`
// 
// Wrap Node.js's `process.stdin` and `process.stdout` to handle the `stdin` and `stdout` streams.
// This usage is helpful for integrating csvio within complex shell scripts.

import { stdin, stdout } from 'node:process';
import { Readable, Writable } from 'node:stream';
import { TextDecoderStream } from 'node:stream/web';
import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';

// Do not `await`, the pipe just runs indefinitely. The shell script will close this process.
Readable.toWeb(stdin)
  .pipeThrough(new TextDecoderStream()) // CSVReader expects text, bring your own text decoder.
  .pipeThrough(new CSVReader())
  .pipeThrough(new CSVTransformer(r => r.map(v => Number(v) + 1)))
  .pipeThrough(new CSVWriter()) // No need to encode on this end, CSVWriter already outputs text.
  .pipeTo(Writable.toWeb(stdout));

// Basic usage example:
// cat ./examples/data/ex1_8-in.csv | node ./examples/ex1_8.js > ./examples/data/ex1_8-out.csv
