import { unlinkSync } from 'node:fs';

import { benchmark, benchmarkIterations } from './utils.js';
import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';
import { createRandomCSV, createTempFile } from '../test/utils.js';

const NAME_LENGTH = 15;

const temp = await createTempFile();
process.on('exit', () => unlinkSync(temp));

let iter = 0;
await benchmarkIterations('Write 1MB'.padEnd(NAME_LENGTH, '.'), 20, async () => {
  await createRandomCSV(1000, 100, iter++).pipeTo(new CSVWriter(temp));
});

await benchmarkIterations('Read 1MB'.padEnd(NAME_LENGTH, '.'), 20, async () => {
  const r = new CSVReader(temp);
  for await (const _ of r) { }
});

iter = 0
await benchmarkIterations('Transform 1MB'.padEnd(NAME_LENGTH, '.'), 20, async () => {
  const r = createRandomCSV(1000, 100, iter++).pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1)));
  for await (const _ of r) { }
});

await benchmark('Write 100MB'.padEnd(NAME_LENGTH, '.'), async () => {
  await createRandomCSV(100000, 100, 2).pipeTo(new CSVWriter(temp));
});

await benchmark('Read 100MB'.padEnd(NAME_LENGTH, '.'), async () => {
  const r = new CSVReader(temp);
  for await (const _ of r) { }
});

await benchmark('Transform 100MB'.padEnd(NAME_LENGTH, '.'), async () => {
  const r = createRandomCSV(100000, 100, 2).pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1)));
  for await (const _ of r) { }
});
