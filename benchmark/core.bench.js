import { unlinkSync } from 'node:fs';

import { benchmark, benchmarkIterations } from './utils.js';
import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';
import { createRandomCSV, createTempFile } from '../test/utils.js';

const NAME_LENGTH = 15;

const temp = await createTempFile();
process.on('exit', () => unlinkSync(temp));
process.on('SIGINT', () => process.exit());

let iter = 0;
await benchmarkIterations('1MB Write'.padEnd(NAME_LENGTH, '.'), 20, async () => {
  await createRandomCSV(1000, 100, iter++).pipeTo(new CSVWriter(temp));
});

await benchmarkIterations('1MB Read'.padEnd(NAME_LENGTH, '.'), 20, async () => {
  const r = new CSVReader(temp);
  for await (const _ of r) { }
});

await benchmarkIterations('1MB Transform'.padEnd(NAME_LENGTH, '.'), 20, async () => {
  const r = createRandomCSV(1000, 100, iter++).pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1)));
  for await (const _ of r) { }
});

await benchmark('50MB Write'.padEnd(NAME_LENGTH, '.'), async () => {
  await createRandomCSV(50000, 100, iter++).pipeTo(new CSVWriter(temp));
});

await benchmark('50MB Read'.padEnd(NAME_LENGTH, '.'), async () => {
  const r = new CSVReader(temp);
  for await (const _ of r) { }
});

await benchmark('50MB Transform'.padEnd(NAME_LENGTH, '.'), async () => {
  const r = createRandomCSV(50000, 100, iter++).pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1)));
  for await (const _ of r) { }
});

await benchmark('100MB Write'.padEnd(NAME_LENGTH, '.'), async () => {
  await createRandomCSV(100000, 100, iter++).pipeTo(new CSVWriter(temp));
});

await benchmark('100MB Read'.padEnd(NAME_LENGTH, '.'), async () => {
  const r = new CSVReader(temp);
  for await (const _ of r) { }
});

await benchmark('100MB Transform'.padEnd(NAME_LENGTH, '.'), async () => {
  const r = createRandomCSV(100000, 100, iter++).pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1)));
  for await (const _ of r) { }
});
