import { unlinkSync } from 'node:fs';

import { benchmark } from './utils.js';
import { CSVReader, CSVTransformer, CSVWriter } from '../src/index.js';
import { createRandomCSV, createTempFile } from '../test/utils.js';

const temp = await createTempFile();
process.on('exit', () => unlinkSync(temp));

await benchmark('Write 100MB', async () => {
  await createRandomCSV(100000, 100, 2).pipeTo(new CSVWriter(temp));
});

await benchmark('Read 100MB', async () => {
  const r = new CSVReader(temp);
  for await (const _ of r) { }
});

await benchmark('Transform 100MB', async () => {
  const r = createRandomCSV(100000, 100, 2).pipeThrough(new CSVTransformer(r => r.map(f => Number(f) + 1)));
  for await (const _ of r) { }
});
