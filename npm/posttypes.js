import { appendFile, readFile } from 'node:fs/promises';

// Inject manually-defined types to work around TypeScript limitations. Note that a re-export
// will not work, the overrides MUST be defined within the top-level index.d.ts to work.
const ambient = await readFile('./npm/ambient.d.ts');
await appendFile('./types/index.d.ts', '\n\n');
await appendFile('./types/index.d.ts', ambient);
