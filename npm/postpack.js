import { copyFile, unlink } from 'node:fs/promises';

await copyFile('./package.backup.json', './package.json');
await unlink('./package.backup.json');
