import { existsSync } from 'node:fs';
import { copyFile } from 'node:fs/promises';
import { execSync } from 'node:child_process';

// If called with `npm run pack:dev`, then this code will be executed TWICE:
//   node prepack.js -- --dev
//   node prepack.js
//   npm pack
//   node postpack.js
// If called with `npm pack`, then this code will be executed only ONCE:
//   node prepack.js
//   npm pack
//   node postpack.js
// This is the simplest cross-platform solution without (1) using environment variables (non-trivial
// to make work across platforms) or (2) user-defined npm config variables (deprecated in npm v11 and
// will be removed in npm v12).

// First pass if `npm run pack:dev`
if (process.argv.slice(2).includes('--dev')) {
  await copyFile('./package.json', './package.backup.json');
  execSync(`npm pkg set version=0.0.0-DEV-${Date.now()}`);
  execSync('npm pkg set private=true');
}
// Second pass if `npm run pack:dev` OR first pass if `npm pack`
else {
  if (!existsSync('./package.backup.json')) {
    await copyFile('./package.json', './package.backup.json');
  }
  execSync('npm run types');
  execSync('npm pkg delete devEngines');
  execSync('npm pkg delete scripts');
  execSync('npm pkg delete devDependencies');
}
