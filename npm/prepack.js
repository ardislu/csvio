import { copyFile } from 'node:fs/promises';
import { execSync } from 'node:child_process';

execSync('npm run types');

await copyFile('./package.json', './package.backup.json');
execSync('npm pkg delete scripts');
execSync('npm pkg delete devDependencies');
