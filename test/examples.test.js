import { suite, after, test } from 'node:test';
import { ok } from 'node:assert/strict';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, cp, opendir, readFile, rm } from 'node:fs/promises';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { finished } from 'node:stream/promises';

import { createTempFolder, assertConsole } from './utils.js';

const dir = await createTempFolder();
const tempExamples = pathToFileURL(`${dir}/examples/`); // Trailing forward slash is REQUIRED for using this URL as a URL base later
const tempSrc = pathToFileURL(`${dir}/src/`);
const actualExamples = new URL('../examples/', import.meta.url);
const actualSrc = new URL('../src/', import.meta.url);
await Promise.all([
  mkdir(tempExamples, { recursive: true }),
  mkdir(tempSrc, { recursive: true })
]);
await Promise.all([
  cp(actualExamples, tempExamples, { recursive: true }),
  cp(actualSrc, tempSrc, { recursive: true })
]);

const tests = [];
const files = await opendir(tempExamples, { recursive: true });
for await (const file of files) {
  // https://regexr.com/8bo3u
  const { name } = /^(?<name>ex\d+_\d+).js/.exec(file.name)?.groups ?? { name: null }; // Assuming naming convention "ex*_*.js"
  if (name === null) { continue; }
  tests.push({
    name,
    code: new URL(`${name}.js`, tempExamples),
    input: new URL(`./data/${name}-in.csv`, tempExamples),
    output: new URL(`./data/${name}-out.csv`, tempExamples)
  });
}

suite('examples', { concurrency: true }, () => {
  after(async () => await rm(dir, { recursive: true, force: true }));
  for (const { name, code, input, output } of tests) {
    if (name === 'ex1_6') {
      test(name, { concurrency: true }, async (t) => {
        assertConsole(t, { log: 1 });
        const expected = await readFile(output);
        await import(code);
        const actual = await readFile(output);
        ok(actual.equals(expected));
      });
    }
    else if (name === 'ex1_8') {
      test(name, { concurrency: true }, async () => {
        const expected = await readFile(output);
        const child = spawn(process.execPath, [fileURLToPath(code)]);
        createReadStream(input).pipe(child.stdin); // Equivalent to `cat ${input} | child`
        await finished(child.stdout.pipe(createWriteStream(output))); // Equivalent to `child > ${output}`
        const actual = await readFile(output);
        ok(actual.equals(expected));
      });
    }
    else {
      test(name, { concurrency: true }, async () => {
        const expected = await readFile(output);
        await import(code);
        const actual = await readFile(output);
        ok(actual.equals(expected));
      });
    }
  }
});
