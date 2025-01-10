import { suite, after, test } from 'node:test';
import { ok } from 'node:assert/strict';
import { mkdir, cp, opendir, readFile, rm } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

import { createTempFolder } from './utils.js';

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
  const { name } = /(?<name>ex\d+).js/.exec(file.name)?.groups ?? { name: null }; // Assuming naming convention "ex*.js"
  if (name === null) { continue; }
  tests.push({
    name,
    code: new URL(`${name}.js`, tempExamples),
    output: new URL(`./data/${name}-out.csv`, tempExamples)
  });
}

suite('examples', { concurrency: true }, () => {
  after(async () => await rm(dir, { recursive: true, force: true }));
  for (const { name, code, output } of tests) {
    test(name, { concurrency: true }, async () => {
      const expected = await readFile(output);
      await import(code);
      const actual = await readFile(output);
      ok(actual.equals(expected));
    });
  }
});
