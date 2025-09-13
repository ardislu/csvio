import { suite, test } from 'node:test';
import { rejects } from 'node:assert/strict';

import { csvStreamEqualWritable, createCSVMockStream, assertSleep } from './utils.js';
import { CSVTransformer } from '../src/core.js';
import { ErrorStrategies } from '../src/errorStrategies.js';

const { skip, placeholder, retry, backoff } = ErrorStrategies;

/**
 * Generate a function that will throw errors until it is called **exactly** `count` times.
 * 
 * @param {number} count The number of times this function must be called before it returns successfully.
 * @returns `count` wrapped in an array for confirmation purposes.
 */
function retryFactory(count) {
  let i = 0;
  return () => {
    if (++i === count) {
      return [i];
    }
    throw new Error();
  }
}

/**
 * Calculate bounds for the expected sleep duration of an exponential backoff.
 * 
 * @param {number} iterations The number of times the exponential backoff will run before truncating.
 * @returns {{min: number, max: number}} The minimum and maximum bounds of the total sleep time.
 */
function getExponentialSleepBounds(iterations) {
  let min = 0;
  for (let n = 0; n < iterations; n++) {
    min += 1000 * (2 ** n);
  }
  const max = min + (1000 * iterations);
  return { min, max };
}

suite('ErrorStrategies.skip', { concurrency: true }, () => {
  test('skips errors', async () => {
    await createCSVMockStream([
      ['header', 'header', 'header'],
      ['1', '1', '1'],
      ['2', '2', '2'],
      ['3', '3', '3']
    ])
      .pipeThrough(new CSVTransformer(() => { throw new Error(); }, { onError: skip() }))
      .pipeTo(csvStreamEqualWritable([
        ['header', 'header', 'header']
      ]));
  });
});

suite('ErrorStrategies.placeholder', { concurrency: true }, () => {
  test('sets placeholders on all fields in a row', async () => {
    await createCSVMockStream([
      ['header', 'header', 'header'],
      ['1', '1', '1']
    ])
      .pipeThrough(new CSVTransformer(() => { throw new Error(); }, { onError: placeholder('n/a') }))
      .pipeTo(csvStreamEqualWritable([
        ['header', 'header', 'header'],
        ['n/a', 'n/a', 'n/a']
      ]));
  });
  test('sets placeholders on all fields in multiple rows', async () => {
    await createCSVMockStream([
      ['header', 'header', 'header'],
      ['1', '1', '1'],
      ['1', '1', '1']
    ])
      .pipeThrough(new CSVTransformer(() => { throw new Error(); }, { onError: placeholder('n/a') }))
      .pipeTo(csvStreamEqualWritable([
        ['header', 'header', 'header'],
        ['n/a', 'n/a', 'n/a'],
        ['n/a', 'n/a', 'n/a']
      ]));
  });
  test('sets placeholders on row with 1 field', async () => {
    await createCSVMockStream([
      ['header', 'header', 'header'],
      ['1']
    ])
      .pipeThrough(new CSVTransformer(() => { throw new Error(); }, { onError: placeholder('n/a') }))
      .pipeTo(csvStreamEqualWritable([
        ['header', 'header', 'header'],
        ['n/a']
      ]));
  });
  test('sets placeholders on row with 0 fields (empty row)', async () => {
    await createCSVMockStream([
      ['header', 'header', 'header'],
      []
    ])
      .pipeThrough(new CSVTransformer(() => { throw new Error(); }, { onError: placeholder('n/a') }))
      .pipeTo(csvStreamEqualWritable([
        ['header', 'header', 'header'],
        ['n/a']
      ]));
  });
});

suite('ErrorStrategies.retry', { concurrency: 1 }, () => { // TODO: concurrency does not work when mocking utils.sleep
  test('retries until successful (1 retry, 2 total calls)', async () => {
    const fn = retryFactory(2); // 1st try + 1 retry = 2 total calls
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: retry(1) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [2]
      ]));
  });
  test('retries until successful (9 retries, 10 total calls)', async () => {
    const fn = retryFactory(10); // 1st try + 9 retries = pass on 10th and final retry
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: retry(9) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [10]
      ]));
  });
  test('re-throws error if failed after last iteration', async () => {
    const fn = retryFactory(10); // 1st try + 8 retries = re-throw after failing on 9th try
    await rejects(createCSVMockStream([
      ['header'],
      ['1']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: retry(8) }))
      .pipeTo(new WritableStream()));
  });
  test('retries until successful (excess retries)', async () => {
    const fn = retryFactory(10); // 1st try + 10 retries = pass on 10th with extra retries available
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: retry(10) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [10]
      ]));
  });
  test('retries with interval (1 retry)', async (t) => {
    assertSleep(t, { min: 1000, max: 1100 });
    const fn = retryFactory(2); // 1st try + 1 retry = pass on 2nd
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: retry(1, { interval: 1000 }) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [2]
      ]));
  });
  test('retries with interval (10 retries)', async (t) => {
    assertSleep(t, { min: 10 * 1000, max: 10 * 1000 + 1000 });
    const fn = retryFactory(11); // 1st try + 10 retries = pass on 11th
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: retry(10, { interval: 1000 }) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [11]
      ]));
  });
  test('sets placeholder if failed after last iteration and placeholder is given', async () => {
    const fn = retryFactory(100);
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: retry(1, { placeholder: 'n/a' }) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        ['n/a']
      ]));
  });
});

suite('ErrorStrategies.backoff', { concurrency: 1 }, () => { // TODO: These tests can't run concurrently, fix
  test('sets 1s backoff for 1 retry', async (t) => {
    const { min, max } = getExponentialSleepBounds(1);
    assertSleep(t, { min, max });
    const fn = retryFactory(2); // 1st try + 1 retry = 2 total calls
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: backoff(1) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [2]
      ]));
  });
  test('sets exponential backoff for 3 retries', async (t) => {
    const { min, max } = getExponentialSleepBounds(3);
    assertSleep(t, { min, max });
    const fn = retryFactory(4); // 1st try + 3 retries
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: backoff(3) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [4]
      ]));
  });
  test('sets exponential backoff for 10 retries', async (t) => {
    const { min, max } = getExponentialSleepBounds(10);
    assertSleep(t, { min, max });
    const fn = retryFactory(11); // 1st try + 10 retries
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: backoff(10) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [11]
      ]));
  });
  test('truncates exponential backoff at 1s', async (t) => {
    const min = 1000 * 10; // 10x retries at truncated 1s 
    const max = min + 1000 * 10; // Max jitter is 1s each retry
    assertSleep(t, { min, max });
    const fn = retryFactory(11); // 1st try + 10 retries
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: backoff(10, { maxExponent: 0 }) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [11]
      ]));
  });
  test('truncates exponential backoff at 16s', async (t) => {
    const min = 1000 * (1 + 2 + 4 + 8 + 16 + 16 + 16); // Exponential until truncating at 16s for 3x retries
    const max = min + 1000 * 7;
    assertSleep(t, { min, max });
    const fn = retryFactory(8); // 1st try + 7 retries
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: backoff(7, { maxExponent: 4 }) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        [8]
      ]));
  });
  test('sets placeholder if failed after last iteration and placeholder is given', async (t) => {
    const { min, max } = getExponentialSleepBounds(3);
    assertSleep(t, { min, max });
    const fn = retryFactory(100);
    await createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: backoff(3, { placeholder: 'n/a' }) }))
      .pipeTo(csvStreamEqualWritable([
        ['header'],
        ['n/a']
      ]));
  });
  test('re-throws error if failed after last iteration', async (t) => {
    const { min, max } = getExponentialSleepBounds(3);
    assertSleep(t, { min, max });
    const fn = retryFactory(100);
    await rejects(createCSVMockStream([
      ['header'],
      ['0']
    ])
      .pipeThrough(new CSVTransformer(fn, { onError: backoff(3) }))
      .pipeTo(new WritableStream()));
  });
});
