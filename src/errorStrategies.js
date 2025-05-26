import { setTimeout } from 'node:timers/promises';

/** @import {TransformationErrorFunction} from './core.js'; */

/**
 * A wrapper around `node:timers/promises`'s async `setTimeout`. This wrapper is required to mock for testing purposes.
 * 
 * @param {number} ms The duration to wait before resolving.
 */
/* node:coverage ignore next 3 */
async function sleep(ms) {
  await setTimeout(ms);
}
export const utils = { sleep };

/**
 * Create a `TransformationErrorFunction` that handles errors by silently swallowing the error and skipping the output
 * row.
 * 
 * @returns {TransformationErrorFunction} A `TransformationErrorFunction` that may be passed to a `CSVTransformer`'s
 * `onError` option.
 */
function skip() {
  return () => null;
}

/**
 * Create a `TransformationErrorFunction` that handles errors by filling the output row with a given placeholder value.
 * 
 * @param {string} value The placeholder value to fill the row with. This value will be used for each field in the row.
 * @returns {TransformationErrorFunction} A `TransformationErrorFunction` that may be passed to a `CSVTransformer`'s
 * `onError` option.
 */
function placeholder(value) {
  return (row) => {
    if (row.length === 0) {
      return [value];
    }
    else {
      return Array(row.length).fill(value);
    }
  }
}

/**
 * @typedef {Object} RetryOptions
 * @property {number} [interval] The duration (in milliseconds) to wait in between each retry. The default value is `0`
 * (retry immediately).
 * @property {string} [placeholder] A placeholder value to set if the function still errors after the final iteration.
 * If unset, the final error thrown by the function is re-thrown.
 */

/**
 * Create a `TransformationErrorFunction` that handles errors by retrying it `iterations` number of times.
 * 
 * @param {number} iterations The number of times to retry the function.
 * @param {RetryOptions} [options] Extra configuration options for the retry.
 * @returns {TransformationErrorFunction} A `TransformationErrorFunction` that may be passed to a `CSVTransformer`'s
 * `onError` option.
 * @throws If `value` is unset and the function throws an error after the final iteration, the error is re-thrown.
 */
function retry(iterations, options = {}) {
  const interval = options.interval ?? 0;
  return async (row, e, fn) => {
    while (iterations--) {
      await utils.sleep(interval);
      try { return await fn(row); }
      catch { }
    }
    if (options.placeholder !== undefined) {
      return placeholder(options.placeholder)(row, e, fn);
    }
    throw e;
  }
}

/**
 * @typedef {Object} BackoffOptions
 * @property {number} [maxExponent] If provided, the maximum exponent used to calculate the retry duration. In other words,
 * the `iterations` number at which the retry duration stops increasing. If `maxExponent` is unset, the retry duration
 * is unbounded and will increase after each retry.
 * @property {string} [placeholder] A placeholder value to set if the function still errors after the final iteration.
 * If unset, the final error thrown by the function is re-thrown.
 */

/**
 * Create a `TransformationErrorFunction` that handles errors by retrying it an `iterations` number of times, with an
 * exponentially increasing duration in between each retry (i.e., an exponential backoff).
 * 
 * @param {number} iterations The number of times to retry the function.
 * @param {BackoffOptions} [options] Extra configuration options for the backoff.
 * @returns {TransformationErrorFunction} A `TransformationErrorFunction` that may be passed to a `CSVTransformer`'s
 * `onError` option.
 * @throws If `value` is unset and the function throws an error after the final iteration, the error is re-thrown.
 */
function backoff(iterations, options = {}) {
  const maxExponent = options.maxExponent ?? Infinity;
  return async (row, e, fn) => {
    for (let n = 0; n < iterations; n++) {
      const exponent = n > maxExponent ? maxExponent : n;
      const duration = (2 ** exponent) * 1000; // 1s, 2s, 4s, 8s, 16s, ...
      const jitter = Math.random() * 1000;
      await utils.sleep(duration + jitter);
      try { return await fn(row); }
      catch { }
    }
    if (options.placeholder !== undefined) {
      return placeholder(options.placeholder)(row, e, fn);
    }
    throw e;
  }
}

export const ErrorStrategies = { skip, placeholder, retry, backoff };
Object.freeze(ErrorStrategies);