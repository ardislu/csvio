import { setTimeout } from 'node:timers/promises';

/** @import {TransformationErrorFunction} from './core.js'; */

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
 * Create a `TransformationErrorFunction` that handles errors by immediately retrying it `iterations` number of times.
 * 
 * @param {number} iterations The number of times to retry the function.
 * @param {string} [value] If provided, a placeholder value to set if the function still errors after the final iteration.
 * If `value` is unset, the final error thrown by the function is re-thrown.
 * @returns {TransformationErrorFunction} A `TransformationErrorFunction` that may be passed to a `CSVTransformer`'s
 * `onError` option.
 * @throws If `value` is unset and the function throws an error after the final iteration, the error is re-thrown.
 */
function retry(iterations, value) {
  return async (row, e, fn) => {
    while (iterations--) {
      try { return await fn(row); }
      catch { }
    }
    if (value !== undefined) {
      return placeholder(value)(row, e, fn);
    }
    throw e;
  }
}

/**
 * Create a `TransformationErrorFunction` that handles errors by retrying it an `iterations` number of times, with an
 * exponentially increasing duration in between each retry (i.e., an exponential backoff).
 * 
 * @param {number} iterations The number of times to retry the function.
 * @param {number} [maxExponent] If provided, the maximum exponent used to calculate the retry duration. In other words,
 * the `iterations` number at which the retry duration stops increasing. If `maxExponent` is unset, the retry duration
 * is unbounded and will increase after each retry.
 * @param {string} [value] If provided, a placeholder value to set if the function still errors after the final iteration.
 * If `value` is unset, the final error thrown by the function is re-thrown.
 * @returns {TransformationErrorFunction} A `TransformationErrorFunction` that may be passed to a `CSVTransformer`'s
 * `onError` option.
 * @throws If `value` is unset and the function throws an error after the final iteration, the error is re-thrown.
 */
function backoff(iterations, maxExponent, value) {
  if (maxExponent === undefined) {
    maxExponent = Infinity;
  }
  return async (row, e, fn) => {
    for (let n = 0; n < iterations; n++) {
      const exponent = n > maxExponent ? maxExponent : n;
      const duration = (2 ** exponent) * 1000; // 1s, 2s, 4s, 8s, 16s, ...
      const jitter = Math.random() * 1000;
      await setTimeout(duration + jitter);
      try { return await fn(row); }
      catch { }
    }
    if (value !== undefined) {
      return placeholder(value)(row, e, fn);
    }
    throw e;
  }
}

export const ErrorStrategies = Object.create(null, {
  placeholder: { value: placeholder },
  retry: { value: retry },
  backoff: { value: backoff }
});
Object.freeze(ErrorStrategies);