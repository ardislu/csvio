import { styleText } from 'node:util';

/**
 * Measure the time it tasks to execute `fn` and log the result to console.
 * 
 * @param {string} name A label for the benchmark.
 * @param {function} fn The function to measure.
 */
export async function benchmark(name, fn) {
  performance.mark('start');
  await fn();
  performance.mark('end');
  performance.measure('measure', 'start', 'end');

  const measures = performance.getEntriesByType('measure');
  const { duration } = measures[0];

  console.log(`${styleText('green', name)} | ${styleText(['yellow', 'underline'], `${duration.toFixed(3)}ms`)} execution time`);

  performance.clearMarks();
  performance.clearMeasures();
}

/**
 * Measure the average time it takes for `fn` to execute across `iterations` number of executions and
 * log the result to console.
 * 
 * @param {string} name A label for the benchmark.
 * @param {number} iterations The number of times to execute the function.
 * @param {function} fn The function to benchmark.
 */
export async function benchmarkIterations(name, iterations, fn) {
  for (let i = 0; i < iterations; i++) {
    performance.mark(`start:${i}`);
    await fn();
    performance.mark(`end:${i}`);
    performance.measure(`measure:${i}`, `start:${i}`, `end:${i}`);
  };

  const measures = performance.getEntriesByType('measure');
  const average = measures.reduce(((a, c) => a + c.duration), 0) / measures.length;

  console.log(`${styleText('green', name)} | ${styleText(['yellow', 'underline'], `${average.toFixed(3)}ms`)} average execution time in ${styleText(['yellow', 'underline'], iterations.toString())} iterations`);

  performance.clearMarks();
  performance.clearMeasures();
}
