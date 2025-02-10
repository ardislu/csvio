import { styleText } from 'node:util';

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
