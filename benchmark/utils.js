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
