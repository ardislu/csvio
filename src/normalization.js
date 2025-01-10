// User locale's decimal separator
const DECIMAL_SEPARATOR = Intl.NumberFormat().formatToParts(.1).find(p => p.type === 'decimal')?.value ?? '.';

/**
 * Convert a string to camelCase.
 * 
 * @param {string} str A string with words separated using whitespace, hyphens, or underscores. Words can be in lowercase,
 * uppercase, or a mix.
 * @returns {string} The same string in camelCase (i.e., all whitespace, hyphens, and underscores removed and the first
 * letter of each word after the first word capitalized).
 */
export function toCamelCase(str) {
  str = str.trim();
  if (!/[\s\-_]+(.)/.test(str)) {  // No separator found, fix the first letter but must assume the rest is already in camelCase
    return `${str[0].toLowerCase()}${str.substring(1)}`;
  }
  return str
    .toLowerCase()
    .replace(/[\s\-_]+(.)/g, m => m.substring(m.length - 1).toUpperCase());
}

/**
 * Convert scientific notation ("E" notation) to normal form.
 * 
 * @param {string} str A string representing a number expressed using scientific notation (e.g., `"1E+18"`).
 * @param {boolean} truncate If `true`, decimals will be truncated in the expanded string. Defaults to `false`.
 * @returns {string|null} A string representing the normal form of the number (e.g., `"1000000000000000000"`), or `null`
 * if the input could not be parsed.
 */
export function expandScientificNotation(str, truncate = false) {
  let expanded = null;
  const [mantissa, exponent] = str.split?.('E') ?? [undefined, undefined];
  if (mantissa !== undefined && exponent !== undefined) {
    const e = Number(exponent);
    if (mantissa.indexOf(DECIMAL_SEPARATOR) === -1) {
      expanded = mantissa + '0'.repeat(e);
    }
    else {
      const [integer, decimal] = mantissa.split('.');
      expanded = integer + decimal.padEnd(e, '0');
      if (truncate) {
        expanded = expanded.substring(0, e + 1);
      }
      else if (mantissa.length - 2 > e) { // Must re-insert decimal
        expanded = expanded.substring(0, e + 1) + '.' + expanded.substring(e + 1);
      }
    }
  }
  return expanded;
}

/**
 * Fix number mangling by Excel.
 * 
 * For example, converting numbers to dates or adding extra characters (e.g., whitespace, thousands separators, or currency symbols).
 * @param {string} str A string that might be a number.
 * @returns {number|string} If a fix was possible, a number. Otherwise, the original string is returned.
 */
export function fixExcelNumber(str) {
  if (str === '') { return str; }
  const original = str;

  // Can parse normally without any fixes
  const num1 = Number(original);
  if (!Number.isNaN(num1)) {
    return num1;
  }

  // Excel converted the number to a date
  const date = new Date(original);
  if (date.toString() !== 'Invalid Date') {
    // Excel incorrectly thinks 1900 was a leap year, need to offset 1 day before 3/1/1900
    const leapYearOffset = (original === '2/29/1900' || date.valueOf() < -2203862400000) ? -1 : 0;
    // Convert JavaScript date to Excel serialized date:
    // Days since Unix epoch - Timezone offset in days + Days between Jan 1, 1900 and Unix epoch start
    return (date.valueOf() / 1000 / 60 / 60 / 24) - (date.getTimezoneOffset() / 60 / 24) + 25569 + leapYearOffset;
  }

  // Excel added extra characters from setting the format to "Accounting" or other custom formats
  // Delete everything that's not a number or the decimal separator
  const replaced = original.replaceAll(new RegExp(`[^0-9${DECIMAL_SEPARATOR}]`, 'g'), '');
  const num2 = Number(replaced);
  if (!Number.isNaN(num2) && replaced !== '') {
    return num2;
  }

  // Otherwise, pass through as string
  console.warn(`Could not fix number: "${original}". Passing through as a string.`);
  return original;
}

/**
 * Fix BigInt mangling by Excel.
 * 
 * For example, converting BigInt to scientific notation.
 * @param {string} str A string that might be a BigInt.
 * @returns {string} If a fix was possible, a string that may be passed to the `BigInt()` constructor without error.
 * Otherwise, the original string is returned.
 * 
 * Note that **a string is always returned** to simplify downstream use because BigInt serialization is not possible with `JSON.stringify()`.
 */
export function fixExcelBigInt(str) {
  if (str === '') { return str; }
  const original = str;

  // Can parse normally without any fixes
  try {
    return BigInt(original).toString();
  } catch { }

  // Excel converted to scientific notation
  try {
    const expanded = expandScientificNotation(original, true);
    if (expanded === null) { throw new Error(); }
    return BigInt(expanded).toString();
  } catch { }

  // Excel added extra characters from setting the format to "Accounting" or other custom formats
  // Delete everything that's not a number or the decimal separator
  try {
    const str = original.replaceAll(new RegExp(`[^0-9${DECIMAL_SEPARATOR}]`, 'g'), '');
    if (str === '') { throw new Error(); }
    return BigInt(str.split(DECIMAL_SEPARATOR)[0]).toString();
  } catch { }

  // Otherwise, pass through as string
  console.warn(`Could not fix bigint: "${original}". Passing through as a string.`);
  return original;
}

/**
 * Fix date mangling by Excel.
 * 
 * For example, converting date to number.
 * @param {string} str A string that might be a date.
 * @returns {Date|string} If a fix was possible, a date. Otherwise, the original string is returned.
 */
export function fixExcelDate(str) {
  if (str === '') { return str; }
  const original = str;

  // Excel converted date to a number
  // This check MUST come before the new Date() check because numbers can be parsed as dates.
  const num = Number(original);
  if (!Number.isNaN(num)) {
    const day = Math.trunc(num); // Days elapsed since January 1, 1900, including non-existent February 29, 1900
    const time = (num % 1) * 86400000; // Time of day as decimal, so 0 = 12 AM and 0.999... = 11:59:59.999... PM
    if (num < 61) {
      return new Date(Date.UTC(0, 0, day, 0, 0, 0, time));
    }
    return new Date(Date.UTC(0, 0, day - 1, 0, 0, 0, time)); // Offset for non-existent February 29, 1900
  }

  // Can parse normally without any fixes
  const date = new Date(original);
  if (date.toString() !== 'Invalid Date') {
    return date;
  }

  // Otherwise, pass through as string
  console.warn(`Could not fix date: "${original}". Passing through as a string.`);
  return original;
}

/**
 * Metadata about CSV columns used to normalize raw CSV data.
 * @typedef {Object} CreateCSVNormalizationStreamHeader
 * @property {string} name The name of the CSV column, in camelCase. Values in the header row of the CSV will be
 * transformed to camelCase for comparison purposes and for downstream usage (e.g., a header with the value `Example Column`
 * in the input CSV will match with a `name` value of `exampleColumn`).
 * @property {'string'|'number'|'bigint'|'date'} type The JavaScript data type to attempt to cast this column to. If
 * the data cannot be unmangled, the data passes through as a `string`.
 * @property {string} [displayName=name] Optional `string` value to indicate the desired header name in the output CSV.
 * @property {string} [defaultValue] Optional `string` value to use to fill empty CSV fields. If no value is provided, the
 * field value will be `''` (empty string) in the output CSV.
 */

/**
 * Options to configure `createCSVNormalizationStream`.
 * @typedef {Object} CreateCSVNormalizationStreamOptions
 * @property {boolean} [passthroughEmptyRows=false] Set to `true` to send empty rows (i.e., rows where all field values are
 * `''`) downstream. Otherwise, empty rows will be removed. The default value is `false`.
 * @property {boolean} [passthroughNumber=false] Set to `true` to NOT apply data casting to `type='number'` columns. Otherwise,
 * there will be an attempt to cast columns with `type='number'` to a `number`. The default value is `false`.
 * @property {boolean} [passthroughBigInt=false] Set to `true` to NOT apply data casting to `type='bigint'` columns. Otherwise,
 * there will be an attempt to cast columns with `type='bigint'` to a `BigInt`. The default value is `false`.
 * @property {boolean} [passthroughDate=false] Set to `true` to NOT apply data casting to `type='date'` columns. Otherwise,
 * there will be an attempt to cast columns with `type='date'` to a `Date`. The default value is `false`.
 */

/**
 * The output of `createCSVNormalizationStream`.
 * @typedef {Object} CreateCSVNormalizationStreamRow
 * @property {string} name The column name after normalization to camelCase.
 * @property {string} displayName The desired column name in the output CSV.
 * @property {string|number} value The value of the field after attempted data casting. If the original field was empty, this value
 * is set to the provided `defaultValue` or `''` (empty string) if no `defaultValue` was provided.
 * @property {boolean} emptyField Boolean to indicate whether the original field was empty.
 */

/**
 * Create a `TransformStream` to transform raw CSV data into a JavaScript object using provided metadata.
 * 
 * Use this function to undo common CSV data mangling caused by spreadsheet programs such as Excel and perform other common 
 * cleanup such as renaming headers and removing empty rows.
 * @param {Array<CreateCSVNormalizationStreamHeader>} headers An array of metadata objects to configure the data casting and
 * cleanup transformations.
 * @param {CreateCSVNormalizationStreamOptions} options Object containing flags to configure the stream logic. 
 * @returns {TransformStream} A `TransformStream` where each chunk is one row of the CSV file after normalization, represented
 * as a JSON string of a `createCSVNormalizationStreamRow` object.
 */
export function createCSVNormalizationStream(headers, options = {}) {
  options.passthroughEmptyRows ??= false;
  options.passthroughNumber ??= false;
  options.passthroughBigInt ??= false;
  options.passthroughDate ??= false;

  const columns = [];
  for (const { name, type, displayName = name, defaultValue = null } of headers) {
    let normalizedType = type.toLowerCase();
    if (!['string', 'number', 'bigint', 'date'].includes(normalizedType)) {
      console.warn(`Type "${normalizedType}" is not supported, defaulting to string.`);
      normalizedType = 'string';
    }
    columns.push({
      name: toCamelCase(name),
      type: normalizedType,
      displayName,
      defaultValue,
      index: null
    });
  }

  let firstChunk = true;
  return new TransformStream({
    transform(chunk, controller) {
      const row = JSON.parse(chunk);

      // Assume first row is headers and use it to prepare the columns object
      // Note: the headers row is NOT forwarded downstream
      if (firstChunk) {
        const normalizedRow = row.map(f => toCamelCase(f));
        let i = 0;
        for (const header of normalizedRow) {
          const col = columns.find(c => c.name === header);
          if (col !== undefined) { // Drop columns provided in the CSV but not the headers input
            col.index = i;
          }
          i++;
        }
        columns.filter(c => c.index !== null); // Drop columns provided in the headers input but not in the CSV
        firstChunk = false;
        return;
      }

      const out = [];
      for (const { name, type, displayName, defaultValue, index } of columns) {
        let value = row[index];
        const emptyField = value === '' ? true : false;
        switch (type) {
          case 'string': break;
          case 'number': value = options.passthroughNumber ? value : fixExcelNumber(value); break;
          case 'bigint': value = options.passthroughBigInt ? value : fixExcelBigInt(value); break;
          case 'date': value = options.passthroughDate ? value : fixExcelDate(value); break;
        }
        if (emptyField && defaultValue !== null) {
          value = defaultValue;
        }
        out.push({ name, displayName, value, emptyField });
      }

      if (!options.passthroughEmptyRows && out.every(f => f.emptyField)) { return; }

      controller.enqueue(JSON.stringify(out));
    }
  });
}

/**
 * Create a `TransformStream` to transform a `createCSVNormalizationStreamRow` into an array that can be converted to CSV data.
 * 
 * @returns {TransformStream} A `TransformStream` where each chunk is one row of the CSV file.
 */
export function createCSVDenormalizationStream() {
  let firstRow = true;
  return new TransformStream({
    transform(chunk, controller) {
      const row = JSON.parse(chunk);
      if (firstRow) {
        firstRow = false;
        controller.enqueue(JSON.stringify(row.map(f => f.displayName)));
      }
      controller.enqueue(JSON.stringify(row.map(f => f.value)));
    }
  });
}
