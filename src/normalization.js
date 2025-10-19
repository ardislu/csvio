import { TransformStream } from 'node:stream/web';
/** @import { TransformerTransformCallback } from 'node:stream/web'; */

/**
 * Get the decimal separator character to parse numbers formatted in different locales.
 * @param {string|Intl.Locale} [locale] A `string` Unicode locale identifier, or an `Intl.Locale` object specifying
 * the locale that is applicable. If no value is provided, the current execution environment's locale is used.
 * @returns {string} The locale's decimal separator as a `string` (e.g., "." for the "en-US" locale).
 */
export function getDecimalSeparator(locale) {
  return Intl.NumberFormat(locale).formatToParts(.1).find(p => p.type === 'decimal').value;
}

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
  if (str === '') { return ''; }
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
 * @param {boolean} [truncate=false] If `true`, decimals will be truncated in the expanded string. Defaults to `false`.
 * @returns {string} A string representing the normal form of the number (e.g., `"1000000000000000000"`).
 * @throws {TypeError} If `str` is not a `string`, a `TypeError` will be thrown.
 * @throws {SyntaxError} If `str` is not valid scientific notation, a `SyntaxError` will be thrown.
 */
export function expandScientificNotation(str, truncate = false) {
  if (typeof str !== 'string') {
    throw new TypeError(`Expected a string, but received ${typeof str}.`);
  }
  const [mantissa, exponent] = str.split('E');
  if (mantissa === undefined || exponent === undefined) {
    throw new SyntaxError(`Unable to parse string "${str}" as scientific notation.`);
  }
  const e = Number(exponent);
  let expanded = null;
  if (mantissa.indexOf(getDecimalSeparator()) === -1) {
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
  return expanded;
}

/**
 * Metadata about CSV columns used to normalize raw CSV data.
 * @typedef {Object} CSVNormalizerHeader
 * @property {string} name The name of the CSV column, in camelCase. Values in the header row of the CSV will be
 * transformed to camelCase for comparison purposes and for downstream usage (e.g., a header with the value `Example Column`
 * in the input CSV will match with a `name` value of `exampleColumn`).
 * @property {'string'|'number'|'bigint'|'date'} [type='string'] The JavaScript data type to attempt to cast this column to. If
 * the data cannot be unmangled, the data passes through as a `string`. The default value is `'string'`.
 * @property {string} [displayName=name] Optional `string` value to indicate the desired header name in the output CSV.
 * @property {string} [defaultValue=''] Optional `string` value to use to fill empty CSV fields. If no value is provided, the
 * field value will be `''` (empty string) in the output CSV.
 */

/**
 * Options to configure `CSVNormalizer`.
 * @typedef {Object} CSVNormalizerOptions
 * @property {boolean} [useLiteralNames=false] Set to `true` to use the literal `name` value to match with literal input CSV header
 * names. Otherwise, both `name` and the input CSV header name will be normalized (i.e., trimmed and converted to camelCase).
 * The default value is `false`.
 * @property {boolean} [passthroughEmptyRows=false] Set to `true` to send empty rows (i.e., rows where all field values are
 * `''`) downstream. Otherwise, empty rows will be removed. The default value is `false`.
 * @property {boolean} [typeCastOnly=false] Set to `true` to NOT apply any error correction to 'number', `bigint`, or `date`
 * columns. Only type casting will be applied to the data. The default value is `false`.
 */

/**
 * The output of `CSVNormalizer`.
 * @typedef {Object} CSVNormalizerField
 * @property {string} name The column name after normalization to camelCase.
 * @property {string} [displayName] The desired column name in the output CSV.
 * @property {any} value The value of the field after attempted data casting. If the original field was empty, this value
 * is set to the provided `defaultValue` or `''` (empty string) if no `defaultValue` was provided.
 * @property {boolean} [emptyField] Boolean to indicate whether the original field was empty.
 */

/**
 * A `TransformStream` to transform raw CSV data into a JavaScript object using provided metadata.
 * 
 * Use this class to undo common CSV data mangling caused by spreadsheet programs such as Excel and perform other common 
 * cleanup such as renaming headers and removing empty rows.
 * @extends TransformStream
 */
export class CSVNormalizer extends TransformStream {
  #columns = [];
  #headersCaptured = false;
  #useLiteralNames;
  #passthroughEmptyRows;
  #typeCastOnly;

  /**
   * @param {Array<CSVNormalizerHeader|string>} headers An array of `CSVNormalizerHeader` or `string` to configure the
   * data casting and cleanup transformations. Passing a plain `string` is the same as passing `{ name: string }`.
   * @param {CSVNormalizerOptions} options Object containing flags to configure the stream logic. 
   */
  constructor(headers, options = {}) {
    super({
      transform: (chunk, controller) => this.#transform(chunk, controller)
    });

    this.#useLiteralNames = options.useLiteralNames ?? false;
    this.#passthroughEmptyRows = options.passthroughEmptyRows ?? false;
    this.#typeCastOnly = options.typeCastOnly ?? false;

    for (const header of headers) {
      if (typeof header === 'string') {
        const name = this.#useLiteralNames ? header : toCamelCase(header);
        this.#columns.push({
          name,
          type: 'string',
          displayName: name,
          defaultValue: '',
          index: null
        });
      }
      else {
        const { name, type, displayName = name, defaultValue = '' } = header;
        let normalizedType = type?.toLowerCase() ?? 'string';
        if (!['string', 'number', 'bigint', 'date'].includes(normalizedType)) {
          console.warn(`Type "${normalizedType}" is not supported, defaulting to string.`);
          normalizedType = 'string';
        }
        this.#columns.push({
          name: this.#useLiteralNames ? name : toCamelCase(name),
          type: normalizedType,
          displayName,
          defaultValue,
          index: null
        });
      }
    }
  }

  /** @type {TransformerTransformCallback<Array<string>,Array<Required<CSVNormalizerField>>>} */
  #transform(chunk, controller) {
    // Assume first non-empty row is headers and use it to prepare the columns object
    // Note: the headers row is NOT forwarded downstream
    if (!this.#headersCaptured) {
      if (chunk.every(v => v === '')) { return; } // All empty rows before the header row are removed, even if passthroughEmptyRows is true
      const normalizedRow = this.#useLiteralNames ? chunk : chunk.map(f => toCamelCase(f));
      let i = 0;
      for (const header of normalizedRow) {
        const col = this.#useLiteralNames
          ? this.#columns.find(c => c.name === header)
          : this.#columns.find(c => c.name.toLocaleLowerCase() === header.toLocaleLowerCase());
        if (col !== undefined) { // Drop columns provided in the CSV but not the headers input
          col.index = i;
        }
        i++;
      }
      this.#columns = this.#columns.filter(c => c.index !== null); // Drop columns provided in the headers input but not in the CSV
      this.#headersCaptured = true;
      return;
    }

    /** @type {Array<Required<CSVNormalizerField>>} */
    const out = [];
    for (const { name, type, displayName, defaultValue, index } of this.#columns) {
      let value = chunk[index] ?? '';
      const emptyField = value === '';
      switch (type) {
        case 'string': break;
        case 'number': value = this.#typeCastOnly ? Number(value) : CSVNormalizer.fixExcelNumber(value); break;
        case 'bigint': value = this.#typeCastOnly ? BigInt(value) : CSVNormalizer.fixExcelBigInt(value); break;
        case 'date': value = this.#typeCastOnly ? new Date(value) : CSVNormalizer.fixExcelDate(value); break;
      }
      if (emptyField) {
        value = defaultValue;
      }
      out.push({ name, displayName, value, emptyField });
    }

    if (!this.#passthroughEmptyRows && out.every(f => f.emptyField)) { return; }

    controller.enqueue(out);
  }

  /**
   * Fix number mangling by Excel.
   * 
   * For example, converting numbers to dates or adding extra characters (e.g., whitespace, thousands separators, or currency symbols).
   * @param {string} str A string that might be a number.
   * @returns {number|string} If a fix was possible, a number. Otherwise, the original string is returned.
   */
  static fixExcelNumber(str) {
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
    const replaced = original.replaceAll(new RegExp(`[^0-9${getDecimalSeparator()}]`, 'g'), '');
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
   * @returns {bigint|string} If a fix was possible, a `bigint`. Otherwise, the original string is returned.
   */
  static fixExcelBigInt(str) {
    if (str === '') { return str; }
    const original = str;

    // Can parse normally without any fixes
    try {
      return BigInt(original);
    } catch { }

    // Excel converted to scientific notation
    try {
      const expanded = expandScientificNotation(original, true);
      return BigInt(expanded);
    } catch { }

    // Excel added extra characters from setting the format to "Accounting" or other custom formats
    // Delete everything that's not a number or the decimal separator
    try {
      const decimalSeparator = getDecimalSeparator();
      const str = original.replaceAll(new RegExp(`[^0-9${decimalSeparator}]`, 'g'), '');
      if (str === '') { throw new Error(); }
      return BigInt(str.split(decimalSeparator)[0]);
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
  static fixExcelDate(str) {
    if (str === '') { return str; }
    const original = str;

    // Excel converted date to a number
    // This check MUST come before the new Date() check because numbers can be parsed as dates.
    // Assumptions about what the number represents, with date bounds:
    // num <= 200000 (2447-07-30): native Excel date
    // 200000 (1970-01-03) < num <= 10000000000 (2286-11-20): Unix timestamp (seconds)
    // 10000000000 (1970-04-26) < num: Unix timestamp (milliseconds)
    const num = Number(original);
    if (!Number.isNaN(num)) {
      if (num <= 200000) { // Assume it's a native Excel date casted to number
        const day = Math.trunc(num); // Days elapsed since January 1, 1900, including non-existent February 29, 1900
        const time = (num % 1) * 86400000; // Time of day as decimal, so 0 = 12 AM and 0.999... = 11:59:59.999... PM
        if (num < 61) {
          return new Date(Date.UTC(0, 0, day, 0, 0, 0, time));
        }
        return new Date(Date.UTC(0, 0, day - 1, 0, 0, 0, time)); // Offset for non-existent February 29, 1900
      }
      else if (200000 < num && num <= 10000000000) { // Assume it's a Unix timestamp (seconds)
        return new Date(num * 1000);
      }
      else { // Assume it's a Unix timestamp (milliseconds)
        return new Date(num);
      }
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
   * Create a simple object that maps each `CSVNormalizerField` `name` to its `value`. Useful if you just want to read all the
   * values in a normalized CSV row.
   * 
   * The `displayName` and `emptyField` values of a field are lost in the resulting object.
   * @param {Array<CSVNormalizerField>} row A normalized CSV row.
   * @returns {Record<string,any>} An object where each key is a field `name` and the value is the field's `value`.
   */
  static toObject(row) {
    const object = {};
    for (const field of row) {
      object[field.name] = field.value;
    }
    return object;
  }

  /**
   * Create a subset of the `Map` interface to get and set the `value` of a field using its `name`.
   * 
   * The return value of this method is a `Proxy` of a `Map` that intercepts the `Map` methods so that it gets and sets
   * values by *reference* to the underlying `CSVNormalizerField`. This interception makes the `Map` stay in sync with the
   * `row` array.
   * 
   * Only the methods `.get()`, `.set()`, `.has()` and the property `.size` are supported. All other methods are unsupported.
   * For more advanced usage, use the underlying `row` object directly.
   * 
   * The `displayName` and `emptyField` values of a field cannot be accessed from the `Map`.
   * 
   * Calling `.set()` with a `name` that is not in the row will push a new field to the row.
   * @param {Array<CSVNormalizerField>} row A normalized CSV row.
   * @returns {Pick<Map<string,any>,'get'|'set'|'has'|'size'>} A `Proxy` over a `Map` object with keys set to the `name` of
   * each field in the row.
   */
  static toFieldMap(row) {
    const map = new Map();
    for (const field of row) {
      map.set(field.name, field);
    }
    return new Proxy(map, {
      get(target, prop, receiver) {
        if (prop === 'get') {
          return name => target.get(name)?.value;
        }
        if (prop === 'set') {
          return (name, value) => {
            if (target.has(name)) {
              target.get(name).value = value;
            }
            else {
              const field = { name, value };
              row.push(field);
              target.set(name, field);
            }
            return receiver;
          }
        }
        if (prop === 'has' || prop === 'size') {
          const targetProp = Reflect.get(target, prop);
          return typeof targetProp === 'function' ? targetProp.bind(target) : targetProp;
        }
      }
    });
  }
}

/**
 * A `TransformStream` to transform a `Array<CSVNormalizerField>` into an `Array<string>` that can be converted to CSV data.
 * @extends TransformStream
 */
export class CSVDenormalizer extends TransformStream {
  constructor() {
    let firstRow = true;
    super({
      /** @type {TransformerTransformCallback<Array<CSVNormalizerField>,Array<string>>} */
      transform(chunk, controller) {
        if (firstRow) {
          firstRow = false;
          controller.enqueue(chunk.map(f => f?.displayName ?? f.name));
        }
        controller.enqueue(chunk.map(f => {
          if (f.value instanceof Date) {
            return f.value.toISOString();
          }
          return f.value?.toString?.() ?? '';
        }));
      }
    });
  }
}
