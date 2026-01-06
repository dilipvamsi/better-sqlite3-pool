/**
 * @file lib/utils.js
 * @description Utilities for schema-aware type casting and UDF helpers.
 */
const { SqliteError } = require("better-sqlite3-multiple-ciphers");

// SQL types that must ALWAYS remain BigInt in JS to preserve precision.
const BIGINT_TYPES = new Set(["BIGINT", "INT64", "UNSIGNED BIG INT"]);

// Safe integer bounds for JavaScript Numbers.
const MAX_SAFE = Number.MAX_SAFE_INTEGER;
const MIN_SAFE = Number.MIN_SAFE_INTEGER;

/**
 * Mutates row objects in-place to convert safe BigInts to Numbers.
 * * Logic:
 * 1. If column is explicitly declared BIGINT in schema -> Keep as BigInt.
 * 2. If column is INTEGER/COUNT etc -> Convert to Number IF it fits in 53-bit float.
 * 3. If value is huge (> 2^53) -> Keep as BigInt to avoid corruption.
 * * @param {Object} row - The row object to cast.
 * @param {Array<{name: string, type: string}>} columns - Metadata from better-sqlite3.
 * @returns {Object} The mutated row.
 */
function castRow(row, columns) {
  if (!row || !columns) return row;

  for (const col of columns) {
    const key = col.name;
    const val = row[key];

    // Only inspect BigInt values. Numbers/Strings/Nulls are already safe.
    if (typeof val === "bigint") {
      const declaredType = (col.type || "").toUpperCase();

      // Rule 1: Strict Schema Adherence
      if (BIGINT_TYPES.has(declaredType)) {
        continue;
      }

      // Rule 2: Smart Downgrade
      if (val >= MIN_SAFE && val <= MAX_SAFE) {
        row[key] = Number(val);
      }
    }
  }
  return row;
}

/**
 * Reconstructs a function from a string.
 * Used for passing UDFs to worker threads.
 * @param {string} fnString - The .toString() of a function.
 * @returns {Function} The executable function.
 */
function deserializeFunction(fnString) {
  // Wrap in parenthesis to ensure it evaluates as an expression, not a statement
  return (0, eval)(`(${fnString})`);
}

/**
 * Reconstructs a genuine SqliteError or Error from the worker payload.
 * @param {Object} errPayload - { message, code }
 * @returns {Error}
 */
function createError(errPayload) {
  // 1. If it's already a SqliteError (e.g. caught locally), return it as-is.
  if (errPayload instanceof SqliteError) {
    return errPayload;
  }

  // 2. If it's a standard Error object (local), return it as-is.
  if (errPayload instanceof Error) {
    return errPayload;
  }

  // 3. Worker Payload (Structured Clone) handling
  // Fallback to string if message is undefined (fixes "SqliteError: undefined")
  const msg =
    (errPayload && errPayload.message) ||
    String(errPayload || "Unknown Worker Error");

  const code = errPayload && errPayload.code;

  if (code && typeof code === "string" && code.startsWith("SQLITE_")) {
    return new SqliteError(msg, code);
  }

  return new Error(msg);
}

module.exports = { castRow, deserializeFunction, createError };
