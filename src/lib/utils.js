/**
 * @file lib/utils.js
 * @description Utilities for schema-aware type casting and UDF helpers.
 */
const fs = require("fs/promises");
const path = require("path");
const betterSqlite3 = require("better-sqlite3-multiple-ciphers");

/**
 * SqliteError which contains the optional test data as well.
 */
class SqliteError extends betterSqlite3.SqliteError {
  constructor(message, code, __data = undefined) {
    super(message, code);
    this.__data = __data;
    Object.setPrototypeOf(this, SqliteError.prototype);
  }
}

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
 * Reconstructs a genuine SqliteError or Error from the worker payload.
 * @param {Object} errorPayload - { message, code }
 * @param {any} data - Additional data to include in the error.
 * @returns {Error}
 */
function createErrorByType(errorPayload, data = undefined) {
  let err;
  // console.log(error);
  switch (errorPayload.name) {
    case "TypeError":
      err = new TypeError(errorPayload.message);
      break;
    case "RangeError":
      err = new RangeError(errorPayload.message);
      break;
    case "ReferenceError":
      err = new ReferenceError(errorPayload.message);
      break;
    case "SyntaxError":
      err = new SyntaxError(errorPayload.message);
      break;
    case "URIError":
      err = new URIError(errorPayload.message);
      break;
    case "URIError":
      err = new URIError(errorPayload.message);
      break;
    case "EvalError":
      err = new EvalError(errorPayload.message);
      break;
    case "AggregateError":
      err = new AggregateError(errorPayload.message);
      break;
    case "Error":
      err = new Error(errorPayload.message);
      break;
    case "SqliteError":
      err = new SqliteError(errorPayload.message, errorPayload.code, data);
      break;
    default:
      err = new Error(errorPayload.message);
  }
  if (data && errorPayload.name !== "SqliteError") {
    err.__data = data;
  }
  return err;
}

/**
 * Check if the file exists.
 * @param {string} filePath - file path
 * @returns {Promise<boolean>}
 */
async function fileExists(filePath) {
  try {
    await fs.access(filePath); // Tries to access the file
    return true; // If successful, the file exists
  } catch (error) {
    // If an error is thrown, the file does not exist or is inaccessible
    if (error.code === "ENOENT") {
      return false;
    }
    // For other errors (e.g., permission issues), you might want to throw the error
    throw error;
  }
}

/**
 * Check if the parent directory exists.
 * @param {string} targetPath - file path
 * @returns {Promise<boolean>}
 */
async function parentDirectoryExists(targetPath) {
  // Get the path of the parent directory
  const parentDir = path.dirname(targetPath);

  try {
    // Check if the parent directory can be accessed (F_OK checks for visibility)
    await fs.access(parentDir, fs.constants.F_OK);
    // console.log(`Parent directory exists: ${parentDir}`);
    return true;
  } catch (error) {
    // If access fails, the directory likely doesn't exist (ENOENT error code)
    if (error.code === "ENOENT") {
      // console.log(`Parent directory does not exist: ${parentDir}`);
      return false;
    }
    // Re-throw other unexpected errors (e.g., permission issues)
    throw error;
  }
}

/**
 * Helper to serialize the aggregate options object.
 * Converts functions to strings.
 */
const serializeAggregateOptions = (opts) => {
  const out = { ...opts };
  if (typeof opts.step === "function") out.step = opts.step.toString();
  if (typeof opts.inverse === "function") out.inverse = opts.inverse.toString();
  if (typeof opts.result === "function") out.result = opts.result.toString();
  // Handle 'start' which can be a value OR a function
  if (typeof opts.start === "function") {
    out.start = opts.start.toString();
    out._startIsFunc = true; // internal flag to signal the worker
  }
  return out;
};

module.exports = {
  castRow,
  createErrorByType,
  fileExists,
  parentDirectoryExists,
  serializeAggregateOptions,
  SqliteError,
};
