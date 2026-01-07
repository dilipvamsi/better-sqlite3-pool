/**
 * @file lib/utils.js
 * @description Utilities for schema-aware type casting and UDF helpers.
 */
const fs = require("fs/promises");
const path = require("path");
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
 * Reconstructs a genuine SqliteError or Error from the worker payload.
 * @param {Object} errPayload - { message, code }
 * @returns {Error}
 */
function createError(errPayload) {
  // Guard against undefined/null payload
  if (!errPayload) {
    return new Error("Unknown Worker Error");
  }

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

  const err = new Error(msg);
  if (code) err.code = code; // Attach code if present (e.g. ENOENT)
  return err;
}

/**
 * Check if the file exists.
 * @param {string} path - file path
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
 * @param {string} path - file path
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

module.exports = {
  castRow,
  createError,
  fileExists,
  parentDirectoryExists,
};
