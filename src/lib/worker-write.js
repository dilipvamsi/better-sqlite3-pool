/**
 * @file lib/worker-write.js
 * @description Dedicated Worker for all WRITE operations (INSERT, UPDATE, DELETE).
 * Also handles READ operations that occur inside transactions or involve RETURNING clauses.
 */

const { parentPort, workerData } = require("node:worker_threads");
const Database = require("better-sqlite3-multiple-ciphers");
const { deserializeFunction } = require("./utils");

// =============================================================================
// TYPE DEFINITIONS
// =============================================================================

/**
 * @typedef {Object} WorkerData
 * @property {string} filename - Path to the SQLite database file.
 * @property {boolean} [fileMustExist] - If true, throws if the database file does not exist.
 * @property {number} [timeout] - The number of milliseconds to wait when locking the database.
 * @property {string} [nativeBinding] - Path to the native addon executable.
 * @property {boolean} [readonly] - Open the database in read-only mode (unused by writer).
 */

/**
 * @typedef {Object} WorkerPayload
 * @property {number} [id] - The message ID (used for correlation).
 * @property {'run' | 'exec' | 'all' | 'get' | 'function'} action - The operation.
 * @property {string} [sql] - SQL query string.
 * @property {string} [fnName] - Function name (for UDFs).
 * @property {string} [fnString] - Function body string (for UDFs).
 * @property {Array<any>} [params] - Query bind parameters.
 * @property {Object} [options] - Execution options (pluck, raw, expand).
 */

// =============================================================================
// INITIALIZATION
// =============================================================================

// Extract configuration passed from the main thread
/** @type {WorkerData} */
const { filename, fileMustExist, timeout, nativeBinding } = workerData;

// console.log("writer: ",workerData);

let db;

try {
  const options = {};

  if (fileMustExist) options.fileMustExist = true;
  if (nativeBinding) options.nativeBinding = nativeBinding;

  // Set busy_timeout in constructor.
  // Default to 5000ms if not provided. This allows us to wait if the DB
  // is locked by an external process (e.g. previous test worker),
  // preventing startup crashes when setting WAL mode.
  options.timeout = timeout !== undefined ? timeout : 5000;

  db = new Database(filename, options);
} catch (err) {
  // Catch startup errors (e.g. file does not exist, binding issues)
  // and report them back to the main thread so the Promise rejects.
  if (parentPort) {
    parentPort.postMessage({
      status: "error",
      error: {
        message: err.message || "Worker initialization failed",
        code: err.code, // CRITICAL: Pass the SQLITE_ code (e.g. SQLITE_CANTOPEN)
      },
    });
    process.exit(0);
  }
  throw err;
}

const isMemory = filename === ":memory:" || filename === "";

try {
  /**
   * Configure Connection Pragma.
   * Strictly only for file-based databases to avoid V8 panics on memory DBs.
   */
  if (!isMemory) {
    // Enable Write-Ahead Logging (WAL).
    // This requires a lock. Thanks to options.timeout, we wait if necessary.
    db.pragma("journal_mode = WAL");
  }
} catch (err) {
  // Suppress initialization errors regarding pragmas.
  // In some environments, failing to set a pragma during worker boot can cause
  // issues, but if the DB is open, we try to proceed.
  if (parentPort) {
    parentPort.postMessage({
      id: -1,
      status: "error",
      error: `Init Warning: ${err.message}`,
    });
  }
}

/**
 * Enable BigInt support.
 * We read all integers as BigInts initially. The main thread's `castRow` helper
 * will downcast them to Numbers if they are small enough (unless raw mode is on).
 */
db.defaultSafeIntegers(true);

// SIGNAL READY: Tell main thread the file exists and we are ready to accept writes.
if (parentPort) parentPort.postMessage({ status: "ready" });

// =============================================================================
// MESSAGE HANDLER
// =============================================================================

if (parentPort) {
  parentPort.on(
    "message",
    /** @param {WorkerPayload} msg */
    ({ id, action, sql, params, options, fnName, fnString }) => {
      // console.log("Write message:", {
      //   id,
      //   action,
      //   sql,
      //   params,
      //   options,
      //   fnName,
      //   fnString,
      // });
      try {
        // --- Close Database Connection ---
        if (action === "close") {
          if (db && db.open) {
            db.close();
          }
          if (id !== -1) parentPort.postMessage({ id, status: "success" });
          process.exit(0);
        }

        // --- 1. Handle UDF Registration ---
        if (action === "function") {
          const fn = deserializeFunction(fnString);
          db.function(fnName, fn);
          if (id !== -1) parentPort.postMessage({ id, status: "success" });
          return;
        }

        // --- 2. Handle Pragma / Exec ---
        // Used for setting pragmas or running simple execution strings
        if (action === "exec") {
          db.exec(sql);
          if (id !== -1) parentPort.postMessage({ id, status: "success" });
          return;
        }

        // --- 3. Handle Standard Queries ---
        const stmt = db.prepare(sql);

        // Apply Row Modes (if provided)
        if (options) {
          if (options.pluck) stmt.pluck(true);
          if (options.raw) stmt.raw(true);
          if (options.expand) stmt.expand(true);
        }

        let data;

        // BRANCH A: Write Operation (INSERT / UPDATE / DELETE)
        if (action === "run") {
          const info = stmt.run(...(params || []));
          data = {
            lastInsertRowid: info.lastInsertRowid,
            changes: info.changes,
          };
        }
        // BRANCH B: Read Operation on Writer (Transaction or RETURNING)
        else {
          // action is 'all' or 'get'
          // @ts-ignore - dynamic method call
          const rows = stmt[action](...(params || []));

          // Only fetch columns if we need to map types (not needed for raw/pluck)
          const columns =
            options && (options.raw || options.pluck) ? [] : stmt.columns();

          data = { rows, columns };
        }

        // Send Result
        parentPort.postMessage({ id, status: "success", data });
      } catch (err) {
        // Handle Errors (Constraint violations, Syntax errors, etc.)
        if (id !== -1) {
          parentPort.postMessage({
            id,
            status: "error",
            error: {
              message: err.message,
              code: err.code, // CRITICAL: Pass the SQLITE_ constraint code
            },
          });
        }
      }
    },
  );
}
