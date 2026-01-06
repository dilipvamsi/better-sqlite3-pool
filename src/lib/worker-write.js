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

const db = new Database(workerData.filename);

/**
 * Enable Write-Ahead Logging (WAL).
 * This is CRITICAL for concurrency. It allows Readers to read from the DB
 * while this Writer is writing to it, without blocking each other.
 */
db.pragma("journal_mode = WAL");

/**
 * Set a busy timeout (5000ms).
 * If the DB is locked by an external process (e.g. backup), wait 5s before throwing SQLITE_BUSY.
 */
db.pragma("busy_timeout = 5000");

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
      try {
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
