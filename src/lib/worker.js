/**
 * @file lib/worker.js
 * @description Unified Worker for SQLite operations.
 * Operates in two modes:
 * - 'write': Handles transactions, INSERT/UPDATE/DELETE, and WAL configuration.
 * - 'read':  Handles high-concurrency SELECTs in read-only mode.
 */

const { parentPort, workerData } = require("node:worker_threads");
const Database = require("better-sqlite3-multiple-ciphers");

// =============================================================================
// TYPE DEFINITIONS
// =============================================================================

/**
 * @typedef {Object} WorkerData
 * @property {'read' | 'write'} mode - The operation mode.
 * @property {string} filename - Path to the SQLite database file.
 * @property {boolean} [fileMustExist] - If true, throws if the database file does not exist.
 * @property {number} [timeout] - The number of milliseconds to wait when locking the database.
 * @property {string} [nativeBinding] - Path to the native addon executable.
 */

/**
 * @typedef {Object} StatementOptions
 * @property {boolean} [pluck] - If true, returns only the first column.
 * @property {boolean} [raw] - If true, returns raw arrays instead of objects.
 * @property {boolean} [expand] - If true, expands dot-notation keys.
 */

/**
 * @typedef {Object} WorkerPayload
 * @property {'run' | 'exec' | 'all' | 'get' | 'function' | 'close' | 'stream_open' | 'steam_next' | 'stream_close'} action - The operation.
 * @property {string} [sql] - SQL query string.
 * @property {string} [fnName] - Function name (for UDFs).
 * @property {string} [fnString] - Function body string (for UDFs).
 * @property {Array<any>} [params] - Query bind parameters.
 * @property {StatementOptions} [options] - Execution options.
 * @property {string} [streamId] - Unique ID for streaming sessions.
 */

/**
 * @typedef {Object} WorkerRequest
 * @property {string} requestId - Unique ID for request correlation.
 * @property {WorkerPayload} data - The operation payload.
 */

/**
 * @typedef {Object} SerializedError
 * @property {string} message - The error message.
 * @property {string} code - The error code (e.g., SQLITE_CONSTRAINT, SQLITE_ERROR).
 */

/**
 * @typedef {Object} WorkerResultData
 * @property {Array<any>} [rows] - Result rows (for read ops & streams).
 * @property {Array<any>} [columns] - Column metadata (for read ops & streams).
 * @property {number|bigint} [lastInsertRowid] - (for write ops).
 * @property {number} [changes] - (for write ops).
 * @property {any} [pragma] - Result of a PRAGMA command.
 */

/**
 * @typedef {Object} WorkerResponse
 * @property {string} requestId - Correlates to the request.
 * @property {'success' | 'error' | 'stream_data'} status - Outcome status.
 * @property {WorkerResultData} [data] - Standardized payload for all success/stream responses.
 * @property {boolean} [done] - End of stream signal.
 * @property {SerializedError} [error] - Standardized error object.
 * @property {string} [streamId] - Stream ID (for stream errors).
 */

// =============================================================================
// HELPER: ERROR FORMATTER
// =============================================================================

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
 * Helper to throw an error with a specific SQLITE code.
 * @param {string} message
 * @param {string} code
 */
function throwSqliteError(message, code = "SQLITE_ERROR") {
  const err = new Error(message);
  // @ts-ignore
  err.code = code;
  throw err;
}

/**
 * Standardizes any error into a structure resembling SqliteError.
 * @param {Error|any} err - The original error.
 * @returns {SerializedError}
 */
function formatError(err) {
  // If it's already a better-sqlite3 error, it has a code (e.g. SQLITE_CONSTRAINT).
  // If it's a generic JS error (TypeError), assign SQLITE_ERROR.
  const code =
    err.code !== undefined && typeof err.code === "string"
      ? err.code
      : "SQLITE_ERROR";
  const message = err.message;
  return { message, code };
}

// =============================================================================
// INITIALIZATION
// =============================================================================

/** @type {WorkerData} */
const { filename, fileMustExist, timeout, nativeBinding, mode } = workerData;

// console.log(`[Worker:${mode}] Starting... DB: ${filename}`);

// console.log(`${mode}: `, workerData);

const isWriter = mode === "write";
const isMemory = filename === ":memory:" || filename === "";

let db;

try {
  const options = {};

  // 1. Configure Mode-Specific Options
  if (!isWriter) {
    // Readers must strictly be Read-Only
    options.readonly = true;
  }

  if (fileMustExist) options.fileMustExist = true;
  if (nativeBinding) options.nativeBinding = nativeBinding;

  // Default timeout prevents immediate failures if DB is locked by another process
  options.timeout = timeout !== undefined ? timeout : 5000;

  db = new Database(filename, options);
} catch (err) {
  // Report initialization error to pool
  if (parentPort) {
    parentPort.postMessage({
      status: "error",
      error: formatError(err),
    });
    process.exit(1);
  }
  throw err;
}

try {
  // 2. Setup WAL Mode
  // - Only allowed for WRITERS (Readers are readonly and cannot change pragmas)
  // - Only allowed for FILE-BASED databases (Memory DBs don't use WAL files)
  if (isWriter && !isMemory) {
    db.pragma("journal_mode = WAL");
  }
} catch (err) {
  if (parentPort) {
    parentPort.postMessage({
      status: "error",
      error: formatError(err),
    });
  }
}

// Enable BigInt support for large integers
db.defaultSafeIntegers(true);

if (parentPort) parentPort.postMessage({ status: "ready" });

// =============================================================================
// STATE MANAGEMENT
// =============================================================================

/** @type {Map<string, Iterator<any>>} Stores active iterators for streaming. */
const activeStreams = new Map();

// =============================================================================
// MESSAGE HANDLER
// =============================================================================

if (parentPort) {
  parentPort.on("message", handleMessage);
}

/**
 * Handles incoming messages from the main thread.
 * @param {WorkerRequest} request
 */
function handleMessage({ requestId, data }) {
  if (!data) return;

  const { action, sql, params, options, fnName, fnString, streamId } = data;

  // console.log(`${mode}: message:`, data);

  /**
   * Helper to send standardized responses.
   * @param {Partial<WorkerResponse>} payload
   */
  const respond = (payload) => {
    parentPort.postMessage({ requestId, ...payload });
  };

  try {
    // --- 1. CONNECTION MANAGEMENT ---
    if (action === "close") {
      cleanupStreams();
      if (db && db.open) {
        try {
          db.close();
        } catch (e) {
          /* ignore */
        }
      }
      respond({ status: "success" });
      return;
    }

    // --- 2. UDF REGISTRATION ---
    if (action === "function") {
      const fn = deserializeFunction(fnString);
      db.function(fnName, fn);
      respond({ status: "success" });
      return;
    }

    // --- 3. BASIC EXECUTION (DDL) ---
    if (action === "exec") {
      db.exec(sql);
      respond({ status: "success" });
      return;
    }

    // --- 4. PRAGMA HANDLER ---
    if (action === "pragma") {
      const result = db.pragma(sql, options);
      // Map pragma result to 'pragma' property to keep types clean,
      // or just 'rows' if it returns a set. For generic consistency, we use 'pragma'.
      respond({ status: "success", data: { pragma: result } });
      return;
    }

    // --- 5. STREAMING PROTOCOL ---
    if (action === "stream_open") {
      const stmt = prepareStatement(sql, options);
      const iterator = stmt.iterate(...(params || []));
      activeStreams.set(streamId, iterator);

      // Get columns for first batch (if needed for casting)
      const columns =
        options && (options.raw || options.pluck) ? undefined : stmt.columns();

      const batch = getBatch(streamId, iterator, columns);
      respond(batch);
      return;
    }

    if (action === "steam_next") {
      const iterator = activeStreams.get(streamId);
      if (iterator) {
        const batch = getBatch(streamId, iterator, undefined);
        respond(batch);
      } else {
        // Stream likely closed or finished
        respond({ rows: [], done: true });
      }
      return;
    }

    if (action === "stream_close") {
      closeStream(streamId);
      respond({ status: "success" }); // Ack the close
      return;
    }

    // --- 6. STANDARD QUERY EXECUTION ---
    const stmt = prepareStatement(sql, options);

    // VALIDATION & ROUTING
    if (action === "run") {
      if (!isWriter) {
        // Enforce specific error code for read-only violation
        throwSqliteError(
          "Cannot execute 'run' (write) operation in read-only mode",
          "SQLITE_READONLY",
        );
      }
      const info = stmt.run(...(params || []));
      respond({
        status: "success",
        data: { lastInsertRowid: info.lastInsertRowid, changes: info.changes },
      });
      return;
    } else if (action === "all" || action === "get") {
      // @ts-ignore
      const result = stmt[action](...(params || []));

      const columns =
        options && (options.raw || options.pluck) ? [] : stmt.columns();

      // Normalize response structure
      const rows = action === "get" ? (result ? [result] : []) : result;

      respond({
        status: "success",
        data: { rows, columns },
      });
      return;
    } else {
      // Enforce specific error code for unknown actions (Protocol misuse)
      throwSqliteError(`Unknown action: ${action}`, "SQLITE_MISUSE");
    }
  } catch (err) {
    respond({
      status: "error",
      error: formatError(err),
    });
  }
}

// =============================================================================
// HELPERS
// =============================================================================

function prepareStatement(sql, options) {
  const stmt = db.prepare(sql);
  if (options) {
    if (options.pluck) stmt.pluck(true);
    if (options.raw) stmt.raw(true);
    if (options.expand) stmt.expand(true);
  }
  return stmt;
}

function closeStream(streamId) {
  const iterator = activeStreams.get(streamId);
  if (iterator && iterator.return) {
    try {
      iterator.return();
    } catch (e) {
      /* ignore */
    }
  }
  activeStreams.delete(streamId);
}

function cleanupStreams() {
  for (const streamId of activeStreams.keys()) {
    closeStream(streamId);
  }
}

/**
 * Syncronously fetches a batch of rows.
 * Returns the object structure expected by WorkerResultData.
 */
function getBatch(streamId, iterator, columns) {
  const BATCH_SIZE = 50;
  const rows = [];
  let done = false;

  try {
    for (let i = 0; i < BATCH_SIZE; i++) {
      const next = iterator.next();
      if (next.done) {
        done = true;
        break;
      }
      rows.push(next.value);
    }
  } catch (err) {
    // If iteration throws, close and re-throw (caught by main try/catch)
    closeStream(streamId);
    throw err;
  }

  if (done) activeStreams.delete(streamId);

  return {
    rows,
    columns, // Only sent on first batch
    done,
  };
}
