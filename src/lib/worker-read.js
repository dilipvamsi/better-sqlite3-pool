/**
 * @file lib/worker-read.js
 * @description Dedicated Worker for READ operations with Streaming support.
 * Handles SELECT queries, Pragmas (read-only context), and UDF registration.
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
 * @property {boolean} [readonly] - Open the database in read-only mode.
 */

/**
 * @typedef {Object} WorkerPayload
 * @property {number} [id] - The message ID (used for correlation).
 * @property {'exec' | 'function' | 'stream_open' | 'stream_ack' | 'stream_close'} action - The operation to perform.
 * @property {string} [sql] - SQL query string.
 * @property {string} [fnName] - Function name (for UDFs).
 * @property {string} [fnString] - Function body string (for UDFs).
 * @property {Array<any>} [params] - Query bind parameters.
 * @property {Object} [options] - Execution options (pluck, raw, expand).
 * @property {string} [streamId] - Unique ID for stream sessions.
 */

/**
 * @typedef {Object} StreamSession
 * @property {Iterator<any>} iterator - The active better-sqlite3 iterator.
 */

// =============================================================================
// INITIALIZATION
// =============================================================================

// Extract configuration passed from the main thread
/** @type {WorkerData} */
const { filename, fileMustExist, timeout, nativeBinding } = workerData;

// console.log("reader: ",workerData);

let db;

try {
  const options = {
    readonly: true, // Readers are always read-only
  };

  if (fileMustExist) options.fileMustExist = true;
  if (nativeBinding) options.nativeBinding = nativeBinding;

  // Set busy_timeout via constructor options.
  // Default to 5000ms if not provided to match standard retry behavior.
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

// Enable BigInt support for large integers
db.defaultSafeIntegers(true);

// SIGNAL READY: Tell main thread we are initialized and ready for work
if (parentPort) parentPort.postMessage({ status: "ready" });

/** * Active stream sessions map.
 * Key: streamId, Value: StreamSession
 * @type {Map<string, StreamSession>}
 */
const activeStreams = new Map();

// =============================================================================
// MESSAGE HANDLER
// =============================================================================

if (parentPort) {
  parentPort.on(
    "message",
    /** @param {WorkerPayload} msg */
    ({ id, action, sql, params, streamId, options, fnName, fnString }) => {
      try {
        // --- 1. Handle UDF Registration (Synced from Manager) ---
        if (action === "function") {
          const fn = deserializeFunction(fnString);
          db.function(fnName, fn);
          // UDFs are usually fire-and-forget syncs, but we ack if an ID is present
          if (id !== -1) parentPort.postMessage({ id, status: "success" });
          return;
        }

        // --- 2. Handle Pragma (Synced from Manager) ---
        if (action === "exec") {
          db.exec(sql);
          if (id !== -1) parentPort.postMessage({ id, status: "success" });
          return;
        }

        // --- 3. Handle Streaming (Start) ---
        if (action === "stream_open") {
          const stmt = db.prepare(sql);

          // Apply options BEFORE creating iterator
          if (options) {
            if (options.pluck) stmt.pluck(true);
            if (options.raw) stmt.raw(true);
            if (options.expand) stmt.expand(true);
          }

          const iterator = stmt.iterate(...(params || []));
          activeStreams.set(streamId, { iterator });

          // Get metadata to send with the first batch (needed for BigInt casting)
          const columns =
            options && (options.raw || options.pluck)
              ? undefined
              : stmt.columns();

          // Send first batch immediately
          pumpStream(id, streamId, iterator, columns);
          return;
        }

        // --- 4. Handle Streaming (Flow Control) ---
        if (action === "stream_ack") {
          const session = activeStreams.get(streamId);
          if (session) {
            // Subsequent batches don't need column metadata
            pumpStream(id, streamId, session.iterator, undefined);
          }
          return;
        }

        // --- 5. Handle Streaming (Cleanup) ---
        if (action === "stream_close") {
          const session = activeStreams.get(streamId);
          if (session && session.iterator.return) {
            session.iterator.return();
          }
          activeStreams.delete(streamId);
          return;
        }

        // --- 6. Handle Standard Reads (all/get) ---
        const stmt = db.prepare(sql);

        // Apply Row Modes
        if (options) {
          if (options.pluck) stmt.pluck(true);
          if (options.raw) stmt.raw(true);
          if (options.expand) stmt.expand(true);
        }

        const rows = stmt.all(...(params || []));

        // Retrieve columns for casting logic on main thread
        const columns =
          options && (options.raw || options.pluck) ? [] : stmt.columns();

        parentPort.postMessage({
          id,
          status: "success",
          data: { rows, columns },
        });
      } catch (err) {
        // Send strictly formatted error back to main thread
        if (id !== -1) {
          parentPort.postMessage({
            id,
            status: "error",
            error: {
              message: err.message,
              code: err.code, // CRITICAL: Pass the SQLITE_ code
            },
            streamId, // Include streamId if error happened during stream
          });
        }
      }
    },
  );
}

// =============================================================================
// HELPERS
// =============================================================================

/**
 * Reads a small batch from the iterator and pushes to main thread.
 * @param {number} [msgId] - The original message ID (for logging/correlation).
 * @param {string} streamId - The unique stream session ID.
 * @param {Iterator<any>} iterator - The DB iterator.
 * @param {Array<any>} [columns] - Metadata to send (only for first batch).
 */
function pumpStream(msgId, streamId, iterator, columns) {
  const BATCH_SIZE = 50; // Keep RAM usage low per packet
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

    if (done) activeStreams.delete(streamId);

    // Send batch with special status 'stream_data'
    if (parentPort) {
      parentPort.postMessage({
        id: msgId,
        status: "stream_data",
        streamId, // CRITICAL: Main thread uses this to route to correct iterator
        data: rows,
        columns, // Only present in first batch
        done,
      });
    }
  } catch (err) {
    if (parentPort) {
      parentPort.postMessage({
        id: msgId,
        status: "stream_data", // Send as stream data so the listener catches it
        streamId,
        error: err.message,
      });
    }
  }
}
