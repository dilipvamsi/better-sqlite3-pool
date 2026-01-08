/**
 * @file lib/worker.js
 * @description Unified Worker for SQLite operations.
 * Operates in two modes:
 * - 'write': Handles transactions, INSERT/UPDATE/DELETE, and WAL configuration.
 * - 'read':  Handles high-concurrency SELECTs in read-only mode.
 */

const { parentPort, workerData } = require("node:worker_threads");
const Database = require("better-sqlite3-multiple-ciphers");
const { SqliteError } = require("better-sqlite3-multiple-ciphers");

// =============================================================================
// 1. TYPE IMPORTS & DEFINITIONS
// =============================================================================

/**
 * @typedef {Object} WorkerData
 * @property {'read' | 'write'} mode - The operation mode.
 * @property {string} filename - Path to the SQLite database file.
 * @property {boolean} [fileMustExist] - If true, throws if the database file does not exist.
 * @property {number} [timeout] - The number of milliseconds to wait when locking the database.
 * @property {string} [nativeBinding] - Path to the native addon executable.
 */

/** @typedef {import('better-sqlite3-multiple-ciphers').Database} DatabaseInstance */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.RunResult} RunResult */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.RegistrationOptions} RegistrationOptions */
/** @typedef {import('better-sqlite3-multiple-ciphers').ColumnDefinition} ColumnDefinition */
/** @typedef {import('better-sqlite3-multiple-ciphers').SqliteError} SqliteError */
/** @typedef {import('better-sqlite3-multiple-ciphers').Statement} Statement */

// --- SHARED UTILITIES ---

/**
 * @typedef {Object} SerializedError
 * @property {string} message
 * @property {string} code
 */

/**
 * @typedef {Object} StatementOptions
 * @property {boolean} [pluck] - Return only the first column.
 * @property {boolean} [raw] - Return raw arrays instead of objects.
 * @property {boolean} [expand] - Expand dot-notation keys.
 */

/**
 * Options for registering a custom aggregate.
 * @typedef {Object} AggregateOptions
 * @property {any} [start] - Initial value.
 * @property {string} [step] - Serialized step function.
 * @property {string} [inverse] - Serialized inverse function.
 * @property {string} [result] - Serialized result function.
 * @property {boolean} [varargs]
 * @property {boolean} [deterministic]
 * @property {boolean} [safeIntegers]
 */

/**
 * All possible action strings.
 * @typedef {'run' | 'get' | 'all' | 'exec' | 'stream_open' | 'stream_next' | 'stream_close' | 'pragma' | 'function' | 'aggregate' | 'key' | 'rekey' | 'load_extension' | 'serialize' | 'close'} WorkerAction
 */

// =============================================================================
// 2. RESPONSE DATA STRUCTURES (The 'data' inside responses)
// =============================================================================

/**
 * @typedef {Object} ResultRead
 * @property {Array<any>} rows
 * @property {ColumnDefinition[]} [columns]
 */

/**
 * @typedef {Object} ResultStream
 * @property {string} streamId
 * @property {Array<any>} rows
 * @property {ColumnDefinition[]} [columns]
 * @property {boolean} done
 */

/**
 * @typedef {Object} ResultPragma
 * @property {unknown} pragma
 */

// =============================================================================
// 3. REQUEST PAYLOADS (The 'data' inside requests)
// =============================================================================

/**
 * @typedef {Object} PayloadRun
 * @property {'run'} action
 * @property {string} sql
 * @property {Array<any>|Object} [params]
 * @property {StatementOptions} [options]
 */

/**
 * @typedef {Object} PayloadRead
 * @property {'get' | 'all'} action
 * @property {string} sql
 * @property {Array<any>|Object} [params]
 * @property {StatementOptions} [options]
 */

/**
 * @typedef {Object} PayloadExec
 * @property {'exec'} action
 * @property {string} sql
 */

/**
 * @typedef {Object} PayloadStreamOpen
 * @property {'stream_open'} action
 * @property {string} streamId
 * @property {string} sql
 * @property {Array<any>|Object} [params]
 * @property {StatementOptions} [options]
 */

/**
 * @typedef {Object} PayloadStreamNext
 * @property {'stream_next'} action
 * @property {string} streamId
 */

/**
 * @typedef {Object} PayloadStreamClose
 * @property {'stream_close'} action
 * @property {string} streamId
 */

/**
 * @typedef {Object} PayloadPragma
 * @property {'pragma'} action
 * @property {string} sql
 * @property {StatementOptions} [options]
 */

/**
 * @typedef {Object} PayloadFunction
 * @property {'function'} action
 * @property {string} fnName
 * @property {string} fnString
 * @property {RegistrationOptions} fnOptions
 */

/**
 * @typedef {Object} PayloadAggregate
 * @property {'aggregate'} action
 * @property {string} aggName
 * @property {AggregateOptions} aggOptions
 */

/**
 * @typedef {Object} PayloadKey
 * @property {'key'} action
 * @property {string|Buffer} key
 */

/**
 * @typedef {Object} PayloadRekey
 * @property {'rekey'} action
 * @property {string|Buffer} key
 */

/**
 * @typedef {Object} PayloadLoadExtension
 * @property {'load_extension'} action
 * @property {string} path
 */

/**
 * @typedef {Object} PayloadSerialize
 * @property {'serialize'} action
 * @property {SerializeOptions} [options]
 */

/**
 * @typedef {Object} PayloadClose
 * @property {'close'} action
 */

/**
 * Union of all Request Payloads.
 * @typedef {PayloadRun | PayloadRead | PayloadExec | PayloadStreamOpen | PayloadStreamNext | PayloadStreamClose | PayloadPragma | PayloadFunction | PayloadAggregate | PayloadKey | PayloadRekey | PayloadLoadExtension | PayloadSerialize | PayloadClose} WorkerRequestPayload
 */

// =============================================================================
// 4. RESPONSE TYPES (Strictly tied to Actions)
// =============================================================================

/**
 * @typedef {Object} ResponseRun
 * @property {string} requestId
 * @property {'run'} action
 * @property {'success'} status
 * @property {RunResult} data
 */

/**
 * @typedef {Object} ResponseRead
 * @property {string} requestId
 * @property {'get' | 'all'} action
 * @property {'success'} status
 * @property {ResultRead} data
 */

/**
 * @typedef {Object} ResponseStream
 * @property {string} requestId
 * @property {'stream_open' | 'stream_next'} action
 * @property {'success'} status
 * @property {ResultStream} data
 */

/**
 * @typedef {Object} ResponsePragma
 * @property {string} requestId
 * @property {'pragma'} action
 * @property {'success'} status
 * @property {ResultPragma} data
 */

/**
 * @typedef {Object} ResponseSerialize
 * @property {string} requestId
 * @property {'serialize'} action
 * @property {'success'} status
 * @property {ResultSerialize} data
 */

/**
 * @typedef {Object} ResponseVoid
 * @property {string} requestId
 * @property {'exec' | 'close' | 'function' | 'aggregate' | 'stream_close' | 'key' | 'rekey' | 'loadExtension'} action
 * @property {'success'} status
 */

/**
 * @typedef {Object} ResponseError
 * @property {string} requestId
 * @property {WorkerAction} action
 * @property {'error'} status
 * @property {SerializedError} error
 */

/**
 * Unified Response Type
 * @typedef {ResponseRun | ResponseRead | ResponseStream | ResponsePragma | ResponseSerialize | ResponseVoid | ResponseError} WorkerResponse
 */

// =============================================================================
// 5. INITIALIZATION
// =============================================================================

/** @type {WorkerData} */
const { filename, fileMustExist, timeout, nativeBinding, mode } = workerData;

// console.log(`[Worker:${mode}] Starting... DB: ${filename}`);

// console.log(`${mode}: `, workerData);

const isWriter = mode === "write";
const isMemory = filename === ":memory:" || filename === "";

/** @type {DatabaseInstance} */
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
// 6. CENTRAL DISPATCHER
// =============================================================================

if (parentPort) {
  parentPort.on("message", handleMessage);
}

/**
 * Routes the request to the specific handler function.
 * @param {WorkerRequest} request
 */
function handleMessage({ requestId, data }) {
  if (!data) return;

  try {
    switch (data.action) {
      case "run": {
        const result = processRun(data);
        /** @type {ResponseRun} */
        const res = {
          requestId,
          action: "run",
          status: "success",
          data: result,
        };
        parentPort.postMessage(res);
        break;
      }
      case "get":
      case "all": {
        const result = processRead(data);
        /** @type {ResponseRead} */
        const res = {
          requestId,
          action: data.action,
          status: "success",
          data: result,
        };
        parentPort.postMessage(res);
        break;
      }
      case "exec": {
        processExec(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "exec", status: "success" };
        parentPort.postMessage(res);
        break;
      }
      case "pragma": {
        const result = processPragma(data);
        /** @type {ResponsePragma} */
        const res = {
          requestId,
          action: "pragma",
          status: "success",
          data: result,
        };
        parentPort.postMessage(res);
        break;
      }
      case "key": {
        processKey(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "key", status: "success" };
        parentPort.postMessage(res);
        break;
      }
      case "rekey": {
        processRekey(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "rekey", status: "success" };
        parentPort.postMessage(res);
        break;
      }
      case "load_extension": {
        processLoadExtension(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "load_extension", status: "success" };
        parentPort.postMessage(res);
        break;
      }
      case "serialize": {
        const result = processSerialize(data);
        /** @type {ResponseSerialize} */
        const res = {
          requestId,
          action: "serialize",
          status: "success",
          data: result,
        };
        parentPort.postMessage(res);
        break;
      }
      case "stream_open": {
        const result = processStreamOpen(data);
        /** @type {ResponseStream} */
        const res = {
          requestId,
          action: "stream_open",
          status: "success",
          data: result,
        };
        parentPort.postMessage(res);
        break;
      }
      case "stream_next": {
        const result = processStreamNext(data);
        /** @type {ResponseStream} */
        const res = {
          requestId,
          action: "stream_next",
          status: "success",
          data: result,
        };
        parentPort.postMessage(res);
        break;
      }
      case "stream_close": {
        processStreamClose(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "stream_close", status: "success" };
        parentPort.postMessage(res);
        break;
      }
      case "function": {
        processFunction(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "function", status: "success" };
        parentPort.postMessage(res);
        break;
      }
      case "aggregate": {
        processAggregate(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "aggregate", status: "success" };
        parentPort.postMessage(res);
        break;
      }
      case "close": {
        processClose();
        /** @type {ResponseVoid} */
        const res = { requestId, action: "close", status: "success" };
        parentPort.postMessage(res);
        break;
      }
      default:
        // @ts-ignore
        throwSqliteError(`Unknown action: ${data.action}`, "SQLITE_MISUSE");
    }
  } catch (err) {
    /** @type {ResponseError} */
    const res = {
      requestId,
      action: data.action,
      status: "error",
      error: formatError(err),
    };
    parentPort.postMessage(res);
  }
}

// =============================================================================
// 7. ACTION HANDLERS
// =============================================================================

/**
 * Handles 'run' requests (INSERT, UPDATE, DELETE).
 * @param {PayloadRun} payload
 * @returns {ResultRun}
 */
function processRun(payload) {
  if (!isWriter) {
    throwSqliteError(
      "Cannot execute 'run' in read-only mode",
      "SQLITE_READONLY",
    );
  }
  const stmt = prepareStatement(payload.sql, payload.options);
  // better-sqlite3 types define .run() returning RunResult directly
  return stmt.run(...(payload.params || []));
}

/**
 * Handles 'get' and 'all' requests (SELECT).
 * @param {PayloadRead} payload
 * @returns {ResultRead}
 */
function processRead(payload) {
  const stmt = prepareStatement(payload.sql, payload.options);

  // @ts-ignore - dynamic access to 'get' or 'all'
  const result = stmt[payload.action](...(payload.params || []));

  const includeColumns = !(payload.options?.raw || payload.options?.pluck);
  const columns = includeColumns ? stmt.columns() : undefined;

  const rows =
    payload.action === "get"
      ? result
        ? [result]
        : []
      : /** @type {Array<any>} */ (result);

  return { rows, columns };
}

/**
 * Handles 'exec' requests (DDL).
 * @param {PayloadExec} payload
 */
function processExec(payload) {
  db.exec(payload.sql);
}

/**
 * Handles 'pragma' requests.
 * @param {PayloadPragma} payload
 * @returns {ResultPragma}
 */
function processPragma(payload) {
  // @ts-ignore - pragma options mapping
  const result = db.pragma(payload.sql, payload.options);
  return { pragma: result };
}

/** @param {PayloadKey} payload */
function processKey(payload) {
  // @ts-ignore
  db.key(payload.key);
}

/** @param {PayloadRekey} payload */
function processRekey(payload) {
  // @ts-ignore
  db.rekey(payload.key);
}

/** @param {PayloadLoadExtension} payload */
function processLoadExtension(payload) {
  db.loadExtension(payload.path);
}

/**
 * @param {PayloadSerialize} payload
 * @returns {ResultSerialize}
 */
function processSerialize(payload) {
  const buffer = db.serialize(payload.options);
  return { buffer };
}
/**
 * Handles 'stream_open'.
 * @param {PayloadStreamOpen} payload
 * @returns {ResultStream}
 */
function processStreamOpen(payload) {
  const stmt = prepareStatement(payload.sql, payload.options);
  const iterator = stmt.iterate(...(payload.params || []));
  activeStreams.set(payload.streamId, iterator);

  const includeColumns = !(payload.options?.raw || payload.options?.pluck);
  const columns = includeColumns ? stmt.columns() : undefined;

  return getStreamBatch(payload.streamId, iterator, columns);
}

/**
 * Handles 'stream_next'.
 * @param {PayloadStreamNext} payload
 * @returns {ResultStream}
 */
function processStreamNext(payload) {
  const iterator = activeStreams.get(payload.streamId);
  if (!iterator) {
    return { streamId: payload.streamId, rows: [], done: true };
  }
  return getStreamBatch(payload.streamId, iterator, undefined);
}

/**
 * Handles 'stream_close'.
 * @param {PayloadStreamClose} payload
 */
function processStreamClose(payload) {
  closeStream(payload.streamId);
}

/**
 * Handles UDF registration.
 * @param {PayloadFunction} payload
 */
function processFunction(payload) {
  const fn = deserializeFunction(payload.fnString);
  if (payload.fnOptions) {
    db.function(payload.fnName, payload.fnOptions, fn);
  } else {
    db.function(payload.fnName, fn);
  }
}

/**
 * Handles Aggregate registration.
 * @param {PayloadAggregate} payload
 */
function processAggregate(payload) {
  const opts = deserializeAggregateOptions(payload.aggOptions);
  db.aggregate(payload.aggName, opts);
}

/**
 * Handles cleanup.
 */
function processClose() {
  cleanupStreams();
  if (db && db.open) {
    db.close();
  }
}

// =============================================================================
// 8. UTILITY FUNCTIONS
// =============================================================================

/**
 * Prepares a statement with options.
 * @param {string} sql
 * @param {StatementOptions} [options]
 * @returns {Statement}
 */
function prepareStatement(sql, options) {
  const stmt = db.prepare(sql);
  if (options) {
    if (options.pluck) stmt.pluck(true);
    if (options.raw) stmt.raw(true);
    if (options.expand) stmt.expand(true);
  }
  return stmt;
}

/**
 * Fetches a batch of rows from an iterator.
 * @param {string} streamId
 * @param {Iterator<unknown>} iterator
 * @param {ColumnDefinition[]} [columns]
 * @returns {ResultStream}
 */
function getStreamBatch(streamId, iterator, columns) {
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
    closeStream(streamId);
    throw err;
  }

  if (done) activeStreams.delete(streamId);

  return { streamId, rows, columns, done };
}

/**
 * @param {string} streamId
 */
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
 * @param {string} fnString
 * @returns {Function}
 */
function deserializeFunction(fnString) {
  return (0, eval)(`(${fnString})`);
}

/**
 * @param {AggregateOptions} opts
 */
function deserializeAggregateOptions(opts) {
  const out = { ...opts };
  if (opts.step) out.step = deserializeFunction(opts.step);
  if (opts.inverse) out.inverse = deserializeFunction(opts.inverse);
  if (opts.result) out.result = deserializeFunction(opts.result);
  return out;
}

/**
 * @param {string} message
 * @param {string} [code]
 */
function throwSqliteError(message, code = "SQLITE_ERROR") {
  throw new SqliteError(message, code);
}

/**
 * @param {Error | SqliteError | any} err
 * @returns {SerializedError}
 */
function formatError(err) {
  const code =
    err.code !== undefined && typeof err.code === "string"
      ? err.code
      : "SQLITE_ERROR";
  return { message: err.message, code };
}
