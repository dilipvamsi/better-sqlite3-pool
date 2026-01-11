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
 * @property {boolean} [verbose] - If true, logs are sent.
 */

/** @typedef {import('better-sqlite3-multiple-ciphers').Database} DatabaseInstance */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.RunResult} RunResult */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.RegistrationOptions} RegistrationOptions */
/** @typedef {import('better-sqlite3-multiple-ciphers').ColumnDefinition} ColumnDefinition */
/** @typedef {import('better-sqlite3-multiple-ciphers').SqliteError} SqliteError */
/** @typedef {import('better-sqlite3-multiple-ciphers').Statement} Statement */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.SerializeOptions} SerializeOptions */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.BackupOptions} BackupOptions */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.BackupMetadata} BackupMetadata */

// --- SHARED UTILITIES ---

/**
 * @typedef {Object} SerializedError
 * @property {string} message
 * @property {string} code
 * @property {string} name
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
 * @typedef {'run' | 'get' | 'all' | 'exec' | 'iterator_open' | 'iterator_next' | 'iterator_close' | 'pragma' | 'function' | 'aggregate' | 'key' | 'rekey' | 'load_extension' | 'serialize' | 'close'} WorkerAction
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
 * @typedef {Object} ResultIterator
 * @property {string} iteratorId
 * @property {Array<any>} rows
 * @property {ColumnDefinition[]} [columns]
 * @property {boolean} done
 */

/**
 * @typedef {Object} ResultPragma
 * @property {unknown} pragma
 */

/**
 * @typedef {Object} ResultSerialize
 * @property {Buffer} buffer
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
 * @typedef {Object} PayloadIteratorOpen
 * @property {'iterator_open'} action
 * @property {string} iteratorId
 * @property {string} sql
 * @property {Array<any>|Object} [params]
 * @property {StatementOptions} [options]
 */

/**
 * @typedef {Object} PayloadIteratorNext
 * @property {'iterator_next'} action
 * @property {string} iteratorId
 */

/**
 * @typedef {Object} PayloadIteratorClose
 * @property {'iterator_close'} action
 * @property {string} iteratorId
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
 * @typedef {Object} Payloadload_extension
 * @property {'load_extension'} action
 * @property {string} path
 */

/**
 * @typedef {Object} PayloadSerialize
 * @property {'serialize'} action
 * @property {SerializeOptions} [options]
 */

/**
 * @typedef {Object} PayloadBackup
 * @property {'backup'} action
 * @property {string} filename
 * @property {BackupOptions} [options]
 */

/**
 * @typedef {Object} PayloadTable
 * @property {'table'} action
 * @property {string} name
 * @property {string} factoryString
 */

/**
 * @typedef {Object} PayloadUnsafeMode
 * @property {'unsafe_mode'} action
 * @property {boolean} on
 */

/**
 * @typedef {Object} PayloadClose
 * @property {'close'} action
 */

/**
 * @typedef {Object} PayloadDefaultSafeIntegers
 * @property {'default_safe_integers'} action
 * @property {boolean} state
 */

/**
 * Union of all Request Payloads.
 * @typedef {PayloadRun | PayloadRead | PayloadExec | PayloadIteratorOpen | PayloadIteratorNext | PayloadIteratorClose | PayloadPragma | PayloadFunction | PayloadAggregate | PayloadKey | PayloadRekey | Payloadload_extension | PayloadSerialize | PayloadBackup | PayloadTable | PayloadUnsafeMode | PayloadClose | PayloadDefaultSafeIntegers} WorkerRequestPayload
 */

/**
 * @typedef {Object} WorkerRequest
 * @property {string} requestId
 * @property {WorkerRequestPayload} data
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
 * @typedef {Object} ResponseIterator
 * @property {string} requestId
 * @property {'iterator_open' | 'iterator_next'} action
 * @property {'success'} status
 * @property {ResultIterator} data
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
 * @typedef {Object} ResponseBackup
 * @property {string} requestId
 * @property {'backup'} action
 * @property {'done'} status
 * @property {ResultBackup} data
 */

/**
 * @typedef {Object} ResponseBackupProgress
 * @property {string} requestId
 * @property {'backup'} action
 * @property {'next'} status
 * @property {BackupMetadata} data
 */

/**
 * @typedef {Object} ResponseVoid
 * @property {string} requestId
 * @property {'exec' | 'close' | 'function' | 'aggregate' | 'iterator_close' | 'key' | 'rekey' | 'load_extension'} action
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
 * @typedef {Object} ResponseProgress
 * @description Generic streaming progress response.
 * @property {string} requestId
 * @property {string} action
 * @property {'next'} status
 * @property {any} data
 */

/**
 * Unified Response Type
 * @typedef {ResponseRun | ResponseRead | ResponseIterator | ResponsePragma | ResponseSerialize | ResponseVoid | ResponseError | ResponseBackup| ResponseBackupProgress} WorkerResponse
 */

// =============================================================================
// 5. INITIALIZATION
// =============================================================================

/** @type {WorkerData} */
const { filename, fileMustExist, timeout, nativeBinding, mode, verbose } =
  workerData;

// console.log(`[Worker:${mode}] Starting... DB: ${filename}`);

// console.log(`${mode}: `, workerData);

const isWriter = mode === "write";
const isMemory = filename === ":memory:" || filename === "";

/** @type {DatabaseInstance} */
let db;

const options = {};

if (verbose) {
  options.verbose = (message) => {
    // Send log back to parent with correlation ID
    parentPort.postMessage({
      requestId: currentRequestId,
      status: "log",
      data: message,
    });
  };
}
// 1. Configure Mode-Specific Options
if (!isWriter) {
  // Readers must strictly be Read-Only
  options.readonly = true;
}

if (fileMustExist) options.fileMustExist = true;
if (nativeBinding) options.nativeBinding = nativeBinding;

// Default timeout prevents immediate failures if DB is locked by another process
options.timeout = timeout !== undefined ? timeout : 5000;

try {
  db = new Database(filename, options);
} catch (err) {
  // Report initialization error to pool
  if (parentPort) {
    parentPort.postMessage({
      status: "boot_error",
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
      status: "setup_error",
      error: formatError(err),
    });
  }
}

// Enable BigInt support for large integers
// db.defaultSafeIntegers(true);

if (parentPort) {
  // console.log(`${mode}: is ready`);
  parentPort.postMessage({ status: "ready" });
}

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

function send(response) {
  if (parentPort) {
    // Capture the ACTUAL physical transaction state from SQLite
    if (db && typeof db.inTransaction === "boolean") {
      response.inTransaction = db.inTransaction;
    }
    parentPort.postMessage(response);
  }
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
        send(res);
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
        send(res);
        break;
      }
      case "exec": {
        processExec(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "exec", status: "success" };
        send(res);
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
        send(res);
        break;
      }
      case "key": {
        processKey(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "key", status: "success" };
        send(res);
        break;
      }
      case "rekey": {
        processRekey(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "rekey", status: "success" };
        send(res);
        break;
      }
      case "load_extension": {
        processload_extension(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "load_extension", status: "success" };
        send(res);
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
        send(res);
        break;
      }
      case "backup": {
        // Streamed response handled inside
        processBackup(requestId, data);
        break;
      }
      case "table": {
        processTable(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "table", status: "success" };
        send(res);
        break;
      }
      case "unsafe_mode": {
        processUnsafeMode(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "unsafe_mode", status: "success" };
        send(res);
        break;
      }
      case "default_safe_integers": {
        processDefaultSafeIntegers(data);
        /** @type {ResponseVoid} */
        const res = {
          requestId,
          action: "default_safe_integers",
          status: "success",
        };
        send(res);
        break;
      }
      case "iterator_open": {
        const result = processIteratorOpen(data);
        /** @type {ResponseIterator} */
        const res = {
          requestId,
          action: "iterator_open",
          status: "success",
          data: result,
        };
        send(res);
        break;
      }
      case "iterator_next": {
        const result = processIteratorNext(data);
        /** @type {ResponseIterator} */
        const res = {
          requestId,
          action: "iterator_next",
          status: "success",
          data: result,
        };
        send(res);
        break;
      }
      case "iterator_close": {
        processIteratorClose(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "iterator_close", status: "success" };
        send(res);
        break;
      }
      case "function": {
        processFunction(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "function", status: "success" };
        send(res);
        break;
      }
      case "aggregate": {
        processAggregate(data);
        /** @type {ResponseVoid} */
        const res = { requestId, action: "aggregate", status: "success" };
        send(res);
        break;
      }
      case "close": {
        processClose();
        /** @type {ResponseVoid} */
        const res = { requestId, action: "close", status: "success" };
        send(res);
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
    // console.log(data);
    // console.log(err);
    send(res);
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

/** @param {Payloadload_extension} payload */
function processload_extension(payload) {
  db.loadExtension(payload.path);
}

/** @param {PayloadDefaultSafeIntegers} payload */
function processDefaultSafeIntegers(payload) {
  db.defaultSafeIntegers(payload.state);
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
 * Handles backup. Note: This sends its own messages.
 * @param {string} requestId
 * @param {PayloadBackup} payload
 */
async function processBackup(requestId, payload) {
  try {
    const { filename, options } = payload;
    const backupOpts = { ...options };

    // Intercept progress callback to stream updates
    backupOpts.progress = (info) => {
      /** @type {ResponseBackupProgress} */
      const progressMsg = {
        requestId,
        action: "backup",
        status: "next",
        data: info,
      };
      parentPort.postMessage(progressMsg);

      // Throttle backup to allow event loop to breathe
      return 50;
    };

    const result = await db.backup(filename, backupOpts);

    /** @type {ResponseBackup} */
    const res = {
      requestId,
      action: "backup",
      status: "done",
      data: result,
    };
    parentPort.postMessage(res);
  } catch (err) {
    /** @type {ResponseError} */
    const res = {
      requestId,
      action: "backup",
      status: "error",
      error: formatError(err),
    };
    parentPort.postMessage(res);
  }
}

/** @param {PayloadTable} payload */
function processTable(payload) {
  const factory = deserializeFunction(payload.factoryString);
  // Execute factory to get table options
  const tableOpts = factory();
  db.table(payload.name, tableOpts);
}

/** @param {PayloadUnsafeMode} payload */
function processUnsafeMode(payload) {
  db.unsafeMode(payload.on);
}

/**
 * Handles 'iterator_open'.
 * @param {PayloadIteratorOpen} payload
 * @returns {ResultIterator}
 */
function processIteratorOpen(payload) {
  const stmt = prepareStatement(payload.sql, payload.options);
  const iterator = stmt.iterate(...(payload.params || []));
  activeStreams.set(payload.iteratorId, iterator);

  const includeColumns = !(payload.options?.raw || payload.options?.pluck);
  const columns = includeColumns ? stmt.columns() : undefined;

  return getStreamBatch(payload.iteratorId, iterator, columns);
}

/**
 * Handles 'iterator_next'.
 * @param {PayloadIteratorNext} payload
 * @returns {ResultIterator}
 */
function processIteratorNext(payload) {
  const iterator = activeStreams.get(payload.iteratorId);
  if (!iterator) {
    return { iteratorId: payload.iteratorId, rows: [], done: true };
  }
  return getStreamBatch(payload.iteratorId, iterator, undefined);
}

/**
 * Handles 'iterator_close'.
 * @param {PayloadIteratorClose} payload
 */
function processIteratorClose(payload) {
  closeIterator(payload.iteratorId);
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
 * @param {string} iteratorId
 * @param {Iterator<unknown>} iterator
 * @param {ColumnDefinition[]} [columns]
 * @returns {ResultIterator}
 */
function getStreamBatch(iteratorId, iterator, columns) {
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
    closeIterator(iteratorId);
    throw err;
  }

  if (done) activeStreams.delete(iteratorId);

  return { iteratorId, rows, columns, done };
}

/**
 * @param {string} iteratorId
 */
function closeIterator(iteratorId) {
  const iterator = activeStreams.get(iteratorId);
  if (iterator && iterator.return) {
    try {
      iterator.return();
    } catch (e) {
      /* ignore */
    }
  }
  activeStreams.delete(iteratorId);
}

function cleanupStreams() {
  for (const iteratorId of activeStreams.keys()) {
    closeIterator(iteratorId);
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
  return { message: err.message, code, name: err.name };
}
