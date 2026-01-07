/**
 * @file lib/database.js
 * @description Database implementation of better-sqlite3-pool.
 * This module manages a thread pool of SQLite connections, coordinating a single
 * Writer worker (WAL mode) and multiple Reader workers to enable high-concurrency
 * read-heavy workloads without blocking the event loop.
 */

const { Worker } = require("node:worker_threads");
const path = require("node:path");
const EventEmitter = require("events");
const Mutex = require("./mutex");
const Statement = require("./statement");
const { createError, fileExists, parentDirectoryExists } = require("./utils");
const { SqliteError } = require("better-sqlite3-multiple-ciphers");
const { SingleWorkerClient, MultiWorkerClient } = require("./worker-pool");

// =============================================================================
// INTERNAL TYPE DEFINITIONS
// =============================================================================

/**
 * @typedef {Object} DatabaseOptions
 * @property {number} [minWorkers=1] - Minimum number of reader workers (alias: min).
 * @property {number} [maxWorkers=2] - Maximum number of reader workers (alias: max).
 * @property {boolean} [readonly=false] - Open the database in read-only mode.
 * @property {boolean} [fileMustExist=false] - If true, throws if the database file does not exist.
 * @property {number} [timeout=5000] - The number of milliseconds to wait when locking the database.
 * @property {string} [nativeBinding] - Path to the native addon executable.
 * @property {Function} [verbose] - (Not supported in threaded mode) Function to call with execution information.
 */

/**
 * @description Represents a pending operation waiting for a worker response.
 * @typedef {Object} PendingPromise
 * Stored in `writeQueue` and `reader.queue` maps.
 * @property {Function} resolve - callback to resolve the outer Promise.
 * @property {Function} reject - callback to reject the outer Promise.
 */

/**
 * @description A full read task waiting in the global pool queue for an available worker.
 * @typedef {Object} QueuedTask
 * Stored in `this.readQueue` array.
 * @property {'exec' | 'run' | 'all' | 'get' | 'function' | 'stream_open' | 'stream_ack' | 'stream_close'} action - The specific operation to perform.
 * @property {string} sql - The SQL query string.
 * @property {Array<any>} [params] - The bind parameters.
 * @property {Object} [options] - execution options (raw, pluck, etc.).
 * @property {Function} resolve - callback to resolve the outer Promise.
 * @property {Function} reject - callback to reject the outer Promise.
 */

/**
 * @description Represents a Reader Worker in the pool.
 * @typedef {Object} Reader
 * @property {number} id - Unique internal ID for tracking.
 * @property {Worker} worker - The native Node.js Worker thread instance.
 * @property {boolean} busy - True if the worker is currently executing a query.
 * @property {boolean} ready - True if the worker has finished initialization.
 * @property {Map<number, PendingPromise>} queue - Map of message IDs to pending promises.
 * @property {Promise<void>} readyPromise - Resolves when the worker sends the 'ready' signal.
 */

/**
 * @description The structure of messages sent to workers.
 * @typedef {Object} WorkerPayload
 * @property {'run' | 'exec' | 'all' | 'get' | 'function' | 'close' | 'stream_open' | 'stream_ack' | 'stream_close'} action - The operation to perform.
 * @property {string} [sql] - SQL query.
 * @property {string} [fnName] - Function name (for UDFs).
 * @property {string} [fnString] - Function body string (for UDFs).
 * @property {Array<any>} [params] - Query parameters.
 * @property {Object} [options] - Execution options.
 */

/**
 * @description The structure of messages received from workers.
 * @typedef {Object} WorkerResponse
 * @property {number} id - The correlation ID matching the request.
 * @property {'success' | 'error' | 'stream_data'} status - The outcome status.
 * @property {any} [data] - The result payload (on success).SQLITE_CANTOPEN
 * @property {any} [error] - The error payload (on error).
 */

// =============================================================================
// MAIN CLASS
// =============================================================================

/**
 * Internal Symbol to prevent direct constructor usage.
 */
const kInternal = Symbol("DatabaseInternal");

/**
 * @class Database
 * @extends EventEmitter
 * @description The main Database class that acts as a connection pool manager.
 * It abstracts away the complexity of managing worker threads, query routing,
 * and synchronization between the main thread and the worker pool.
 */
class Database extends EventEmitter {
  /**
   * Asynchronously creates and initializes a new Database connection pool.
   * This method spawns workers and waits for the database file to be ready.
   *
   * @param {string} filename - Path to the SQLite database file (or ':memory:').
   * @param {DatabaseOptions} [options] - Configuration options.
   * @returns {Promise<Database>} A fully initialized Database instance.
   */
  static async create(filename, options = {}) {
    if (typeof filename !== "string") {
      throw new TypeError("filename has to be string");
    }
    if (options !== undefined && typeof options !== "object") {
      throw new TypeError("Options has to be object");
    }

    // Pre-flight FS checks
    const parentDirExists = await parentDirectoryExists(filename);
    if (!parentDirExists) {
      throw new SqliteError(
        `${filename} parent directory does not exist`,
        "SQLITE_CANTOPEN",
      );
    }

    const exists = await fileExists(filename);
    options = options === undefined ? {} : options;
    options.exists = exists;

    // Instantiate with internal token
    const db = new this(filename, options, kInternal);

    try {
      await db._init();
      return db;
    } catch (err) {
      await db.close(); // Clean up partial connections
      throw err;
    }
  }

  /**
   * Create a new Database connection pool.
   * @param {string} filename - Path to the SQLite database file (or ':memory:').
   * @param {DatabaseOptions & {exists: boolean}} [options] - Configuration Options
   * @param {Symbol} [token] - Internal token to prevent public usage.
   * @throws {TypeError|RangeError} If options are invalid.
   */
  constructor(filename, options = {}, token) {
    if (token !== kInternal) {
      throw new ReferenceError(
        "Direct constructor usage is not supported. Use 'await Database.create(filename, options)' instead.",
      );
    }
    super();
    this.name = filename;

    // --- Configuration Validation ---
    let minReaders = options.minWorkers !== undefined ? options.minWorkers : 1;
    let maxReaders = options.maxWorkers !== undefined ? options.maxWorkers : 2;

    // Validate Types
    if (!Number.isInteger(minReaders) || minReaders < 0) {
      throw new TypeError("options.minWorkers must be a positive integer");
    }
    if (!Number.isInteger(maxReaders) || maxReaders < 0) {
      throw new TypeError("options.maxWorkers must be a positive integer");
    }
    if (minReaders > maxReaders) {
      throw new RangeError(
        "options.minWorkers cannot be greater than options.maxWorkers",
      );
    }
    if (
      options.readonly !== undefined &&
      typeof options.readonly !== "boolean"
    ) {
      throw new TypeError("options.readonly must be a boolean");
    }

    if (
      options.fileMustExist !== undefined &&
      typeof options.fileMustExist !== "boolean"
    ) {
      throw new TypeError("options.fileMustExist must be a boolean");
    }
    if (
      options.timeout !== undefined &&
      (!Number.isInteger(options.timeout) || options.timeout < 0)
    ) {
      throw new TypeError("options.timeout must be a positive integer");
    } else if (options.timeout > 0x7fffffff) {
      // 2147483647
      throw new RangeError(
        "options.timeout should not be greater than max 32 bit integer",
      );
    }
    if (
      options.nativeBinding !== undefined &&
      typeof options.nativeBinding !== "string"
    ) {
      throw new TypeError("options.nativeBinding must be a string");
    }

    this.readonly = options?.readonly === true;

    /** @type {boolean} - True if the database is strictly in-memory. */
    this.memory = filename === ":memory:" || filename === "";

    // console.log("database: ", {
    //   ...options,
    //   ...{
    //     filename,
    //     readonly: this.readonly,
    //     memory: this.memory,
    //   },
    // });

    if (this.readonly) {
      if (this.memory) {
        throw new TypeError(
          "In memory database cannot be reaSQLITE_CANTOPENdonly",
        );
      }
      if (!options.exists) {
        throw new SqliteError(
          `${filename} should exist for read only mode`,
          "SQLITE_CANTOPEN",
        );
      }
    }

    if (options.fileMustExist === true) {
      if (!options.exists) {
        throw new SqliteError(
          `${filename} should already be exisiting`,
          "SQLITE_CANTOPEN",
        );
      }
    }

    if (this.memory) {
      // In-memory DBs cannot share state across threads easily, so we disable the pool.
      // All queries will be routed to the single "Writer" thread.
      this.minReaders = 0;
      this.maxReaders = 0;
      // this.readonly = false; // Memory DBs must be writable to exist
    } else {
      this.minReaders = minReaders;
      this.maxReaders = maxReaders;
    }

    /** @type {DatabaseOptions} - Configuration Options. */
    this.options = {
      readonly: this.readonly,
      fileMustExist: !!options.fileMustExist,
      timeout: options.timeout,
      nativeBinding: options.nativeBinding,
      minReaders,
      maxReaders,
    };

    /** * Indicates if the database is open and fully initialized.
     * @type {boolean}
     */
    this.open = false;

    // --- WORKER CLIENTS ---

    /** @type {SingleWorkerClient|null} The single Writer thread (handles transactions). */
    this.writer = null;

    /** @type {MultiWorkerClient|null} The pool of Reader threads. */
    this.readerPool = null;

    // --- WORKER CONFIGURATION ---
    // Prepare the payload that goes to every worker
    this._workerConfig = {
      filename: this.name,
      readonly: this.readonly,
      fileMustExist: this.options.fileMustExist,
      timeout: this.options.timeout,
      nativeBinding: this.options.nativeBinding,
    };

    /** @type {boolean} - Internal flag for transaction state tracking. */
    this.inTransaction = false;
  }

  /**
   * Internal Initialization Method.
   * Enforces specific startup order: Writer -> Readers.
   * @private
   */
  async _init() {
    // 1. Initialize Writer (if not readonly)
    // The Writer MUST start first to create the WAL/SHM files.
    if (!this.readonly) {
      this.writer = await SingleWorkerClient.create(
        path.resolve(__dirname, "worker.js"),
        {
          workerData: { ...this._workerConfig, mode: "write" },
          useMutex: true, // <--- CRITICAL: Transactions/Writes must be serialized.
        },
        this._workerInitCheck, // Custom hook to wait for "ready" message
      );
    }

    // 2. Initialize Reader Pool
    // If in-memory, we skip the pool completely.
    if (!this.memory) {
      this.readerPool = await MultiWorkerClient.create(
        path.resolve(__dirname, "worker.js"),
        this.minReaders,
        this.maxReaders,
        {
          workerData: { ...this._workerConfig, mode: "read" },
          useMutex: false, // <--- CRITICAL: Readers must be parallel. No Mutex.
        },
        this._workerInitCheck, // Custom hook to wait for "ready" message per worker
      );
    }

    this.open = true;
    this.emit("open");
  }

  /**
   * Initialization hook for Worker Clients.
   * Waits for the SQLite worker script to send { status: 'ready' }.
   * This ensures the Promise returned by create() doesn't resolve until the DB is truly usable.
   * @param {SingleWorkerClient} client
   */
  async _workerInitCheck(client) {
    return new Promise((resolve, reject) => {
      const handler = (msg) => {
        if (msg.status === "ready") {
          client.worker.off("message", handler);
          resolve();
        } else if (msg.status === "error") {
          client.worker.off("message", handler);
          reject(createError(msg.error));
        }
      };

      client.worker.on("message", handler);

      // Failsafe timeout
      setTimeout(() => {
        client.worker.off("message", handler);
        reject(new Error("Worker initialization timed out (10s)"));
      }, 10000);
    });
  }

  /**
   * Creates a new prepared statement.
   * @param {string} sql - The SQL query string.
   * @returns {Statement} A wrapped Statement instance.
   */
  prepare(sql) {
    return new Statement(this, sql);
  }

  /**
   * Internal Helper: Waits for the database infrastructure to be ready.
   * This includes the Writer (DB file creation) and any Readers currently booting.
   * @returns {Promise<void>}
   * @private
   */
  async _waitForInitialization() {
    try {
      // 1. Wait for Writer (Critical: File existence)
      // In readonly, this resolves immediately.
      if (!this.writerReady) {
        await this.writerReadyPromise;
      }

      // 2. Wait for any booting Readers (Consistency: Ensure they receive broadcasts)
      const pendingReaders = this.readers
        .filter((r) => !r.ready && r.readyPromise)
        .map((r) => r.readyPromise);

      if (pendingReaders.length > 0) {
        await Promise.all(pendingReaders);
      }
      this.open = true;
      this.emit("open");
    } catch (err) {
      this.open = false;
      this.emit("error", err);
      // Ensure cleanup if init failed
      await this.close();
    }
  }

  /**
   * Returns a promise that resolves when the database pool is fully initialized.
   * @returns {Promise<void>}
   */
  ready() {
    return this._waitForInitialization();
  }

  /**
   * Helper to ensure DB is open before queuing work.
   * @throws {TypeError}
   */
  _ensureOpen() {
    if (!this.open) {
      throw new TypeError("The database connection is not open");
    }
  }

  /**
   * Register a User Defined Function (UDF).
   * Broadcasts the function to the Writer and all Readers.
   * Waits for acknowledgement from all workers to ensure consistency.
   * @param {string} name - The name of the SQL function.
   * @param {Function} fn - The JavaScript function to execute.
   * @returns {Promise<this>} The Database instance.
   */
  async function(name, fn) {
    this._ensureOpen();
    if (typeof name !== "string")
      throw new TypeError("Expected first argument to be a string");
    if (typeof fn !== "function")
      throw new TypeError("Expected second argument to be a function");

    const fnString = fn.toString();
    this._initFunctions.push({ name, fnString });
    const payload = { action: "function", fnName: name, fnString };

    const promises = [];

    // 1. Send to Reader Pool (via broadcast with sticky=true)
    if (this.readerPool) {
      promises.push(this.readerPool.broadcast(payload, true));
    }

    // 2. Send to Writer
    if (this.writer) {
      promises.push(this.writer.execute(payload));
    }

    await Promise.all(promises);
    return this;
  }

  /**
   * Execute a PRAGMA statement.
   * Broadcasts the setting to the Writer and all Readers.
   * Waits for acknowledgement from all workers.
   * @param {string} sql - The PRAGMA statement (e.g., "journal_mode = WAL").
   * @param {Object} [options] - Options.
   * @param {boolean} [options.simple] - If true, returns the first value of the first row.
   * @returns {Promise<void>} The result of the PRAGMA execution.
   */
  async pragma(sql, options = {}) {
    this._ensureOpen();

    // Use specific 'pragma' action so worker uses db.pragma() instead of db.exec()
    const payload = { action: "pragma", sql, options };

    // READONLY MODE
    if (this.readonly) {
      if (this.readerPool) {
        // Sticky broadcast ensuring new readers get this pragma
        const results = await this.readerPool.broadcast(payload, true);
        // Return the result from the first worker (they should all be identical)
        return results[0].pragma;
      }
      return;
    }

    // WRITE MODE
    // 1. Execute on Writer (Primary) - This returns the actual pragma result
    const writerRes = await this.writer.execute(payload);

    // 2. Broadcast to Readers (Sticky) to keep them in sync with Writer settings
    if (this.readerPool) {
      await this.readerPool.broadcast(payload, true);
    }

    return writerRes.pragma;
  }

  /**
   * Execute a simple SQL query (no result retrieval).
   * Useful for DDL statements (CREATE TABLE, DROP TABLE, etc.).
   * @param {string} sql - The SQL statement.
   * @returns {Promise<void>}
   */
  async exec(sql) {
    this._ensureOpen();
    return this.prepare(sql).run();
  }

  /**
   * Dynamically resize the reader worker pool.
   * This is an async operation that waits for new workers to fully initialize.
   * @param {number} min - New minimum number of readers.
   * @param {number} max - New maximum number of readers.
   * @returns {Promise<void>}
   */
  async pool(min, max) {
    this._ensureOpen();
    if (this.memory || !this.readerPool) return;

    // Validate inputs
    if (!Number.isInteger(min) || min < 0)
      throw new TypeError("min must be a positive integer");
    if (!Number.isInteger(max) || max < 0)
      throw new TypeError("max must be a positive integer");
    if (min > max) throw new RangeError("min cannot be greater than max");

    // Validate scaling direction (Currently only supports scaling up)
    if (min < this.minReaders)
      throw new RangeError(
        `New min (${min}) cannot be smaller than current min (${this.minReaders})`,
      );
    if (max < this.maxReaders)
      throw new RangeError(
        `New max (${max}) cannot be smaller than current max (${this.maxReaders})`,
      );

    // Delegate to MultiWorkerClient implementation
    await this.readerPool.resize(min, max);

    this.minReaders = min;
    this.maxReaders = max;
  }

  /**
   * Close the database connection pool.
   * Sends a graceful close signal to workers to allow SQLite to checkpoint WAL,
   * then terminates the threads.
   * @returns {Promise<this>}
   */
  async close() {
    if (!this.open) {
      return this;
    }

    const closePayload = { action: "close" };

    // 1. Graceful Close (Send 'close' message to allow WAL checkpointing)
    try {
      const promises = [];
      if (this.readerPool) {
        await this.readerPool.broadcast(closePayload);
      }
      if (this.writer) {
        await this.writer.execute(closePayload);
      }

      await Promise.race([
        Promise.all(promises),
        new Promise((r) => setTimeout(r, 1000)), // 1s timeout
      ]);
    } catch (e) {
      // Ignore errors during close (worker might be dead already)
    }

    // 2. Hard Termination
    if (this.readerPool) {
      await this.readerPool.close();
    }
    if (this.writer) {
      await this.writer.terminate();
    }

    this.writer = null;
    this.readerPool = null;
    this.open = false;
    this.emit("close");

    return this;
  }

  // ===========================================================================
  // INTERNAL ENGINE METHODS (Private)
  // ===========================================================================

  /**
   * Helper to broadcast a payload to ALL workers (Writer + Readers).
   * @param {Object} payload
   * @returns {Promise<void>}
   * @private
   */
  async _broadcast(payload) {
    const readPromises = this.readers.map((r) => this._postReader(r, payload));
    if (this.writer) {
      const writePromise = this._postWriter(payload);
      await Promise.all([writePromise, ...readPromises]);
    } else {
      await Promise.all(readPromises);
    }
  }

  /**
   * Routes a Write request (INSERT/UPDATE/DELETE/PRAGMA).
   * @param {string} action - The action type (e.g., 'run', 'exec').
   * @param {string|Object} sqlOrPayload - The SQL or statement payload.
   * @param {Array} params - Query parameters.
   * @returns {Promise<any>}
   * @private
   */
  async _requestWrite(action, sqlOrPayload, params) {
    this._ensureOpen();
    if (this.readonly || !this.writer) {
      throw new Error("Cannot execute write operation in readonly mode");
    }

    const isObj = typeof sqlOrPayload === "object" && sqlOrPayload !== null;
    const sql = isObj ? sqlOrPayload.sql : sqlOrPayload;
    const options = isObj ? sqlOrPayload.options : undefined;
    const p = isObj ? sqlOrPayload.params : params;

    // Delegate to generic helper
    return this.writer.execute({ action, sql, params: p, options });
  }

  /**
   * Routes a Read request (SELECT).
   * @param {'all'|'get'} action - The action type (e.g., 'all', 'get').
   * @param {string|Object} sqlOrPayload - The SQL or statement payload.
   * @param {Array} params - Query parameters.
   * @returns {Promise<any>}
   * @private
   */
  async _requestRead(action, sqlOrPayload, params) {
    this._ensureOpen();
    // In-Memory DBs or Single-Threaded mode fallback to Writer
    if (this.memory || (!this.readerPool && this.writer)) {
      return this._requestWrite(action, sqlOrPayload, params);
    }

    if (!this.readerPool) {
      throw new TypeError("No available workers for read operation");
    }

    const isObj = typeof sqlOrPayload === "object" && sqlOrPayload !== null;
    const sql = isObj ? sqlOrPayload.sql : sqlOrPayload;
    const options = isObj ? sqlOrPayload.options : undefined;
    const p = isObj ? sqlOrPayload.params : params;

    // Load Balanced Execution (No Mutex)
    return this.readerPool.execute({ action, sql, params: p, options });
  }
}

module.exports = { Database };
