/**
 * @file lib/database.js
 * @description Database implementation of better-sqlite3-pool.
 * This module manages a thread pool of SQLite connections, coordinating a single
 * Writer worker (WAL mode) and multiple Reader workers to enable high-concurrency
 * read-heavy workloads without blocking the event loop.
 */

const { AsyncLocalStorage } = require("node:async_hooks");
const { SqliteError } = require("./utils");
const path = require("node:path");
const EventEmitter = require("events");
const Statement = require("./statement");
const {
  fileExists,
  parentDirectoryExists,
  serializeAggregateOptions,
} = require("./utils");
const { SingleWorkerClient, MultiWorkerClient } = require("./worker-pool");
const Connection = require("./connection");

// =============================================================================
// INTERNAL TYPE DEFINITIONS
// =============================================================================

/** @typedef {import('better-sqlite3-multiple-ciphers').Database.Options} NativeOptions */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.RegistrationOptions} RegistrationOptions */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.AggregateOptions} AggregateOptions */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.SerializeOptions} SerializeOptions */

/**
 * @typedef {Object} DatabaseOptions
 * @property {number} [minWorkers=1] - Minimum number of reader workers.
 * @property {number} [maxWorkers=2] - Maximum number of reader workers.
 * @property {boolean} [readonly=false] - Open the database in read-only mode.
 * @property {boolean} [fileMustExist=false] - If true, throws if the database file does not exist.
 * @property {number} [timeout=5000] - The number of milliseconds to wait when locking the database.
 * @property {number} [transactionTimeout=30000] - Max duration (ms) for an idle transaction before auto-rollback.
 * @property {number} [connectionIdleTimeout=5000] - Max duration (ms) of inactivity for a manual connection.
 * @property {number} [connectionMaxLife=60000] - Max total duration (ms) for a manual connection/transaction.
 * @property {string} [nativeBinding] - Path to the native addon executable.
 * @property {Function} [verbose] - (Verbose logging function).
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

    // --- TIMEOUT CONFIGURATION ---
    if (
      options.transactionTimeout !== undefined &&
      (!Number.isInteger(options.transactionTimeout) ||
        options.transactionTimeout < 0)
    ) {
      throw new TypeError(
        "options.transactionTimeout must be a positive integer",
      );
    }

    if (
      options.connectionIdleTimeout !== undefined &&
      (!Number.isInteger(options.connectionIdleTimeout) ||
        options.connectionIdleTimeout < 0)
    ) {
      throw new TypeError(
        "options.connectionIdleTimeout must be a positive integer",
      );
    }

    if (
      options.connectionMaxLife !== undefined &&
      (!Number.isInteger(options.connectionMaxLife) ||
        options.connectionMaxLife < 0)
    ) {
      throw new TypeError(
        "options.connectionMaxLife must be a positive integer",
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

    // Store Connection Limits for use in acquire()
    // Defaults: Max Life = 60s, Idle = 5s
    this.connectionMaxLife = options.connectionMaxLife || 60000;
    this.connectionIdleTimeout = options.connectionIdleTimeout || 5000;

    if (this.readonly) {
      if (this.memory) {
        throw new TypeError("In memory database cannot be readonly");
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
      transactionTimeout: options.transactionTimeout, // passed to worker
      nativeBinding: options.nativeBinding,
      minReaders,
      maxReaders,
      verbose: options.verbose,
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
    /** @type {WorkerConfig} */
    this._workerConfig = {
      filename: this.name,
      readonly: this.readonly,
      fileMustExist: this.options.fileMustExist,
      timeout: this.options.timeout,
      transactionTimeout: this.options.transactionTimeout, // Pass to worker for heartbeat
      nativeBinding: this.options.nativeBinding,
      verbose: !!this.options.verbose,
    };

    if (
      options.verbose !== undefined &&
      options.verbose !== null &&
      typeof options.verbose !== "function"
    ) {
      throw new TypeError("Expected 'verbose' option to be a function");
    }

    /** @type {Function|undefined} */
    // this.verbose =
    //   typeof options.verbose === "function" ? options.verbose : undefined;
    this.verbose =
      typeof options.verbose === "function"
        ? options.verbose.bind(this)
        : undefined;

    /** @type {AsyncLocalStorage} - Stores the async context of the transaction. */
    this.transactionContext = new AsyncLocalStorage();

    this._spId = 0;
  }

  /**
   * Getter to check if the CURRENT async execution context is inside a transaction.
   * Updated to reflect the actual physical state of the Writer worker.
   * @returns {boolean}
   */
  get inTransaction() {
    // Returns true if the Writer worker reports it is inside a transaction.
    // Falls back to false if writer is not initialized (e.g. readonly mode or booting).
    return this.writer ? this.writer.inTransaction : false;
  }

  /**
   * Internal Initialization Method.
   * Enforces specific startup order: Writer -> Readers.
   * @private
   */
  async _init() {
    // Define the log handler that routes worker logs to the user's function
    const onLog = this.verbose
      ? (msg) => {
          if (this.verbose) this.verbose(msg);
        }
      : undefined;

    // 1. Initialize Writer (if not readonly)
    // The Writer MUST start first to create the WAL/SHM files.
    if (!this.readonly) {
      this.writer = await SingleWorkerClient.create(
        path.resolve(__dirname, "worker.js"),
        {
          workerData: { ...this._workerConfig, mode: "write" },
          useMutex: true, // <--- CRITICAL: Transactions/Writes must be serialized.
          onLog,
        },
        null, // Custom hook to wait for "ready" message
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
          onLog,
        },
        null, // Custom hook to wait for "ready" message per worker
      );
    }

    this.open = true;
    // console.log("Database is ready");
    this.emit("open");
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
   * ACQUIRE WRITE LOCK
   * Returns a Connection object that holds the exclusive lock on the writer.
   * The caller MUST ensure `conn.release()` is called (or use `await using`).
   * @returns {Promise<Connection>}
   */
  async acquire() {
    this._ensureOpen();
    if (this.readonly) {
      throw new SqliteError(
        "Cannot acquire write connection in readonly mode",
        "SQLITE_READONLY",
      );
    }

    // Return the session wrapper
    return Connection.create(
      this,
      this.connectionMaxLife,
      this.connectionIdleTimeout,
    );
  }

  /**
   * Creates a new prepared statement.
   * @param {string} sql - The SQL query string.
   * @param {Object} [options] - Configuration for the statement (e.g. { readonly: true/false }).
   * @returns {Statement} A wrapped Statement instance.
   */
  prepare(sql, options) {
    this._ensureOpen();

    if (typeof sql !== "string") {
      throw new TypeError("SQL statement must be a string");
    }

    const trimmedSql = sql.trim();

    if (trimmedSql.length === 0) {
      throw new RangeError("SQL statement cannot be empty");
    } else if (trimmedSql.startsWith(";")) {
      throw new RangeError("SQL statement cannot start with a semicolon");
    }

    // Pass options (if any) to Statement constructor to allow override of Read-Only routing
    return new Statement(this, sql, options);
  }

  /**
   * Execute a function within a transaction.
   * Supports nested transactions via SAVEPOINT.
   *
   * Returns a wrapper function that, when executed, spins up the transaction.
   * The wrapper also has properties (.immediate, .exclusive) to change locking behavior.
   *
   * @param {Function} fn - Async function to execute.
   * @returns {Function} - A wrapper function.
   */
  transaction(fn) {
    if (typeof fn !== "function") throw new TypeError("Expected a function");
    this._ensureOpen();

    // Helper to generate the specific wrapper based on locking behavior
    const createWrapper = (behavior) => {
      return async (...args) => {
        // 1. Check for Active Context (Nested Transaction)
        // If we are already inside a transaction, we reuse that connection
        // and use SAVEPOINTs. The 'behavior' arg is ignored for nested calls.
        const parentConn = this.transactionContext.getStore();

        if (parentConn) {
          const savepointName = `sp_${this._spId++}`;
          try {
            await parentConn.exec(`SAVEPOINT ${savepointName}`);
            const result = await fn(...args);
            await parentConn.exec(`RELEASE ${savepointName}`);
            return result;
          } catch (err) {
            try {
              await parentConn.exec(`ROLLBACK TO ${savepointName}`);
            } catch (e) {
              /* ignore rollback errors */
            }
            throw err;
          }
        }

        // 2. Root Transaction (New Connection)
        // We acquire a new exclusive connection from the pool.
        const conn = await this.acquire();

        // Wrap execution in AsyncLocalStorage context so internal queries find 'conn'
        return this.transactionContext.run(conn, async () => {
          try {
            // Start the transaction with the specific locking behavior
            // (DEFERRED, IMMEDIATE, or EXCLUSIVE)
            await conn.exec(`BEGIN ${behavior}`);

            const result = await fn(...args);

            await conn.exec("COMMIT");
            return result;
          } catch (err) {
            try {
              await conn.exec("ROLLBACK");
            } catch (e) {
              // FIX: Ignore "no transaction" errors.
              // This happens if the transaction was killed internally (e.g. INSERT OR ROLLBACK)
              if (!e.message.includes("no transaction is active")) {
                console.error(
                  "[better-sqlite3-pool] Rollback failed:",
                  e.message,
                );
              }
            }
            throw err;
          } finally {
            // Always release the connection lock
            conn.release();
          }
        });
      };
    };

    // 1. Create distinct function instances for each behavior
    const deferred = createWrapper("DEFERRED");
    const immediate = createWrapper("IMMEDIATE");
    const exclusive = createWrapper("EXCLUSIVE");

    // 2. Define the API structure (Circular references)
    // Standard better-sqlite3 uses 'deferred' as the default.
    const properties = {
      default: deferred,
      deferred: deferred,
      immediate: immediate,
      exclusive: exclusive,
    };

    // 3. Attach properties to ALL wrappers so they are interchangeable
    for (const wrapper of [deferred, immediate, exclusive]) {
      Object.assign(wrapper, properties);

      // Attach .database property to satisfy API compatibility
      Object.defineProperty(wrapper, "database", {
        value: this,
        enumerable: true,
        configurable: true,
      });
    }

    return deferred;
  }

  /**
   * Helper to broadcast configuration (Pragmas, Functions, Keys) to all workers.
   * Ensures settings persist across worker restarts (Sticky).
   *
   * @param {Object} payload - The data payload.
   */
  async _execConfig(payload) {
    this._ensureOpen();
    const promises = [];

    // 1. Broadcast to Reader Pool (if exists)
    // We use sticky=true so new readers get this config on spawn
    if (this.readerPool) {
      promises.push(this.readerPool.broadcast(payload, true));
    }

    // 2. Apply to Writer
    if (this.writer) {
      const activeConn = this.transactionContext.getStore();

      // CASE A: Inside a Transaction
      // We cannot acquire the lock (we already hold it).
      // We cannot use 'sticky' execution (modifying history mid-transaction is risky).
      // We just execute it ephemerally.
      if (activeConn) {
        promises.push(this.writer.noLockExecute(payload, true));
      }
      // CASE B: Standard Config Change
      // Execute, and mark as STICKY so it replays if writer crashes.
      else {
        promises.push(this.writer.execute(payload, true));
      }
    }

    await Promise.all(promises);
  }

  // =================================================================
  // ENCRYPTION SUPPORT (Critical for multiple-ciphers)
  // =================================================================

  /**
   * Set the encryption key for the database.
   * Must be called immediately after creation.
   * @param {string|Buffer} key
   */
  async key(key) {
    if (!Buffer.isBuffer(key) && typeof key !== "string") {
      throw new TypeError("Expected first argument to be a Buffer or String");
    }
    const payload = { action: "key", key };
    await this._execConfig(payload);
  }

  /**
   * Change the encryption key.
   * @param {string|Buffer} key
   */
  async rekey(key) {
    if (!Buffer.isBuffer(key) && typeof key !== "string") {
      throw new TypeError("Expected first argument to be a Buffer or String");
    }
    // 1. Writer: Executes 'rekey' (rewrites database)
    if (this.writer) {
      await this.writer.execute({ action: "rekey", key });
      // On the writer restart, we need key for the writer
      await this.writer.execute({ action: "key", key }, true);
    }

    // 2. Readers: Execute 'key' (update internal handle to read new format)
    // Readers cannot 'rekey' (write), so we just give them the new key.
    if (this.readerPool) {
      await this.readerPool.broadcast({ action: "key", key }, true);
    }
  }

  /**
   * Toggle default BigInt support for the database.
   * Broadcasts the setting to all workers.
   * @param {boolean} [toggleState=true]
   */
  async defaultSafeIntegers(toggleState = true) {
    const payload = { action: "default_safe_integers", state: toggleState };
    await this._execConfig(payload);
  }

  /**
   * Loads a compiled SQLite extension.
   * @param {string} path
   */
  async loadExtension(path) {
    if (typeof path !== "string") {
      throw new TypeError("Expected first argument to be a String");
    }
    const payload = { action: "load_extension", path };
    await this._execConfig(payload);
  }

  /** @typedef {import('better-sqlite3-multiple-ciphers').Database.RegistrationOptions} RegistrationOptions */

  /**
   * Register a User Defined Function (UDF).
   * Broadcasts the function to the Writer and all Readers.
   * Waits for acknowledgement from all workers to ensure consistency.
   * @param {string} name - The name of the SQL function.
   * @param {Function | RegistrationOptions} options - Function Registration Options.
   * @param {Function} [fn] - The JavaScript function to execute.
   * @returns {Promise<this>} The Database instance.
   */
  async function(name, options, fn) {
    // Argument shuffling to support optional 'options'
    let callback = fn;
    let opts = options;

    if (typeof options === "function") {
      callback = options;
      opts = {};
    }

    if (typeof name !== "string")
      throw new TypeError("Expected first argument to be a string");
    if (typeof callback !== "function")
      throw new TypeError("Expected second argument to be a function");

    // -------------------------------------------------------------------------
    // VALIDATE FUNCTION ARITY (ARGUMENT COUNT)
    // -------------------------------------------------------------------------
    // Native better-sqlite3 enforces that UDFs must have a valid argument count (0-100).
    // It checks the '.length' property of the function object.
    //
    // CRITICAL: We must validate this HERE in the main thread.
    // Why? Because we send the function to the worker using 'callback.toString()'.
    // 'toString()' returns only the source code. If the user used 'Object.defineProperty'
    // to manually set an invalid '.length' (e.g., -1 or 101) on the function object,
    // that custom property is LOST during serialization. The worker would reconstruct
    // the function with a valid default length (based on source args), bypassing the check.
    if (!opts.varargs) {
      const len = callback.length;

      // Check for non-integers or negative numbers
      if (!Number.isInteger(len) || len < 0) {
        throw new TypeError(
          "Expected function.length to be a non-negative integer",
        );
      }

      // Check SQLite limit (max 100 arguments for UDFs)
      if (len > 100) {
        throw new RangeError(
          "User-defined functions cannot have more than 100 arguments",
        );
      }
    }

    const fnString = callback.toString();
    const payload = {
      action: "function",
      fnName: name,
      fnString,
      fnOptions: opts,
    };

    await this._execConfig(payload);
    return this;
  }

  /**
   * Register a custom Aggregate Function.
   * Broadcasts to all workers.
   *
   * @param {string} aggName - Name of the aggregate function (e.g. 'MEDIAN').
   * @param {AggregateOptions} options - Configuration object (start, step, inverse, result).
   * @returns {Promise<this>}
   */
  async aggregate(aggName, options) {
    if (typeof aggName !== "string")
      throw new TypeError("Expected first argument to be a string");
    if (typeof options !== "object" || options === null)
      throw new TypeError("Expected second argument to be an options object");
    if (!options.step)
      throw new TypeError("Expected options.step to be a function");

    // Prepare payload
    const payload = {
      action: "aggregate",
      aggName,
      aggOptions: serializeAggregateOptions(options),
    };
    await this._execConfig(payload);
    return this;
  }

  /**
   * Serialize the database to a Buffer.
   * @param {SerializeOptions} [options] - { attached: string }
   * @returns {Promise<Buffer>}
   */
  async serialize(options) {
    this._ensureOpen();
    // Always use the writer to get the most up-to-date state
    let result;
    if (this.readonly) {
      result = await this._requestRead("serialize", { options });
    } else {
      result = await this._requestWrite("serialize", { options });
    }
    return Buffer.from(result); // The worker returns the Buffer
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

    if (!sql) {
      throw new TypeError("SQL statement is required");
    } else if (typeof sql !== "string") {
      throw new TypeError("SQL statement must be a string");
    }

    if (options !== undefined && typeof options !== "object") {
      throw new TypeError("Options must be an object");
    } else if (
      options.simple !== undefined &&
      typeof options.simple !== "boolean"
    ) {
      throw new TypeError("Options.simple must be a boolean");
    }

    const trimmedSql = sql.trim();
    const payload = { action: "pragma", sql, options };

    // READONLY MODE
    if (this.readonly) {
      if (this.readerPool) {
        const results = await this.readerPool.broadcast(payload, true);
        return results[0].pragma;
      }
      return;
    }

    // WRITE MODE (Standard)
    // 1. Execute on Writer (Primary) - This returns the actual pragma result
    let writerRes;
    const activeConn = this.transactionContext.getStore();

    // SPECIAL HANDLING FOR 'REKEY' PRAGMA
    if (/^rekey\s*=/i.test(trimmedSql)) {
      let writerRes;

      // 1. Readers: Convert 'rekey' to 'key'
      const readerSql = trimmedSql.replace(/^rekey/i, "key");
      const readerPayload = { action: "pragma", sql: readerSql, options };

      // 2. Writer: Execute as-is (Perform rewrite)
      if (activeConn) {
        // Inside transaction
        writerRes = await activeConn.writer.noLockExecute(payload);
        // On the writer restart, we need key for the writer
        await activeConn.writer.noLockExecute(readerPayload, true);
      } else {
        // Atomic
        // Note: Not 'Sticky' because the file header persists this setting.
        writerRes = await this.writer.execute(payload, true);
        // On the writer restart, we need key for the writer
        await this.writer.execute(readerPayload, true);
      }

      if (this.readerPool) {
        await this.readerPool.broadcast(readerPayload, true);
      }

      return writerRes;
    }

    // SPECIAL HANDLING FOR 'JOURNAL_MODE' PRAGMA
    // ONLY Execute on Writer. DO NOT Broadcast.
    // Readers automatically pick up the mode from the file header.
    // Broadcasting causes errors (readers cannot change mode) or redundancy.
    if (/^journal_mode\s*=/i.test(trimmedSql)) {
      if (activeConn) {
        // Inside transaction
        const writerRes = await activeConn.writer.noLockExecute(payload, false);
        return writerRes.pragma;
      } else {
        // Atomic
        // Note: Not 'Sticky' because the file header persists this setting.
        const writerRes = await this.writer.execute(payload, false);
        return writerRes.pragma;
      }
    }

    if (activeConn) {
      // Inside transaction
      writerRes = await activeConn.writer.noLockExecute(payload);

      // Broadcast to readers (Sticky)
      if (this.readerPool) {
        await this.readerPool.broadcast(payload, true);
      }
    } else {
      // Atomic: Lock and make Sticky
      writerRes = await this.writer.execute(payload, true);
    }

    // Sync Readers (Sticky)
    if (this.readerPool) {
      await this.readerPool.broadcast(payload, true);
    }

    return writerRes.pragma;
  }

  /**
   * Register a Virtual Table.
   * Broadcasts the table definition to the Writer and all Reader workers.
   *
   * @warning The `factory` function is serialized to a string and executed inside the worker threads.
   * Therefore, it **MUST be strictly self-contained** (pure). It cannot reference variables
   * from the parent scope (closures) or external libraries not available in the worker context.
   *
   * @example
   * await db.table('my_vtab', function() {
   *   return {
   *     rows: function* () { yield [1, 'a']; },
   *     columns: ['id', 'name']
   *   };
   * });
   *
   * @param {string} name - The name of the virtual table.
   * @param {Function} factory - A function that returns the VirtualTableOptions object.
   * @returns {Promise<this>} The Database instance.
   */
  async table(name, factory) {
    this._ensureOpen();

    if (typeof name !== "string")
      throw new TypeError("Expected first argument to be a string");
    if (name.length === 0)
      throw new TypeError("Expected table name to be a non-empty string");
    if (
      typeof factory !== "function" &&
      (typeof factory !== "object" || factory === null)
    ) {
      throw new TypeError(
        "Expected second argument to be a function or a module object",
      );
    }

    let factoryString;
    let isEponymous = false;

    if (typeof factory === "object") {
      // 1. Synchronous Validation (Mimic native better-sqlite3 strictness)
      if (!Array.isArray(factory.columns)) {
        if (factory.columns === undefined)
          throw new TypeError("Expected columns to be an array");
        throw new TypeError("Expected columns to be an array");
      }
      if (factory.columns.length === 0)
        throw new RangeError("Expected columns to be a non-empty array");

      const seen = new Set();
      for (const col of factory.columns) {
        if (typeof col !== "string")
          throw new TypeError("Expected column names to be strings");
        if (seen.has(col)) throw new TypeError("Duplicate column name");
        seen.add(col);
      }

      // If it is present, it MUST be an array. undefined/null are not arrays.
      if ("parameters" in factory && !Array.isArray(factory.parameters)) {
        throw new TypeError("Expected parameters to be an array");
      }

      if (factory.parameters !== undefined) {
        if (!Array.isArray(factory.parameters))
          throw new TypeError("Expected parameters to be an array");
        if (factory.parameters.length > 32)
          throw new RangeError("Too many parameters");
        const seenParams = new Set();
        for (const param of factory.parameters) {
          if (typeof param !== "string")
            throw new TypeError("Expected parameter names to be strings");
          if (seenParams.has(param))
            throw new TypeError("Duplicate parameter name");
          seenParams.add(param);
        }
      }

      if (typeof factory.rows !== "function")
        throw new TypeError("Expected rows to be a generator function");

      const len = factory.rows.length;
      if (!Number.isInteger(len) || len < 0)
        throw new TypeError(
          "Expected function.length to be a non-negative integer",
        );
      if (len > 32)
        throw new RangeError("Virtual table module has too many parameters");

      // 2. Manual Serialization for Objects (Eponymous Wrapper)
      const cols = JSON.stringify(factory.columns);
      // CRITICAL FIX: Only include 'parameters' if it exists.
      // better-sqlite3 throws if 'parameters' is present but undefined.
      const paramsLine = factory.parameters
        ? `parameters: ${JSON.stringify(factory.parameters)},`
        : "";
      const rowsStr = factory.rows.toString().trim();

      // FIX: Correctly determine if rowsStr is a Method Definition or Function Expression
      let methodPart;

      // If it starts with 'function', '(', 'async function', or 'async (', it needs a key "rows:"
      // If it starts with 'rows', '*rows', 'async rows', it INCLUDES the key (Method Shorthand).
      if (
        rowsStr.startsWith("function") ||
        rowsStr.startsWith("(") ||
        rowsStr.startsWith("async function") ||
        rowsStr.startsWith("async (")
      ) {
        methodPart = `rows: ${rowsStr}`;
      } else {
        // Assume Method Shorthand (e.g. "*rows() {}", "rows() {}")
        // This must be pasted AS IS into the object literal.
        methodPart = rowsStr;
      }

      factoryString = `() => ({
         columns: ${cols},
         ${paramsLine}
         ${methodPart}
       })`;

      isEponymous = true;
    } else {
      // Function (Module)
      factoryString = factory.toString();
      isEponymous = false;
    }

    const payload = { action: "table", name, factoryString, isEponymous };
    await this._execConfig(payload);
    return this;
  }

  /**
   * Enable or disable unsafe mode.
   * Unsafe mode disables certain internal SQLite mutexes and safety checks.
   * This can improve performance but may lead to data corruption if the file is accessed
   * by multiple processes without external coordination.
   *
   * Broadcasts the setting to all workers.
   *
   * @param {boolean} [unsafe=true] - If `true`, enables unsafe mode; if `false`, disables it.
   * @returns {Promise<this>} The Database instance.
   */
  async unsafeMode(unsafe = true) {
    const payload = { action: "unsafe_mode", on: unsafe };
    await this._execConfig(payload);
    return this;
  }

  /**
   * Create a backup of the database.
   * This operation runs exclusively on the Writer thread to ensure consistency with the WAL.
   *
   * @description
   * The backup is performed incrementally. If a `progress` callback is provided in the options,
   * it will be invoked periodically as the backup proceeds.
   *
   * Note: The `progress` callback runs on the main thread, while the actual backup logic
   * runs on the worker thread.
   *
   * @example
   * const metadata = await db.backup('backup.db', {
   *   progress({ totalPages, remainingPages }) {
   *     const percent = ((totalPages - remainingPages) / totalPages) * 100;
   *     console.log(`Backup progress: ${percent.toFixed(2)}%`);
   *   }
   * });
   * console.log('Backup complete:', metadata);
   *
   * @param {string} destinationFile - The destination file path for the backup.
   * @param {BackupOptions} [options] - Configuration options (e.g., `attached`, `progress`).
   * @returns {Promise<BackupMetadata>} Resolves with metadata (totalPages, remainingPages) upon completion.
   * @throws {Error} If the database is closed or the Writer worker is unavailable.
   */
  async backup(destinationFile, options = {}) {
    this._ensureOpen();
    if (typeof destinationFile !== "string") {
      throw new TypeError("Expected destinationFile to be a string");
    }
    if (!(await parentDirectoryExists(destinationFile))) {
      throw new SqliteError(
        "Parent directory does not exist",
        "SQLITE_CANTOPEN",
      );
    }
    // We explicitly check if the current async context is inside a transaction wrapper.
    if (this.inTransaction) {
      throw new SqliteError(
        "Cannot execute backup within a transaction",
        "SQLITE_ERROR",
      );
    }
    if (!this.writer) {
      throw new Error("Writer not available");
    }

    const { progress, ...workerOptions } = options;
    if (progress && typeof progress !== "function")
      throw new TypeError("options.progress must be a function");

    // Use streaming protocol on the writer
    const iterator = this.writer.streamExecute({
      action: "backup",
      filename: destinationFile,
      options: workerOptions,
    });

    let finalResult = null;

    for await (const info of iterator) {
      if (progress) progress(info);
      finalResult = info;
    }

    return finalResult;
  }

  /**
   * Execute a simple SQL query (no result retrieval).
   * Useful for DDL statements (CREATE TABLE, DROP TABLE, etc.).
   * @param {string} sql - The SQL statement.
   * @returns {Promise<void>}
   */
  async exec(sql) {
    this._ensureOpen();
    const payload = { action: "exec", sql };

    // READONLY MODE:
    // Route to the Reader Pool.
    // The Worker will let SQLite throw SQLITE_READONLY if it's actually a write.
    if (this.readonly) {
      if (!this.readerPool) {
        throw new TypeError("No available workers for execution");
      }
      await this.readerPool.execute(payload);
      return this;
    }

    // WRITE MODE:
    // Route to Writer via _requestWrite to respect transaction locks.
    await this._requestWrite("exec", payload);
    return this;
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
   * Attaches an external database to the current connection pool.
   * This ensures the database is attached on the Writer and all Reader workers.
   *
   * @param {string} filename - Path to the database file.
   * @param {string} alias - Alias name for the attached database.
   * @param {Object} [options] - Configuration options.
   * @param {string} [options.journalMode] - Optional. Sets the journal_mode for the attached DB (e.g. 'WAL').
   * @param {boolean} [options.fileMustExist=false] - If true, throws if the file does not exist.
   * @returns {Promise<void>}
   */
  async attach(filename, alias, options = {}) {
    this._ensureOpen();

    if (typeof filename !== "string") {
      throw new TypeError("Filename must be a string");
    }
    if (typeof alias !== "string") {
      throw new TypeError("Alias must be a string");
    }

    if (options.fileMustExist) {
      const exists = await fileExists(filename);
      if (!exists) {
        throw new SqliteError(
          `Attached database file "${filename}" does not exist`,
          "SQLITE_CANTOPEN",
        );
      }
    }

    // 1. Construct the ATTACH statement
    // We use a simple string interpolation.
    // Security Note: Ensure 'filename' and 'alias' are trusted or sanitized if coming from user input.
    const sql = `ATTACH DATABASE '${filename}' AS ${alias}`;

    // 2. Broadcast ATTACH to all workers (Writer + Readers) and mark as Sticky.
    // This ensures current and future workers have this DB attached.
    await this._execConfig({ action: "exec", sql });

    // 3. Handle Journal Mode (Optional)
    // We only need to execute this on the Writer. The file header change will apply to readers automatically.
    if (options.journalMode) {
      if (typeof options.journalMode !== "string") {
        throw new TypeError("options.journalMode must be a string");
      }

      const mode = options.journalMode.toUpperCase();
      const pragmaSql = `PRAGMA ${alias}.journal_mode = ${mode}`;

      // We manually route to the Writer instead of using db.pragma()
      // because we don't want to broadcast this setting to read-only workers
      // (which might throw errors trying to change journal modes).
      if (this.writer) {
        // If we are currently in a transaction, use the active connection
        const activeConn = this.transactionContext.getStore();
        if (activeConn) {
          await activeConn.writer.noLockExecute({
            action: "exec",
            sql: pragmaSql,
          });
        } else {
          // Atomic execution on writer.
          // Note: We don't mark this as sticky. The journal_mode is persisted in the database file header.
          await this.writer.execute({ action: "exec", sql: pragmaSql }, false);
        }
      }
    }
  }

  /**
   * Detaches a database from the connection pool.
   *
   * @param {string} alias - The alias of the database to detach.
   * @returns {Promise<void>}
   */
  async detach(alias) {
    this._ensureOpen();
    if (typeof alias !== "string") {
      throw new TypeError("Alias must be a string");
    }

    const sql = `DETACH DATABASE ${alias}`;

    // Broadcast DETACH to all workers and mark as Sticky.
    // This effectively appends a DETACH to the initialization history,
    // ensuring new workers don't keep the attached DB.
    await this._execConfig({ action: "exec", sql });
  }

  /**
   * Close the database connection pool.
   * Sends a graceful close signal to workers to allow SQLite to checkpoint WAL,
   * then terminates the threads.
   * @returns {Promise<this>}
   */
  async close() {
    if (!this.open) return this;

    // 1. Force Release Locks
    // If a transaction was active, we break the lock so we can send the close signal.
    if (this.writer && this.writer.isLocked()) {
      this.writer.unlock();
    }

    const closePayload = { action: "close" };

    // 3. Graceful Shutdown Helper
    const forceCloseWorker = async (client) => {
      try {
        // Use noLockExecute to ensure message sends even if logic thinks it's busy
        await client.noLockExecute(closePayload);
      } catch (e) {
        /* ignore dead worker */
      }
    };

    // console.log("Graceful shutdown initiated");

    try {
      // Close Readers first (allows WAL checkpointing)
      if (this.readerPool) {
        const promises = this.readerPool.workers.map((w) =>
          forceCloseWorker(w),
        );
        await Promise.all(promises);
      }
      // Close Writer last
      if (this.writer) {
        await forceCloseWorker(this.writer);
      }
    } catch (e) {
      /* ignore */
    }

    // console.log("Graceful shutdown completed");

    // 4. Terminate Threads (Hard Kill)
    const terminatePromises = [];
    if (this.writer) terminatePromises.push(this.writer.terminate());
    if (this.readerPool) terminatePromises.push(this.readerPool.close());

    await Promise.all(terminatePromises);

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
   * Routes a Write request (INSERT/UPDATE/DELETE/PRAGMA).
   * @param {string} action - The action type (e.g., 'run', 'exec').
   * @param {string|Object} sqlOrPayload - The SQL or statement payload.
   * @param {Array} params - Query parameters.
   * @returns {Promise<any>}
   * @private
   */
  async _requestWrite(action, sqlOrPayload, params) {
    this._ensureOpen();
    if (this.readonly) {
      throw new SqliteError(
        "attempt to write a readonly database",
        "SQLITE_READONLY",
      );
    }

    // 1. Check for Active Context (db.transaction wrapper)
    // If we are inside a transaction, the lock is already held by the connection object.
    // We route this query through that connection to bypass the lock check.
    const activeConn = this.transactionContext.getStore();
    if (activeConn) {
      return activeConn._requestWrite(action, sqlOrPayload, params);
    }

    // 2. Safety Check: Forbid Raw BEGIN on global scope
    // Manual transactions MUST use 'await db.acquire()' to ensure they get released.
    const sql =
      typeof sqlOrPayload === "string" ? sqlOrPayload : sqlOrPayload.sql;
    if (typeof sql === "string" && /^\s*BEGIN/i.test(sql)) {
      throw new Error(
        "Manual transactions on the global 'db' instance are disabled. Use 'await db.acquire()' or 'db.transaction()'.",
      );
    }

    // 3. Atomic Execution
    // This is a standard "One-Shot" write.
    await this.writer.lock();
    try {
      const payload =
        typeof sqlOrPayload === "object"
          ? { action, ...sqlOrPayload }
          : { action, sql: sqlOrPayload, params };

      return await this.writer.noLockExecute(payload);
    } finally {
      this.writer.unlock();
    }
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

    // 1. Check for Active Context (Read-Your-Own-Writes)
    const activeConn = this.transactionContext.getStore();
    if (activeConn) {
      // Route to Writer via Connection
      return activeConn._requestRead(action, sqlOrPayload, params);
    }

    // 2. Fallback to Writer (Memory DB or No Pool)
    if (this.memory || (!this.readerPool && this.writer)) {
      // Route to Writer (Atomic)
      return this._requestWrite(action, sqlOrPayload, params);
    }

    if (!this.readerPool) {
      throw new TypeError("No available workers for read operation");
    }

    const isObj = typeof sqlOrPayload === "object" && sqlOrPayload !== null;
    const payload = isObj
      ? { action, ...sqlOrPayload }
      : { action, sql: sqlOrPayload, params, options: undefined };

    // 3. Parallel Execution (Load Balanced)
    return this.readerPool.execute(payload);
  }
}

module.exports = Database;
