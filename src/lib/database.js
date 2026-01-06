/**
 * @file database.js
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
const { createError } = require("./utils");

// =============================================================================
// INTERNAL TYPE DEFINITIONS
// =============================================================================

/**
 * @typedef {Object} DatabaseOptions
 * @property {number} [min=1] - Minimum number of reader workers to keep alive.
 * @property {number} [max=2] - Maximum number of reader workers allowed.
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
 * @property {number} [id] - The message ID (used for correlation).
 * @property {string} action - The operation to perform.
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
 * @property {any} [data] - The result payload (on success).
 * @property {any} [error] - The error payload (on error).
 */

// =============================================================================
// MAIN CLASS
// =============================================================================

/**
 * @class Database
 * @extends EventEmitter
 * @description The main Database class that acts as a connection pool manager.
 * It abstracts away the complexity of managing worker threads, query routing,
 * and synchronization between the main thread and the worker pool.
 */
class Database extends EventEmitter {
  /**
   * Create a new Database connection pool.
   * @param {string} filename - Path to the SQLite database file (or ':memory:').
   * @param {Object} [options] - Configuration options.
   * @param {number} [options.min=2] - Minimum number of reader workers to keep alive.
   * @param {number} [options.max=8] - Maximum number of reader workers allowed.
   * @throws {TypeError|RangeError} If options are invalid.
   */
  constructor(filename, options = {}) {
    super();
    this.filename = filename;
    this.options = options;

    /** @type {boolean} - True if the database is strictly in-memory. */
    this.memory = filename === ":memory:" || filename === "";

    // --- Configuration Validation ---
    let targetMin = options.min !== undefined ? options.min : 1;
    let targetMax = options.max !== undefined ? options.max : 2;

    if (this.memory) {
      // In-memory DBs cannot share state across threads easily, so we disable the pool.
      // All queries will be routed to the single "Writer" thread.
      this.minReaders = 0;
      this.maxReaders = 0;
    } else {
      if (!Number.isInteger(targetMin) || targetMin < 0)
        throw new TypeError("options.min must be a positive integer");
      if (!Number.isInteger(targetMax) || targetMax < 0)
        throw new TypeError("options.max must be a positive integer");
      if (targetMin > targetMax)
        throw new RangeError("options.min cannot be greater than options.max");
      this.minReaders = targetMin;
      this.maxReaders = targetMax;
    }

    /** * Indicates if the database is open and fully initialized.
     * @type {boolean}
     */
    this.open = false;

    /** @type {string[]} Stores pragmas to replay on new workers. */
    this._initPragmas = [];

    /** * @type {Array<{name: string, fnString: string}>}
     * Stores User Defined Functions (UDFs) to replay on new workers.
     * We store the function body as a string to pass it to workers.
     */
    this._initFunctions = [];

    // --- INITIALIZATION STATE ---

    /** @type {boolean} - Flag indicating if the Writer has created the DB file. */
    this.writerReady = false;

    /** * @type {Promise<void>}
     * A global promise that resolves when the Writer is fully initialized.
     * Readers will not be spawned until this resolves to prevent race conditions.
     */
    this.writerReadyPromise = new Promise((resolve, reject) => {
      this._resolveWriterReady = resolve;
      this._rejectWriterReady = reject;
    });

    // --- WRITER SETUP ---

    /** @type {Map<number, PendingPromise>} - Pending write tasks. */
    this.writeQueue = new Map();
    /** @type {number} - Auto-incrementing ID for write tasks. */
    this.writeId = 0;
    /** @type {Mutex} - Serializes write operations to ensure ACID compliance. */
    this.writeMutex = new Mutex();

    // Spawn the single Writer worker (Privileged thread)
    this.writer = new Worker(path.resolve(__dirname, "worker-write.js"), {
      workerData: { filename },
    });

    // Listen for messages from the Writer
    this.writer.on("message", (msg) => {
      // Handle the specific "Ready" signal
      if (msg.status === "ready") {
        this.writerReady = true;

        // Replay any settings configured before the DB was ready
        this._replayStateToWorker(this.writer);

        // Resolve the initialization promise
        if (this._resolveWriterReady) this._resolveWriterReady();

        // Safe to start readers now that the file exists
        if (!this.memory) this._initReaders();
        return;
      }
      // Handle standard query results
      this._handleMsg(this.writeQueue, msg);
    });

    // Delegate error handling to dedicated binder
    this._bindWriteWorkerEvents(this.writer);

    // --- READER SETUP ---

    /** * List of active Reader worker objects.
     * @type {Reader[]}
     */
    this.readers = [];

    /** * Queue of read tasks waiting for an available worker.
     * @type {QueuedTask[]}
     */
    this.readQueue = [];

    /** @type {boolean} - Internal flag for transaction state tracking. */
    this._inTransaction = false;
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
    if (typeof name !== "string")
      throw new TypeError("Expected first argument to be a string");
    if (typeof fn !== "function")
      throw new TypeError("Expected second argument to be a function");

    // Ensure infrastructure is up before broadcasting
    await this._waitForInitialization();

    const fnString = fn.toString();
    this._initFunctions.push({ name, fnString });
    const payload = { action: "function", fnName: name, fnString };

    // Send to Writer and wait for Ack
    const writePromise = this._postWriter(payload);
    // Send to Readers and wait for Ack
    const readPromises = this.readers.map((r) => this._postReader(r, payload));

    await Promise.all([writePromise, ...readPromises]);
    return this;
  }

  /**
   * Execute a PRAGMA statement.
   * Broadcasts the setting to the Writer and all Readers.
   * Waits for acknowledgement from all workers.
   * @param {string} sql - The PRAGMA statement (e.g., "journal_mode = WAL").
   * @param {Object} [options] - Options.
   * @param {boolean} [options.simple] - If true, returns the first value of the first row.
   * @returns {Promise<any>} The result of the PRAGMA execution.
   */
  async pragma(sql, options = {}) {
    await this._waitForInitialization();

    this._initPragmas.push(sql);
    const payload = { action: "exec", sql: `PRAGMA ${sql}` };

    // Send to Writer (Primary execution)
    const writePromise = this._postWriter({
      ...payload,
      params: [],
      options: {},
    });
    // Send to Readers (Synchronization only)
    const readPromises = this.readers.map((r) => this._postReader(r, payload));

    // Wait for everyone, but return the Writer's result
    const [result] = await Promise.all([writePromise, ...readPromises]);

    return options.simple ? undefined : result;
  }

  /**
   * Execute a simple SQL query (no result retrieval).
   * Useful for DDL statements (CREATE TABLE, DROP TABLE, etc.).
   * @param {string} sql - The SQL statement.
   * @returns {Promise<void>}
   */
  async exec(sql) {
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
    if (this.memory) return;

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

    this.minReaders = min;
    this.maxReaders = max;

    // Ensure we don't scale before the DB exists
    await this._waitForInitialization();

    // Spawn new readers if necessary
    const startupPromises = [];
    while (this.readers.length < this.minReaders) {
      const reader = this._spawnReader();
      if (reader && reader.readyPromise) {
        startupPromises.push(reader.readyPromise);
      }
    }

    // Wait for all new readers to signal "Ready"
    if (startupPromises.length > 0) await Promise.all(startupPromises);
  }

  /**
   * Close the database connection pool.
   * Terminates the Writer and all Reader workers.
   * @returns {Promise<this>}
   */
  async close() {
    this.open = false;
    this.emit("close");

    const promises = [];

    // Terminate Readers
    if (this.readers && this.readers.length > 0) {
      promises.push(...this.readers.map((r) => r.worker.terminate()));
    }

    // Terminate Writer
    if (this.writer) {
      promises.push(this.writer.terminate());
    }

    await Promise.all(promises);

    this.readers = [];
    this.writer = null;
    this.readQueue = [];
    this.writeQueue.clear();
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
    const writePromise = this._postWriter(payload);
    const readPromises = this.readers.map((r) => this._postReader(r, payload));
    await Promise.all([writePromise, ...readPromises]);
  }

  /**
   * Generic helper to send a payload to the Writer and wait for a response.
   * Handles Mutex locking for write operations.
   * @param {WorkerPayload} payload - The message payload.
   * @returns {Promise<any>} The worker response.
   * @private
   */
  async _postWriter(payload) {
    // Acquire lock if not already inside a transaction
    const lock = !this._inTransaction;
    if (lock) await this.writeMutex.acquire();
    try {
      return await new Promise((resolve, reject) => {
        const id = this.writeId++;
        // Store the promise handlers to be called when worker replies
        this.writeQueue.set(id, { resolve, reject });
        this.writer.postMessage({ ...payload, id });
      });
    } finally {
      if (lock) this.writeMutex.release();
    }
  }

  /**
   * Generic helper to send a payload to a specific Reader and wait for a response.
   * @param {Reader} reader - The reader object.
   * @param {WorkerPayload} payload - The message payload.
   * @returns {Promise<any>} The worker response.
   * @private
   */
  _postReader(reader, payload) {
    return new Promise((resolve, reject) => {
      const id = Date.now() + Math.random();
      reader.queue.set(id, { resolve, reject });
      reader.worker.postMessage({ ...payload, id });
    });
  }

  /**
   * Replays recorded Pragmas and UDFs to a newly spawned worker.
   * Ensures new workers inherit the state of the pool.
   * @param {Worker} worker - The worker thread instance.
   * @private
   */
  _replayStateToWorker(worker) {
    // Send as fire-and-forget (id: -1) during initialization
    for (const sql of this._initPragmas) {
      worker.postMessage({ action: "exec", sql: `PRAGMA ${sql}`, id: -1 });
    }
    for (const udf of this._initFunctions) {
      worker.postMessage({
        action: "function",
        fnName: udf.name,
        fnString: udf.fnString,
        id: -1,
      });
    }
  }

  /**
   * Initializes the reader pool up to the minimum required size.
   * @private
   */
  _initReaders() {
    for (let i = 0; i < this.minReaders; i++) this._spawnReader();
  }

  /**
   * Spawns a single new Reader worker.
   * Sets up event listeners, replays state, and registers the startup promise.
   * @returns {Reader} The internal reader object.
   * @private
   */
  _spawnReader() {
    const worker = new Worker(path.resolve(__dirname, "worker-read.js"), {
      workerData: { filename: this.filename },
    });

    // Sync state immediately
    this._replayStateToWorker(worker);

    // Setup Startup Promise
    let resolveReady, rejectReady;
    const readyPromise = new Promise((res, rej) => {
      resolveReady = res;
      rejectReady = rej;
    });

    const reader = {
      id: Date.now() + Math.random(),
      worker,
      busy: false,
      ready: false,
      queue: new Map(),
      readyPromise,
    };

    worker.on("message", (msg) => {
      // Handle Ready Signal
      if (msg.status === "ready") {
        reader.ready = true;
        if (resolveReady) resolveReady();
        // If tasks accumulated while booting, process them now
        this._drainSpecificReaderQueue(reader);
        return;
      }
      // Handle Data Stream/Result
      if (msg.status !== "stream_data") {
        this._handleMsg(reader.queue, msg);
        reader.busy = false;
        // Reader is free, try to take a job from the global queue
        this._drainReadQueue();
      }
    });

    // Delegate error handling (passing the rejectReady callback)
    this._bindReadWorkerEvents(worker, reader, rejectReady);

    this.readers.push(reader);
    // Check if we can assign immediate work
    this._drainReadQueue();
    return reader;
  }

  /**
   * Dedicated Error/Exit handler for the WRITER worker.
   * Handles global state rejection (this.writerReadyPromise) on crash.
   * @param {Worker} worker - The writer worker instance.
   * @private
   */
  _bindWriteWorkerEvents(worker) {
    worker.on("error", (err) => {
      const error = createError(err);
      // If writer crashes during startup, reject the global init promise
      if (!this.writerReady && this._rejectWriterReady) {
        this._rejectWriterReady(error);
      }
      // Flush any pending Reads that were waiting for init
      if (this.readQueue && this.readQueue.length > 0) {
        this.readQueue.forEach((task) => task.reject(error));
        this.readQueue.length = 0;
      }
      // Fail all pending writes
      this._cleanupWorker(this.writeQueue, err, null);
    });

    worker.on("exit", (code) => {
      if (code !== 0) {
        const error = new Error(`Writer Worker exited with code ${code}`);
        if (!this.writerReady && this._rejectWriterReady) {
          this._rejectWriterReady(error);
        }
        this._cleanupWorker(this.writeQueue, error, null);
      }
    });
  }

  /**
   * Dedicated Error/Exit handler for READER workers.
   * Handles specific startup rejection (rejectReady) on crash.
   * @param {Worker} worker - The reader worker instance.
   * @param {Reader} readerRef - The internal reader object reference.
   * @param {Function} [rejectReady] - Callback to reject the startup promise.
   * @private
   */
  _bindReadWorkerEvents(worker, readerRef, rejectReady) {
    worker.on("error", (err) => {
      const error = createError(err);
      // If this specific reader is booting, reject its startup promise
      if (!readerRef.ready && rejectReady) {
        rejectReady(error);
      }
      this._cleanupWorker(readerRef.queue, err, readerRef);
    });

    worker.on("exit", (code) => {
      if (code !== 0) {
        const error = new Error(`Reader Worker exited with code ${code}`);
        if (!readerRef.ready && rejectReady) {
          rejectReady(error);
        }
        this._cleanupWorker(readerRef.queue, error, readerRef);
      }
    });
  }

  /**
   * Tries to pull a task from the global read queue for a specific reader.
   * Used when a reader becomes "ready" after booting.
   * @param {Reader} reader - The reader object.
   * @private
   */
  _drainSpecificReaderQueue(reader) {
    if (!this.readQueue.length || reader.busy) return;
    const task = this.readQueue.shift();
    if (task) this._execRead(reader, task);
  }

  /**
   * Cleans up tasks associated with a crashed worker.
   * @param {Map} queue - The task queue for the worker.
   * @param {Error} err - The error that caused the crash.
   * @param {Reader|null} readerRef - The reader reference (null for Writer).
   * @private
   */
  _cleanupWorker(queue, err, readerRef) {
    for (const [id, task] of queue.entries()) {
      task.reject(createError(err));
    }
    queue.clear();
    if (readerRef) {
      const idx = this.readers.indexOf(readerRef);
      if (idx !== -1) this.readers.splice(idx, 1);
    }
  }

  /**
   * Handles a standard response message from a worker.
   * Resolves or Rejects the promise associated with the message ID.
   * @param {Map} queue - The pending task map.
   * @param {WorkerResponse} msg - The message payload.
   * @private
   */
  _handleMsg(queue, { id, status, data, error }) {
    if (id === -1) return;
    const task = queue.get(id);
    if (!task) return;
    queue.delete(id);
    if (status === "success") {
      task.resolve(data);
    } else {
      task.reject(createError(error));
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
    // Strictly await initialization
    if (!this.writerReady) await this.writerReadyPromise;

    // CRITICAL FIX: Check if we are still open after waiting
    if (!this.writer) {
      throw new TypeError("The database connection is not open");
    }

    const isObj = typeof sqlOrPayload === "object" && sqlOrPayload !== null;
    const sql = isObj ? sqlOrPayload.sql : sqlOrPayload;
    const options = isObj ? sqlOrPayload.options : undefined;
    const p = isObj ? sqlOrPayload.params : params;

    // Delegate to generic helper
    return this._postWriter({ action, sql, params: p, options });
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
    // If in memory, fallback to Writer
    if (this.memory) {
      return this._requestWrite(action, sqlOrPayload, params);
    }

    await this._waitForInitialization();

    // CRITICAL FIX: Check if we are still open after waiting
    if (!this.open || !this.writer) {
      throw new TypeError("The database connection is not open");
    }

    return new Promise((resolve, reject) => {
      const isObj = typeof sqlOrPayload === "object" && sqlOrPayload !== null;
      const sql = isObj ? sqlOrPayload.sql : sqlOrPayload;
      const options = isObj ? sqlOrPayload.options : undefined;
      const p = isObj ? sqlOrPayload.params : params;

      const task = { action, sql, params: p, options, resolve, reject };

      // Load Balancing Strategy:
      // 1. Try to find an Idle reader
      const free = this.readers.find((r) => !r.busy);

      if (free) {
        this._execRead(free, task);
      } else if (this.readers.length < this.maxReaders) {
        // 2. If no idle reader, but we can scale up, spawn a new one
        this._execRead(this._spawnReader(), task);
      } else {
        // 3. Queue the task
        this.readQueue.push(task);
      }
    });
  }

  /**
   * Executes a read task on a specific reader.
   * Ensures the reader is fully initialized before sending the message.
   * @param {Reader} reader - The reader object.
   * @param {QueuedTask} task - The task payload.
   * @private
   */
  async _execRead(reader, task) {
    reader.busy = true;
    const id = Math.random();
    reader.queue.set(id, task);

    // Guard: Wait for this specific reader to boot
    if (!reader.ready) {
      try {
        await reader.readyPromise;
      } catch (err) {
        // Task will be rejected via _cleanupWorker if boot fails
        return;
      }
    }

    reader.worker.postMessage({
      id,
      action: task.action,
      sql: task.sql,
      params: task.params,
      options: task.options,
    });
  }

  /**
   * Checks the global read queue and assigns work to idle readers.
   * @private
   */
  _drainReadQueue() {
    if (!this.readQueue.length) return;
    const free = this.readers.find((r) => !r.busy);
    if (free) this._execRead(free, this.readQueue.shift());
  }
}

module.exports = { Database };
