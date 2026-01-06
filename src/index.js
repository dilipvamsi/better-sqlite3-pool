/**
 * @file index.js
 * @description Main entry point for better-sqlite3-pool.
 */
const { Worker } = require("node:worker_threads");
const { SqliteError } = require("better-sqlite3-multiple-ciphers");
const path = require("node:path");
const EventEmitter = require("events");
const Mutex = require("./lib/mutex");
const Statement = require("./lib/statement");
const { createError } = require("./lib/utils");

class Database extends EventEmitter {
  /**
   * @param {string} filename - Path to DB file.
   * @param {Object} [options]
   * @param {number} [options.min=2] - Min readers.
   * @param {number} [options.max=8] - Max readers (auto-scale limit).
   */
  constructor(filename, options = {}) {
    super();
    this.filename = filename;
    this.options = options;

    // DETECT MEMORY MODE
    // Standard :memory: or empty string '' are private in-memory DBs
    this.memory = filename === ":memory:" || filename === "";

    // 1. Determine Target Values
    let targetMin = options.min !== undefined ? options.min : 1;
    let targetMax = options.max !== undefined ? options.max : 2;

    // 2. Validate Options (unless in memory mode, which overrides everything)
    if (this.memory) {
      this.minReaders = 0;
      this.maxReaders = 0;
    } else {
      if (!Number.isInteger(targetMin) || targetMin < 0) {
        throw new TypeError("options.min must be a positive integer");
      }
      if (!Number.isInteger(targetMax) || targetMax < 0) {
        throw new TypeError("options.max must be a positive integer");
      }
      if (targetMin > targetMax) {
        throw new RangeError("options.min cannot be greater than options.max");
      }
      this.minReaders = targetMin;
      this.maxReaders = targetMax;
    }

    // State to replay for new workers
    this._initPragmas = [];
    this._initFunctions = [];

    // Writer (Single Thread, Serialized)
    this.writer = new Worker(path.resolve(__dirname, "lib/worker-write.js"), {
      workerData: { filename },
    });
    this.writeQueue = new Map();
    this.writeId = 0;

    // Bind Messaging
    this.writer.on("message", (msg) => this._handleMsg(this.writeQueue, msg));

    // Bind Error Handling (CRITICAL FIX)
    this._bindWorkerEvents(this.writer, this.writeQueue);

    this.writeMutex = new Mutex(); // Global Transaction Lock

    // Readers (Pool)
    this.readers = [];
    this.readQueue = [];

    // Only init readers if NOT in memory mode
    if (!this.memory) {
      this._initReaders();
    }

    this._inTransaction = false;
  }

  prepare(sql) {
    return new Statement(this, sql);
  }

  /**
   * Register a UDF (User Defined Function).
   * Broadcasts to all current and future workers.
   */
  function(name, fn) {
    const fnString = fn.toString();
    this._initFunctions.push({ name, fnString });

    const payload = { action: "function", fnName: name, fnString, id: -1 };
    this.writer.postMessage(payload);
    this.readers.forEach((r) => r.worker.postMessage(payload));

    return this;
  }

  /**
   * Execute a Pragma. Broadcasts to all workers.
   */
  async pragma(sql, options = {}) {
    this._initPragmas.push(sql);

    // Send to Writer (Await)
    await this._requestWrite("exec", `PRAGMA ${sql}`, []);

    // Send to Readers (Fire & Forget)
    this.readers.forEach((r) =>
      r.worker.postMessage({ action: "exec", sql: `PRAGMA ${sql}`, id: -1 })
    );

    return options.simple ? undefined : [];
  }

  async exec(sql) {
    return this.prepare(sql).run();
  }

  /**
   * Dynamically increases the size of the reader pool.
   * New values must be greater than or equal to current settings.
   * @param {number} min - New minimum readers (must be >= current min)
   * @param {number} max - New maximum readers (must be >= current max)
   */
  pool(min, max) {
    if (this.memory) return;

    if (!Number.isInteger(min) || min < 0)
      throw new Error("min must be a positive integer");
    if (!Number.isInteger(max) || max < 0)
      throw new Error("max must be a positive integer");
    if (min > max) throw new Error("min cannot be greater than max");

    // --- VALIDATION: Ensure we only Scale Up ---
    // This guarantees we never need to shrink, preserving active workers.
    if (min < this.minReaders) {
      throw new Error(
        `New min (${min}) cannot be smaller than current min (${this.minReaders})`
      );
    }
    if (max < this.maxReaders) {
      throw new Error(
        `New max (${max}) cannot be smaller than current max (${this.maxReaders})`
      );
    }

    this.minReaders = min;
    this.maxReaders = max;

    // Grow: Check if we are below the new min and spawn immediately
    while (this.readers.length < this.minReaders) {
      this._spawnReader();
    }
  }

  async close() {
    await this.writer.terminate();
    await Promise.all(this.readers.map((r) => r.worker.terminate()));
  }

  // --- Internal Engine ---

  _initReaders() {
    for (let i = 0; i < this.minReaders; i++) this._spawnReader();
  }

  _spawnReader() {
    const worker = new Worker(path.resolve(__dirname, "lib/worker-read.js"), {
      workerData: { filename: this.filename },
    });

    // Replay State
    for (const sql of this._initPragmas)
      worker.postMessage({ action: "exec", sql: `PRAGMA ${sql}`, id: -1 });
    for (const udf of this._initFunctions)
      worker.postMessage({
        action: "function",
        fnName: udf.name,
        fnString: udf.fnString,
        id: -1,
      });

    const reader = {
      id: Date.now() + Math.random(),
      worker,
      busy: false,
      queue: new Map(),
    };

    // Bind Messaging
    worker.on("message", (msg) => {
      if (msg.status !== "stream_data") {
        this._handleMsg(reader.queue, msg);
        reader.busy = false;
        this._drainReadQueue();
      }
    });

    // Bind Error Handling (CRITICAL FIX)
    this._bindWorkerEvents(worker, reader.queue, reader);

    this.readers.push(reader);
    return reader;
  }

  /**
   * Attaches Error and Exit listeners to a worker.
   * If the worker crashes, it rejects all pending tasks in its queue.
   */
  _bindWorkerEvents(worker, queue, readerRef = null) {
    worker.on("error", (err) => {
      this._cleanupWorker(queue, err, readerRef);
    });

    worker.on("exit", (code) => {
      if (code !== 0) {
        this._cleanupWorker(
          queue,
          new Error(`Worker exited with code ${code}`),
          readerRef
        );
      }
    });
  }

  /**
   * Cleans up a dead worker: rejects tasks and removes from pool.
   */
  _cleanupWorker(queue, err, readerRef) {
    // 1. Reject ALL pending tasks for this worker
    for (const [id, task] of queue.entries()) {
      task.reject(createError(err)); // Use helper to wrap as SqliteError
    }
    queue.clear();

    // 2. Remove from reader pool if applicable
    if (readerRef) {
      const idx = this.readers.indexOf(readerRef);
      if (idx !== -1) this.readers.splice(idx, 1);
    }
  }

  _handleMsg(queue, { id, status, data, error }) {
    if (id === -1) return;
    const task = queue.get(id);
    if (!task) return;
    queue.delete(id);
    if (status === "success") {
      task.resolve(data);
    } else {
      // RECONSTRUCT ERROR HERE
      // This ensures 'err instanceof Database.SqliteError' is true
      task.reject(createError(error));
    }
  }

  async _requestWrite(action, sqlOrPayload, params) {
    // Unpack payload
    const isObj = typeof sqlOrPayload === "object" && sqlOrPayload !== null;
    const sql = isObj ? sqlOrPayload.sql : sqlOrPayload;
    const options = isObj ? sqlOrPayload.options : undefined;
    const p = isObj ? sqlOrPayload.params : params;

    const lock = !this._inTransaction;
    if (lock) await this.writeMutex.acquire();
    try {
      return await new Promise((resolve, reject) => {
        const id = this.writeId++;
        this.writeQueue.set(id, { resolve, reject });
        this.writer.postMessage({ id, action, sql, params: p, options });
      });
    } finally {
      if (lock) this.writeMutex.release();
    }
  }

  _requestRead(action, sqlOrPayload, params) {
    return new Promise((resolve, reject) => {
      const isObj = typeof sqlOrPayload === "object" && sqlOrPayload !== null;
      const sql = isObj ? sqlOrPayload.sql : sqlOrPayload;
      const options = isObj ? sqlOrPayload.options : undefined;
      const p = isObj ? sqlOrPayload.params : params;

      const task = { action, sql, params: p, options, resolve, reject };
      const free = this.readers.find((r) => !r.busy);

      if (free) this._execRead(free, task);
      else if (this.readers.length < this.maxReaders)
        this._execRead(this._spawnReader(), task);
      else this.readQueue.push(task);
    });
  }

  _execRead(reader, task) {
    reader.busy = true;
    const id = Math.random();
    reader.queue.set(id, task);
    reader.worker.postMessage({
      id,
      action: task.action,
      sql: task.sql,
      params: task.params,
      options: task.options,
    });
  }

  _drainReadQueue() {
    if (!this.readQueue.length) return;
    const free = this.readers.find((r) => !r.busy);
    if (free) this._execRead(free, this.readQueue.shift());
  }
}

// --- STATIC EXPORTS ---

// 1. Expose SqliteError for instanceof checks
Database.SqliteError = SqliteError;

// 2. Expose Statement class so users can use it as a type or value
Database.Statement = Statement;

module.exports = Database;
