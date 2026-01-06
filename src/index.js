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

    // FLAG: Is the writer ready?
    this.writerReady = false;

    // Create a Promise that resolves when the DB is created, or rejects if it fails.
    this.writerReadyPromise = new Promise((resolve, reject) => {
      this._resolveWriterReady = resolve;
      this._rejectWriterReady = reject;
    });

    // Writer (Single Thread, Serialized)
    this.writer = new Worker(path.resolve(__dirname, "lib/worker-write.js"), {
      workerData: { filename },
    });
    this.writeQueue = new Map();
    this.writeId = 0;

    // Bind Messaging
    // 1. Handle Ready Signal
    this.writer.on("message", (msg) => {
      if (msg.status === "ready") {
        this.writerReady = true;
        if (this._resolveWriterReady) this._resolveWriterReady();

        // Only now can we safely spawn initial readers
        if (!this.memory) {
          this._initReaders();
        }
        return;
      }
      this._handleMsg(this.writeQueue, msg);
    });

    // 2. Handle Startup Errors (Critical for preventing hangs)
    this.writer.on("error", (err) => {
      const error = createError(err);

      // If we crashed before being ready, reject the init promise
      if (!this.writerReady && this._rejectWriterReady) {
        this._rejectWriterReady(error);
      }

      // Also flush any pending Reads that were waiting in the queue for init
      if (this.readQueue && this.readQueue.length > 0) {
        this.readQueue.forEach((task) => task.reject(error));
        this.readQueue.length = 0;
      }
    });

    // Bind Error Handling (CRITICAL FIX)
    this._bindWorkerEvents(this.writer, this.writeQueue);

    this.writeMutex = new Mutex(); // Global Transaction Lock

    // Readers (Pool)
    this.readers = [];
    this.readQueue = [];

    this._inTransaction = false;
  }

  prepare(sql) {
    return new Statement(this, sql);
  }

  /**
   * Helper: Waits for Writer and ALL currently booting readers to be ready.
   */
  async _waitForInitialization() {
    // 1. Wait for Writer
    if (!this.writerReady) {
      await this.writerReadyPromise;
    }

    // 2. Wait for any booting Readers
    // We filter for readers that exist but haven't flagged 'ready' yet.
    const pendingReaders = this.readers
      .filter((r) => !r.ready && r.readyPromise)
      .map((r) => r.readyPromise);

    if (pendingReaders.length > 0) {
      await Promise.all(pendingReaders);
    }
  }

  /**
   * Register a User Defined Function.
   * Safe to call before database is ready.
   */
  async function(name, fn) {
    // 1. Validate Input
    if (typeof name !== "string")
      throw new TypeError("Expected first argument to be a string");
    if (typeof fn !== "function")
      throw new TypeError("Expected second argument to be a function");

    // Wait for infrastructure
    await this._waitForInitialization();

    // 2. Persist for future workers
    const fnString = fn.toString();
    this._initFunctions.push({ name, fnString });

    const payload = { action: "function", fnName: name, fnString, id: -1 };

    // 3. Send to Writer ONLY if ready (otherwise replayed in 'ready' event)
    if (this.writerReady) {
      this.writer.postMessage(payload);
    }

    // 4. Send to Readers (Node buffers messages, so this is generally safe)
    if (this.readers.length > 0) {
      this.readers.forEach((r) => r.worker.postMessage(payload));
    }
    return this;
  }

  /**
   * Execute a Pragma. Broadcasts to all workers.
   */
  async pragma(sql, options = {}) {
    // Wait for infrastructure
    await this._waitForInitialization();

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
   * Waits for Writer to be ready, then spawns readers and waits for them to be ready.
   * New values must be greater than or equal to current settings.
   * @param {number} min - New minimum readers (must be >= current min)
   * @param {number} max - New maximum readers (must be >= current max)
   */
  async pool(min, max) {
    if (this.memory) return;

    if (!Number.isInteger(min) || min < 0)
      throw new TypeError("min must be a positive integer");
    if (!Number.isInteger(max) || max < 0)
      throw new TypeError("max must be a positive integer");
    if (min > max) throw new RangeError("min cannot be greater than max");

    // Scale Up Only Validation
    if (min < this.minReaders)
      throw new RangeError(
        `New min (${min}) cannot be smaller than current min (${this.minReaders})`
      );
    if (max < this.maxReaders)
      throw new RangeError(
        `New max (${max}) cannot be smaller than current max (${this.maxReaders})`
      );

    // Use common helper
    await this._waitForInitialization();

    this.minReaders = min;
    this.maxReaders = max;

    // 1. Wait for Writer (if not ready)
    if (!this.writerReady) {
      await this.writerReadyPromise;
    }

    // 2. Spawn Readers and collect their ready promises
    const startupPromises = [];
    while (this.readers.length < this.minReaders) {
      const reader = this._spawnReader();
      if (reader && reader.readyPromise) {
        startupPromises.push(reader.readyPromise);
      }
    }

    // 3. Wait for all new readers to be fully operational
    if (startupPromises.length > 0) {
      await Promise.all(startupPromises);
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

    for (const sql of this._initPragmas)
      worker.postMessage({ action: "exec", sql: `PRAGMA ${sql}`, id: -1 });
    for (const udf of this._initFunctions)
      worker.postMessage({
        action: "function",
        fnName: udf.name,
        fnString: udf.fnString,
        id: -1,
      });

    // Setup Reader object with a Ready Promise
    let resolveReady, rejectReady;
    const readyPromise = new Promise((res, rej) => {
      resolveReady = res;
      rejectReady = rej;
    });

    const reader = {
      id: Date.now() + Math.random(),
      worker,
      busy: false,
      ready: false, // FLAG: Is this reader fully initialized?
      queue: new Map(),
      readyPromise, // Expose promise for pool() to await
    };

    worker.on("message", (msg) => {
      if (msg.status === "ready") {
        reader.ready = true;
        if (resolveReady) resolveReady();
        // Flush any tasks that were assigned to this reader while it was booting
        this._drainSpecificReaderQueue(reader);
        return;
      }
      if (msg.status !== "stream_data") {
        this._handleMsg(reader.queue, msg);
        reader.busy = false;
        this._drainReadQueue();
      }
    });

    // Hook up rejection logic for startup failures
    worker.on("error", (err) => {
      if (!reader.ready && rejectReady) rejectReady(createError(err));
      this._cleanupWorker(reader.queue, err, reader);
    });
    worker.on("exit", (code) => {
      if (!reader.ready && code !== 0 && rejectReady)
        rejectReady(new Error(`Worker exited with code ${code}`));
      if (code !== 0)
        this._cleanupWorker(
          reader.queue,
          new Error(`Worker exited with code ${code}`),
          reader
        );
    });

    this._bindWorkerEvents(worker, reader.queue, reader);

    this.readers.push(reader);

    // Check for pending work
    this._drainReadQueue();

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
    // Strictly wait for DB initialization before attempting writes
    if (!this.writerReady) {
      await this.writerReadyPromise;
    }
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

  async _requestRead(action, sqlOrPayload, params) {
    if (this.memory) {
      // Delegates to _requestWrite, which now handles waiting for init
      return this._requestWrite(action, sqlOrPayload, params);
    }

    // 1. Wait for Global DB Init (Writer)
    if (!this.writerReady) {
      await this.writerReadyPromise;
    }

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

  /**
   * Executes a read task on a specific reader.
   * WAITS for the reader to be fully 'ready' before sending the message.
   */
  async _execRead(reader, task) {
    reader.busy = true;
    const id = Math.random();
    reader.queue.set(id, task);

    // Wait for this specific reader to finish booting
    if (!reader.ready) {
      try {
        await reader.readyPromise;
      } catch (err) {
        // If reader failed init, _cleanupWorker handles the queue rejection.
        // We just stop here to prevent posting to a dead worker.
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

  // Helper to drain the main queue into a specific reader (used when a specific reader becomes ready)
  _drainSpecificReaderQueue(reader) {
    if (!this.readQueue.length || reader.busy) return;
    // We grab one task from the global queue and assign it to this specific newly-ready reader
    const task = this.readQueue.shift();
    if (task) this._execRead(reader, task);
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
