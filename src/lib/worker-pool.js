/**
 * @file lib/worker-pool.js
 * @description A robust, thread-safe Worker Pool implementation for SQLite operations.
 *
 * Architecture:
 * 1. SingleWorkerClient:
 *    - Wraps a physical Node.js Worker thread.
 *    - Manages Request/Response correlation via a Map.
 *    - Implements a Streaming Protocol using Async Iterators.
 *    - Enforces Mutex locking (for Writers) to prevent race conditions.
 *
 * 2. MultiWorkerClient:
 *    - Manages a pool of SingleWorkerClients.
 *    - Implements Load Balancing ("Available First" strategy).
 *    - Implements Auto-Scaling (Min/Max workers).
 *    - Implements Task Queuing when the pool is saturated.
 */

const { Worker } = require("worker_threads");
const { EventEmitter } = require("events");
const { SqliteError } = require("better-sqlite3-multiple-ciphers");
const Mutex = require("./mutex");
const { createErrorByType } = require("./utils");

// =============================================================================
// INTERNAL SYMBOLS & CONSTANTS
// =============================================================================

/** Guard token to prevent direct instantiation of internal classes. */
const kSingleWorker = Symbol("SingleWorker");
/** Guard token to prevent direct instantiation of internal classes. */
const kMultiWorker = Symbol("MultiWorker");

// =============================================================================
// TYPE DEFINITIONS
// =============================================================================

/** @typedef {import('./worker').WorkerRequestPayload} WorkerRequestPayload */

/**
 * @typedef {Object} SerializedError
 * @property {string} message - Error message.
 * @property {string} code - Error code (e.g., 'SQLITE_CONSTRAINT').
 * @property {string} name - Error Type
 */

/**
 * @typedef {Object} WorkerMessage
 * @description The raw message structure received from the Worker Thread.
 * @property {string} requestId - Unique ID correlating to a pending promise.
 * @property {'success' | 'done' | 'error' | 'next' | 'log' | 'ready'} status - Outcome status.
 * @property {any} [data] - Payload for success, done, or next events.
 * @property {SerializedError} [error] - Error details (if status is error).
 * @property {boolean} [inTransaction] - Indicates if the statement is within a transaction.
 */

/**
 * @typedef {Object} LogMessage
 * @description The versbose log message received.
 * @property {string} requestId - Unique ID correlating to a pending promise.
 * @property {'log'} status - Outcome status.
 * @property {any} [data] - Message data.
 */

/**
 * @typedef {Object} StreamContext
 * @description Internal state for managing the Async Iterator bridge during streaming.
 * @property {any[]} buffer - Queue of received data chunks not yet yielded.
 * @property {Function|null} wake - Resolver function to unblock the iterator loop.
 * @property {Error|null} error - Terminal error received from worker.
 * @property {boolean} done - Terminal success flag.
 */

/**
 * @typedef {Object} TaskRequest
 * @description Represents a pending operation awaiting a response.
 * @property {Function} [resolve] - Promise resolve function (Standard requests).
 * @property {Function} [reject] - Promise reject function (Standard requests).
 * @property {boolean} isStreaming - Flag indicating if this is a streaming request.
 * @property {StreamContext} [streamCtx] - Context object (Streaming requests only).
 */

/**
 * @typedef {Object} QueueItem
 * @description Represents a task waiting in the MultiWorkerClient queue.
 * @property {'std' | 'stream'} type - The type of execution.
 * @property {any} data - The payload to send.
 * @property {Function} resolve - Outer promise resolver.
 * @property {Function} reject - Outer promise rejecter.
 */

/**
 * @typedef {Object} WorkerConfig
 * @property {string} filename - Filename of the database file.
 * @property {boolean} [readonly] - Flag indicating if the database should be opened in read-only mode.
 * @property {boolean} [fileMustExist] - Flag indicating if the database file must exist.
 * @property {number} [timeout] - Timeout in milliseconds for database operations.
 * @property {string} [nativeBinding] - Path to the native binding file.
 * @property {boolean} [verbose] - Flag indicating if verbose logging should be enabled.
 */

/**
 * Worker Client Options
 * @typedef {Object} WorkerClientOptions
 * @property {WorkerConfig & {mode: 'read' | 'write'}} workerData - Worker configuration data.
 * @property {boolean} [useMutex] - Flag indicating if mutex should be used.
 * @property {(message: LogMessage) => void | undefined} [onLog] - Callback function for logging messages.
 * @property {boolean} [autoRestart] - Flag indicating if worker should automatically restart on failure.
 */

// =============================================================================
// 1. SINGLE WORKER CLIENT
// =============================================================================

/**
 * @class SingleWorkerClient
 * @extends EventEmitter
 * @description Manages a single Node.js Worker thread instance.
 * Handles locking, request correlation, and the streaming protocol.
 */
class SingleWorkerClient extends EventEmitter {
  /**
   * PRIVATE CONSTRUCTOR.
   * @param {Symbol} token - Internal security token.
   * @param {string} workerPath - The absolute or relative path to the worker script.
   * @param {WorkerClientOptions} workerOptions - Options for the Worker instance.
   * @param {Function} initFn - Function to initialize the worker.
   * @throws {ReferenceError} If called directly without the correct token.
   */
  constructor(token, workerPath, workerOptions, initFn) {
    if (token !== kSingleWorker) {
      throw new ReferenceError(
        "Cannot instantiate SingleWorkerClient directly.",
      );
    }
    super();

    const { useMutex, onLog, ...nodeWorkerOpts } = workerOptions;

    /** @type {WorkerClientOptions} used for creating / restarting the Worker instance. */
    this.workerOptions = workerOptions;

    /** @type {string} The absolute or relative path to the worker script. */
    this.workerPath = workerPath;

    /** @type {Function} Function to initialize the worker. */
    this.initFn = initFn;

    /** @type {string} Generate a simple lightweight ID for the worker itself. */
    this.id = "w_" + Math.random().toString(36).slice(2, 7);

    /** @type {Worker} The Node.js Worker instance. */
    this.worker = new Worker(workerPath, nodeWorkerOpts);

    /**
     * Maps RequestID -> TaskRequest.
     * Stores pending promises/iterators waiting for worker responses.
     * @type {Map<string, TaskRequest>}
     */
    this.requestMap = new Map();

    /** @type {number} The count of currently processing tasks. */
    this.activeRequests = 0;

    // HIGH PERFORMANCE COUNTER
    // Sufficient for 9 quadrillion requests per worker instance
    /** @type {number} Sequence number for requests. */
    this.sequence = 0;

    /**
     * Indicates the worker is busy/reserved.
     * Set to TRUE when:
     * 1. A Mutex lock is held (e.g., Writer transaction).
     * 2. A Streaming operation is active (e.g., Backup/Iterator).
     * @type {boolean}
     */
    this.locked = false;

    /** @type {Mutex | null} If useMutex is true, we enforce sequential execution for this worker */
    this.mutex = useMutex ? new Mutex() : null;

    /** @type {(msg: any) => void} logger callback for database verbose logs */
    this.onLog = onLog || null;

    /** @type {boolean} Indicates whether the worker has been initialized */
    this.isInitialized = false;

    /** @type {any[]} History of sticky commands (Pragmas, Keys) to replay on respawn */
    this.stateHistory = [];

    /** @type {boolean} If true, worker will automatically restart on failure */
    this.autoRestart = workerOptions.autoRestart === true;

    /** @type {boolean} Indicates whether the worker is currently in a transaction */
    this.inTransaction = false;
  }

  /**
   * Static Factory Method.
   * Creates the instance and waits for the worker to be online.
   *
   * @param {string} workerPath
   * @param {WorkerClientOptions} workerOptions - Options for the Worker instance.
   * @param {(client: SingleWorkerClient) => Promise<void>} [initFn] - Async function(worker) => Promise<void>
   * @returns {Promise<SingleWorkerClient>}
   */
  static async create(workerPath, workerOptions = {}, initFn = null) {
    const client = new SingleWorkerClient(
      kSingleWorker,
      workerPath,
      workerOptions,
      initFn,
    );
    // This waits for the worker to be fully Online + Initialized
    await client._startWorker();
    return client;
  }

  /**
   * Spawns the worker and handles the Boot Sequence.
   * Used for BOTH initial creation and Auto-Restarts.
   * @returns {Promise<void>} Resolves when worker is ready to accept queries.
   */
  async _startWorker() {
    // 1. Cleanup old instance
    if (this.worker) {
      this.worker.removeAllListeners();
      this.worker.terminate().catch(() => {});
      this.worker = null;
    }
    // console.log(this.workerOptions);

    const { useMutex, onLog, autoRestart, ...nodeOpts } = this.workerOptions;
    this.worker = new Worker(this.workerPath, nodeOpts);

    // 2. Attach Permanent Listeners
    this.worker.on("message", this._handleMessage.bind(this));
    this.worker.on("error", this._handleError.bind(this));
    this.worker.on("exit", (code) => this._handleExit(code));

    // 3. BOOT SEQUENCE
    return new Promise((resolve, reject) => {
      let settled = false;

      const timeoutTimer = setTimeout(() => {
        if (!settled) {
          cleanup();
          this.worker.terminate();
          reject(new Error("Worker initialization timed out (10s)"));
        }
      }, 10000);

      const cleanup = () => {
        settled = true;
        clearTimeout(timeoutTimer);
        this.removeListener("ready", onReady);
        this.removeListener("boot_error", onBootError);
        this.worker.removeListener("exit", onBootExit);
      };

      const onReady = async () => {
        cleanup();
        try {
          // A. Run Custom Init Hook
          if (!this._isInitialized && this.initFn) {
            await this.initFn(this);
          }

          // Mark initialized to enable auto-restart for future crashes
          this._isInitialized = true;

          // B. Replay Sticky State
          if (this.stateHistory.length > 0) {
            for (const data of this.stateHistory) {
              const requestId = `replay_${Date.now()}_${Math.random()}`;
              this.worker.postMessage({ requestId, data });
            }
          }
          resolve();
        } catch (err) {
          this.worker.terminate();
          reject(err);
        }
      };

      const onBootError = (err) => {
        cleanup();
        this.worker.terminate();
        reject(err);
      };

      const onBootExit = (code) => {
        cleanup();
        reject(new Error(`Worker exited with code ${code} during boot`));
      };

      this.on("ready", onReady);
      this.on("boot_error", onBootError);
      // Listen for exit directly to catch startup crashes
      this.worker.on("exit", onBootExit);
    });
  }

  // ===========================================================================
  // LOCK MANAGEMENT
  // ===========================================================================

  /**
   * Checks if the internal mutex is currently locked.
   * Used by Database/Connection to check status during cleanup.
   */
  isLocked() {
    return this.mutex ? this.mutex.isLocked() : false;
  }

  /**
   * Manually locks the worker.
   * 1. Marks it as busy/reserved (for Load Balancer).
   * 2. Acquires the Mutex (if enabled) to block other .execute() calls.
   */
  async lock() {
    this.locked = true;
    if (this.mutex) {
      await this.mutex.acquire();
    }
  }

  /**
   * Unlocks the worker.
   * 1. Releases the Mutex.
   * 2. Marks it as available.
   */
  unlock() {
    if (this.mutex) {
      this.mutex.release();
    }
    this.locked = false;
    // Notify listeners (MultiWorkerClient) that this worker is available
    this.emit("unlock");
  }

  /**
   * Check availability.
   * Workers are idle only if they have no active requests AND are not manually locked.
   */
  isIdle() {
    return this.activeRequests === 0 && !this.locked;
  }

  // ===========================================================================
  // STANDARD EXECUTION (Request -> Single Response)
  // ===========================================================================

  /**
   * Sends a payload to the worker and waits for the response.
   * STANDARD EXECUTION (Auto-Locking).
   * Used for atomic, one-off operations (e.g. db.prepare().run()).
   * Acquires the mutex, runs the task, releases the mutex
   * @param {any} data - The data payload to send.
   * @param {boolean} [sticky=false] - If true, replayed on worker restart.
   * @returns {Promise<any>} Resolves with the worker's result.
   */
  async execute(data, sticky = false) {
    if (sticky) {
      this.stateHistory.push(data);
    }

    // Acquire Lock (if enabled)
    if (this.mutex) {
      await this.mutex.acquire();
    }

    try {
      return await this.noLockExecute(data);
    } finally {
      // Release Lock (if enabled)
      // This allows the next queued task to proceed.
      if (this.mutex) {
        this.mutex.release();
      }
    }
  }

  /**
   * Sends a payload to the worker and waits for the response.
   * RAW EXECUTION (No Internal Locking).
   * Used when the lock is already held externally (e.g. Transactions, Streams).
   * WARNING: Calling this on a Writer without holding the lock can cause race conditions.
   * @param {any} data - The data payload to send.
   * @returns {Promise<any>} Resolves with the worker's result.
   */
  async noLockExecute(data) {
    // Mark as busy immediately
    this.activeRequests++;

    return new Promise((resolve, reject) => {
      // OPTIMIZATION: Use incrementing integer (Base36 string is shorter/cleaner)
      // This is ~100x faster than generating a UUID
      const requestId = `${this.id}_${this.sequence++}`;

      // Store the promise controllers
      this.requestMap.set(requestId, { resolve, reject, isStreaming: false });

      // Send to the actual thread
      this.worker.postMessage({ requestId, data });
    });
  }

  // ===========================================================================
  // STREAMING EXECUTION (Request -> Multiple 'next' events -> Final Response)
  // ===========================================================================

  /**
   * Executes a streaming request and yields results as they arrive.
   *
   * @description
   * 1. Acquires the Mutex (if enabled).
   * 2. Sets logical lock to TRUE (prevents other tasks from being scheduled).
   * 3. Yields results from the worker via an Async Iterator.
   * 4. Releases lock ONLY after iteration completes, errors, or is broken.
   *
   * @param {any} data - The payload.
   * @returns {AsyncGenerator<any, void, unknown>}
   */
  async *streamExecute(data) {
    await this.lock();
    try {
      // Delegate to internal generator
      yield* this.noLockStreamExecute(data);
    } finally {
      // Ensure lock is released even if consumer calls 'break' in their loop
      this.unlock();
    }
  }

  /**
   * Internal streaming generator logic.
   * Implements a Pull-based system where the iterator pauses until data arrives.
   * @private
   * @param {any} data
   */
  async *noLockStreamExecute(data) {
    this.activeRequests++;
    const requestId = `${this.id}_${this.sequence++}`;

    /** @type {StreamContext} */
    const ctx = {
      buffer: [],
      wake: null, // Function to resolve the 'await' below
      error: null,
      done: false,
    };

    // Register streaming task
    this.requestMap.set(requestId, { isStreaming: true, streamCtx: ctx });

    this.worker.postMessage({ requestId, data });

    try {
      while (true) {
        // 1. Flush Buffer: If we have data, yield immediately
        if (ctx.buffer.length > 0) {
          yield ctx.buffer.shift();
          continue;
        }

        // 2. Check Terminal States
        if (ctx.error) throw ctx.error;
        if (ctx.done) break;

        // 3. Suspend: Wait for _handleMessage to call ctx.wake()
        await new Promise((resolve) => {
          ctx.wake = resolve;
        });
      }
    } finally {
      // Cleanup when loop finishes or crashes
      if (this.requestMap.has(requestId)) {
        this.requestMap.delete(requestId);
        this.activeRequests--;
      }
    }
  }

  // ===========================================================================
  // MESSAGE HANDLING & INTERNALS
  // ===========================================================================

  /**
   * Internal handler for incoming messages from the worker thread.
   * Dispatches results to the correct Promise or Stream Context.
   * @private
   * @param {WorkerMessage} message - The worker message.
   */
  _handleMessage({ requestId, status, data, error, inTransaction }) {
    // If the worker reported a transaction state, update our local tracker.
    if (typeof inTransaction === "boolean") {
      this.inTransaction = inTransaction;
    }

    // --- READY ---
    if (status === "ready") {
      this.emit("ready");
      return;
    }
    // --- BOOT ERROR ---
    if (status === "boot_error") {
      this.emit("boot_error", new Error(data));
      return;
    }
    // --- SETUP ERROR ---
    if (status === "setup_error") {
      this.emit("error", new Error(data));
      return;
    }
    // --- LOGGING (Global, not strictly tied to request state) ---
    if (status === "log") {
      if (typeof this.onLog === "function") {
        this.onLog(data);
      }
      return;
    }

    // --- Other logs
    if (!this.requestMap.has(requestId)) return;
    const task = this.requestMap.get(requestId);

    // --- A. STREAMING RESPONSE LOGIC ---
    if (task.isStreaming) {
      const ctx = task.streamCtx;

      if (status === "next") {
        // Intermediate chunk: Buffer it and wake the iterator
        ctx.buffer.push(data);
        if (ctx.wake) {
          ctx.wake();
          ctx.wake = null;
        }
      } else if (status === "done") {
        // Completion: Mark done, buffer optional final data, wake iterator
        ctx.done = true;
        if (data !== undefined) ctx.buffer.push(data);
        if (ctx.wake) {
          ctx.wake();
          ctx.wake = null;
        }
      } else if (status === "error" || error) {
        // Error: Store error and wake iterator to throw it
        ctx.error = createErrorByType(error);
        if (ctx.wake) {
          ctx.wake();
          ctx.wake = null;
        }
      } else if (status === "success") {
        // Protocol Violation: Stream expects 'done', not 'success'
        ctx.error = new SqliteError(
          "Protocol Violation: Received 'success' for stream. Expected 'done'.",
          "SQLITE_INTERNAL",
        );
        if (ctx.wake) {
          ctx.wake();
          ctx.wake = null;
        }
      }
      return;
    }

    // --- B. STANDARD RESPONSE LOGIC ---
    // Remove from map immediately as the request is finished
    this.requestMap.delete(requestId);
    this.activeRequests--;

    if (status === "success") {
      task.resolve(data);
    } else if (status === "error" || error) {
      const finalError = createErrorByType(error);
      finalError.__data = data;
      task.reject(finalError);
    } else if (status === "next" || status === "done") {
      // Protocol Violation: Standard request shouldn't get stream events
      task.reject(
        new SqliteError(
          `Protocol Violation: Received '${status}' for standard request.`,
          "SQLITE_INTERNAL",
        ),
      );
    } else {
      task.reject(
        new SqliteError(`Unknown status: ${status}`, "SQLITE_INTERNAL"),
      );
    }
  }

  /**
   * Handles worker crashes/errors. Rejects all pending promises.
   * @private
   * @param {Error} err
   */
  _handleError(err) {
    console.error(`[better-sqlite3-pool] Worker ${this.id} error:`, err);
    // Flush current requests with the specific error
    this._flushRequests(createErrorByType(err));
  }

  /**
   * Handles unexpected worker termination.
   * @private
   * @param {number} code - Exit code.
   */
  _handleExit(code) {
    // 0 = Intentional close (usually)
    if (code === 0) {
      this._flushRequests(new Error("Worker closed cleanly"));
      this.emit("exit", code);
      return;
    }

    console.warn(
      `[better-sqlite3-pool] Worker ${this.id} crashed (code ${code}).`,
    );

    // 1. Fail all pending requests immediately
    // If we don't do this, clients waiting on Promises will hang until timeout
    this._flushRequests(
      new SqliteError(`Worker crashed with code ${code}`, "SQLITE_INTERNAL"),
    );

    // 2. Restart Logic
    if (this.autoRestart) {
      // SCENARIO A: WRITER (Self-Healing)
      this.emit("restart"); // Notify DB to clear transaction flags

      this._startWorker().catch((err) => {
        console.error(
          "[better-sqlite3-pool] Fatal: Failed to respawn worker:",
          err,
        );
        this.emit("error", err); // Global error, DB is likely unusable now
      });
    } else {
      // SCENARIO B: READER POOL (Managed)
      this.emit("exit", code);
    }
  }

  /**
   * Flushes all pending requests with an error.
   * @private
   * @param {Error} err
   */
  _flushRequests(err) {
    this.activeRequests = 0;
    for (const [_, task] of this.requestMap) {
      task.reject(err);
    }
    this.requestMap.clear();
    this.activeRequests = 0;
  }

  /**
   * Gracefully terminates the underlying worker thread.
   * @returns {Promise<number>} Exit code.
   */
  async terminate() {
    if (this.worker) {
      // 1. Prevent Auto-Restart:
      // Remove the exit listener so _handleExit doesn't run when we kill it.
      this.worker.removeAllListeners("exit");
      this.worker.removeAllListeners("error");

      // 2. Reject pending work:
      this._flushRequests(new Error("Worker terminated"));

      // 3. PREVENT AUTO-RESTART
      // We are shutting down. If the writer crashes during this process,
      // we do NOT want it to respawn.
      this.autoRestart = false;

      // 3. Kill thread:
      return this.worker.terminate();
    }
    return Promise.resolve();
  }
}

// =======================================================================
// 2. MULTI WORKER MANAGER (POOL)
// =======================================================================

/**
 * Manages a pool of SingleWorkerClients.
 * Handles auto-scaling, load balancing, queueing, and direct addressing.
 */
class MultiWorkerClient extends EventEmitter {
  /**
   * PRIVATE CONSTRUCTOR.
   * @param {Symbol} token
   * @param {string} workerPath - Path to the worker file.
   * @param {number} [minWorkers=1] - Minimum number of workers to keep alive.
   * @param {number} [maxWorkers=2] - Maximum number of workers allowed.
   * @param {WorkerClientOptions} [WorkerOptions] - Options for the worker.
   * @param {Function} [initFn] - Function executed on every new worker (init/scale).
   * @throws {ReferenceError} If called directly.
   */
  constructor(
    token,
    workerPath,
    minWorkers = 1,
    maxWorkers = 2,
    workerOptions = {},
    initFn = null,
  ) {
    if (token !== kMultiWorker) {
      throw new ReferenceError(
        "MultiWorkerClient is not constructable. Use MultiWorkerClient.create() instead.",
      );
    }
    super();

    /** @type {string} Worker path. */
    this.workerPath = workerPath;

    /** @type {WorkerOptions} Worker options. */
    this.workerOptions = workerOptions
      ? { ...workerOptions, autoRestart: false }
      : {};

    /** @type {number} Minimum number of workers to keep alive. */
    this.minWorkers = minWorkers;

    /** @type {number} Maximum number of workers allowed. */
    this.maxWorkers = maxWorkers;

    /** @type {Function} Function executed on every new worker (init/scale). */
    this.initFn = initFn;

    /** @type {SingleWorkerClient[]} Array of active worker wrappers. */
    this.workers = [];

    /** @type {Array<{data: any, resolve: Function, reject: Function}>} Queue for pending tasks. */
    this.queue = [];

    /** @type {Function[]} Queue for clients waiting for a locked worker to free up */
    this.clientsWaitQueue = [];

    /** @type {number} Track workers currently being spawned to prevent over-scaling */
    this.spawningCount = 0;

    /** @type {Array<{any}>} Broadcast History */
    this.broadcastHistory = [];

    /** @type {number} Prevent spawning storms */
    this.pendingSpawns = 0;

    /** @type {boolean} Flag to prevent resurrection during shutdown */
    this.isClosing = false;
  }

  // --- Public API ---

  /**
   * Static Factory Method.
   * Initializes the pool and waits for MIN workers to be ready.
   *
   * @param {string} workerPath - Path to the worker file.
   * @param {number} [minWorkers=1] - Minimum number of workers to keep alive.
   * @param {number} [maxWorkers=2] - Maximum number of workers allowed.
   * @param {WorkerOptions | {useMutex: boolean}} [workerOptions={}] - Options for the worker.
   * @param {Function} [initFn] - Function executed on every new worker (init/scale).
   * @returns {Promise<MultiWorkerClient>}
   */
  static async create(
    workerPath,
    minWorkers = 1,
    maxWorkers = 2,
    workerOptions = {},
    initFn = null,
  ) {
    // PASSING THE SECRET TOKEN
    const pool = new MultiWorkerClient(
      kMultiWorker,
      workerPath,
      minWorkers,
      maxWorkers,
      workerOptions,
      initFn,
    );

    // Initialize min workers using the SingleWorker factory (async)
    const initPromises = [];
    for (let i = 0; i < minWorkers; i++) {
      initPromises.push(pool._spawnWorker());
    }

    await Promise.all(initPromises);

    return pool;
  }

  // --- RESIZE (SCALE UP ONLY) ---
  /**
   * Increases the capacity of the worker pool.
   * Throws TypeError if attempting to decrease limits.
   *
   * @param {number} newMin - New minimum (must be >= current min)
   * @param {number} newMax - New maximum (must be >= current max)
   * @returns {Promise<string[]>} Array of newly created Worker IDs
   */
  async resize(newMin, newMax) {
    // 1. Validate Consistency
    if (newMin > newMax) {
      throw new Error(
        `Invalid configuration: min (${newMin}) cannot be greater than max (${newMax})`,
      );
    }

    // 2. Validate Scale Direction (UP Only)
    if (newMin < this.minWorkers) {
      throw new TypeError(
        `Cannot scale down: newMin (${newMin}) is less than current min (${this.minWorkers})`,
      );
    }
    if (newMax < this.maxWorkers) {
      throw new TypeError(
        `Cannot scale down: newMax (${newMax}) is less than current max (${this.maxWorkers})`,
      );
    }

    console.log(
      `[Pool] Resizing... Current Config: ${this.minWorkers}/${this.maxWorkers} -> New Config: ${newMin}/${newMax}`,
    );

    this.minWorkers = newMin;
    this.maxWorkers = newMax;

    const newWorkerIds = [];

    // 3. SCALE UP LOGIC
    // If the new minimum requires more workers than we currently have, spawn them.
    if (this.workers.length < newMin) {
      const countToAdd = newMin - this.workers.length;
      console.log(
        `[Pool] Scaling UP: Spawning ${countToAdd} new workers to meet minimum...`,
      );

      const promises = [];
      for (let i = 0; i < countToAdd; i++) {
        // Use async create to ensure they are fully online
        promises.push(this._spawnWorker());
      }

      const newWorkers = await Promise.all(promises);

      // Add to pool and collect IDs
      newWorkers.forEach((w) => {
        newWorkerIds.push(w.id);
      });

      console.log(
        `[Pool] Scale UP Complete. Total Workers: ${this.workers.length}`,
      );
    }

    // Process queue in case the increased limits allow pending tasks to run
    this._processQueue();
    return newWorkerIds;
  }

  /**
   * Executes a task on a worker.
   *
   * Strategy:
   * 1. If `workerId` is provided: Direct Addressing (Sticky/Streaming).
   * 2. If `workerId` is null: Load Balancing (Available First -> Queue).
   *
   * @param {any} data - The payload to process.
   * @param {string|null} [workerId=null] - Optional ID to force execution on a specific worker.
   * @returns {Promise<any>} The result from the worker.
   */
  execute(data, workerId = null) {
    // STRATEGY 1: Direct Addressing
    if (workerId) {
      const worker = this.workers.find((w) => w.id === workerId);
      if (worker) {
        return worker.execute(data);
      }
      return Promise.reject(
        new Error(`Worker ID ${workerId} not found or inactive.`),
      );
    }

    // STRATEGY 2: Load Balancing (Queued)
    return new Promise((resolve, reject) => {
      this.queue.push({ data, resolve, reject });
      this._processQueue();
    });
  }

  /**
   * Schedule a streaming execution.
   * @param {any} data
   * @param {string|null} [workerId] - Optional direct addressing.
   *  @returns {Promise<AsyncGenerator<any, void, unknown>>} Resolves to the Iterator.
   */
  streamExecute(data, workerId = null, onNext) {
    if (typeof onNext !== "function") {
      return Promise.reject(
        new SqliteError("streamExecute requires callback", "SQLITE_MISUSE"),
      );
    }

    // 1. Direct Addressing
    if (workerId) {
      const worker = this.workers.find((x) => x.id === workerId);
      return worker
        ? Promise.reject(worker.streamExecute(data, onNext))
        : Promise.reject(new SqliteError("Worker not found", "SQLITE_ERROR"));
    }

    // 2. Load Balancing (Queue)
    return new Promise((resolve, reject) => {
      this.queue.push({ type: "stream", data, resolve, reject, onNext });
      this._processQueue();
    });
  }

  /**
   * Sends the payload to ALL currently active workers.
   * Useful for cache clearing, configuration updates, or health checks.
   *
   * @param {any} data
   * @param {boolean} sticky - Whether to use sticky routing or not.
   * @returns {Promise<any[]>} Array of results from all workers
   */
  broadcast(data, sticky = false) {
    if (sticky) {
      this.broadcastHistory.push(data);
    }
    if (this.workers.length === 0) {
      return Promise.resolve([]);
    }

    // Map every currently active worker to an execution promise
    // This bypasses the queue and sends immediately to the threads
    const promises = this.workers.map((worker) => worker.execute(data));

    return Promise.all(promises);
  }

  /**
   * Sends data ONLY to the specific worker IDs provided.
   *
   * @param {string[]} workerIds - Array of Worker IDs to target.
   * @param {any} data - The payload to send.
   * @returns {Promise<any[]>} Resolves with an array of results from those workers.
   */
  specifiedBroadcast(workerIds, data) {
    if (!Array.isArray(workerIds)) {
      return Promise.reject(new Error("workerIds must be an array"));
    }

    const targetWorkers = [];
    const missingIds = [];

    // 1. Validation Phase: Check existence of ALL IDs
    for (const id of workerIds) {
      const worker = this.workers.find((w) => w.id === id);
      if (!worker) {
        missingIds.push(id);
      } else {
        targetWorkers.push(worker);
      }
    }

    // 2. Abort Phase: If any are missing, fail immediately
    if (missingIds.length > 0) {
      return Promise.reject(
        new Error(
          `Operation Aborted. The following Worker IDs were not found: ${missingIds.join(", ")}`,
        ),
      );
    }

    // 3. Execution Phase: All IDs valid, proceed safely
    const promises = targetWorkers.map((worker) => worker.execute(data));
    return Promise.all(promises);
  }

  /**
   * Retrieves a Worker to lock onto for streaming/sticky sessions.
   *
   * Selection Logic:
   * 1. **Free Worker**: If a worker has 0 requests, return it.
   * 2. **Scale Up**: If no free worker & pool not full, create new one.
   * 3. **Random Fallback**: If full and busy, pick a random worker to distribute load.
   *
   * @returns {Promise<SingleWorkerClient>} Available worker client.
   */
  async getWorker() {
    // 1. Try to find a completely idle worker
    const freeWorker = this.workers.find((w) => w.isIdle());
    if (freeWorker) return freeWorker;

    // 2. If no free worker, but we have capacity, spawn a new one
    if (this.workers.length + this.pendingSpawns < this.maxWorkers) {
      return await this._spawnWorker();
    }

    // 3. Try Busy but Unlocked (Load Balancing)
    // We cannot return a locked worker.
    // Select a RANDOM worker (Load distribution for saturated pools)
    // We avoid always picking index 0 to prevent "hot spotting" on the first worker.
    const available = this.workers.filter((w) => !w.locked);
    if (available.length > 0) {
      return available[Math.floor(Math.random() * available.length)];
    }

    // 4. All Locked - Wait in Queue until 'unlock' event fires
    return new Promise((resolve) => {
      this.clientsWaitQueue.push(resolve);
    });
  }

  /**
   * Terminates all workers in the pool.
   * @returns {Promise<void>}
   */
  async close() {
    this.isClosing = true;
    await Promise.all(this.workers.map((w) => w.terminate()));
    this.workers = [];
  }

  // --- Internal Logic ---

  /**
   * Creates a new worker and adds it to the pool.
   * @private
   * @returns {Promise<SingleWorkerClient>} The new worker instance.
   */
  async _spawnWorker() {
    this.pendingSpawns++;
    try {
      // 1. Create (waits for InitFn / DB Open)
      const worker = await SingleWorkerClient.create(
        this.workerPath,
        this.workerOptions,
        this.initFn,
      );

      // Race Condition Handling
      // If the pool was closed while this worker was spinning up, kill it immediately.
      // Otherwise, it becomes a zombie thread that prevents the process from exiting.
      if (this.isClosing) {
        await worker.terminate();
        return null;
      }

      // 2. Replay History (Sticky Broadcasts)
      // This ensures new workers have the correct PRAGMAs
      for (const msg of this.broadcastHistory) {
        await worker.execute(msg);
      }

      worker.once("exit", () => this._handleWorkerExit(worker));

      // Listen for unlocks to notify waiting getWorker callers
      worker.on("unlock", () => this._notifyWaiters());

      this.workers.push(worker);
      return worker;
    } finally {
      this.pendingSpawns--;
    }
  }

  /**
   * Handles unexpected worker death (Auto-Healing)
   * @param {SingleWorkerClient} deadWorker The dead worker instance.
   */
  _handleWorkerExit(deadWorker) {
    // 1. Remove dead worker from array
    this.workers = this.workers.filter((w) => w !== deadWorker);

    // 2. If shutting down, do nothing
    if (this.isClosing) return;

    // 3. Check if we need to resurrect to meet minimums
    if (this.workers.length + this.pendingSpawns < this.minWorkers) {
      // Add a small delay to prevent rapid-fire restart loops if error is persistent
      setTimeout(() => {
        this._spawnWorker()
          .then(() => {
            // New worker is unlocked, notify waiters
            this._notifyWaiters();
            this._processQueue();
          })
          .catch(console.error);
      }, 100);
    }
  }

  /**
   * Notifies waiters in getWorkerQueue when a worker becomes unlocked.
   */
  _notifyWaiters() {
    if (this.clientsWaitQueue.length === 0) return;

    // Find unlocked workers
    const available = this.workers.filter((w) => !w.locked);

    while (available.length > 0 && this.clientsWaitQueue.length > 0) {
      // Pick a random available worker
      const idx = Math.floor(Math.random() * available.length);
      const worker = available.splice(idx, 1)[0];

      const resolve = this.clientsWaitQueue.shift();
      if (resolve) resolve(worker);
    }
  }

  /**
   * Processes the task queue.
   * Uses "Available First" strategy: scans the list for the first free worker.
   * If none found, tries to scale up.
   * @private
   */
  _processQueue() {
    // LOOP: Keep assigning tasks until we run out of Tasks OR Workers
    // This removes the need for recursive nextTicks for every single item
    while (this.queue.length > 0) {
      // 1. Find the first worker with 0 active requests (Available First)
      let selectedWorker = this.workers.find((w) => w.isIdle());

      if (!selectedWorker) {
        // 2. If no one is free, and we haven't hit the limit, spawn a new one
        if (this.workers.length + this.pendingSpawns < this.maxWorkers) {
          this._spawnWorker().then(() =>
            setImmediate(() => this._processQueue()),
          );
        }

        // 3. If NO worker is available (Pool saturated), stop the loop.
        // We must wait for a running task to finish and trigger .finally()
        break;
      }

      // 4. Assign the task
      const task = this.queue.shift(); // Dequeue task

      selectedWorker
        .execute(task.data)
        .then(task.resolve)
        .catch(task.reject)
        .finally(() => {
          // KEY CHANGE: Use setImmediate logic here.
          // When a task finishes, we check the queue again.
          // setImmediate yields to the Event Loop, allowing I/O to happen.
          setImmediate(() => this._processQueue());
        });
    }
  }
}

module.exports = {
  SingleWorkerClient,
  MultiWorkerClient,
};
