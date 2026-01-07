/**
 * @file lib/worker-pool.js
 * @description A robust Worker Thread Pool implementation for Node.js.
 * Features:
 *  - Request/Response mapping using Unique IDs.
 *  - Auto-scaling (Min/Max workers).
 *  - "Available First" Load Balancing strategy.
 *  - Queue system for request buffering when saturated.
 *  - Direct Addressing (Sticky Sessions) for streaming support.
 *  - Random fallback selection when pool is full.
 */

const { Worker } = require("worker_threads");
const { EventEmitter } = require("events");
const { SqliteError } = require("better-sqlite3-multiple-ciphers");
const Mutex = require("./mutex");

// =======================================================================
// PRIVATE TOKENS (Internal access only)
// =======================================================================
const kSingleWorker = Symbol("SingleWorker");
const kMultiWorker = Symbol("MultiWorker");

// =======================================================================
// 1. SINGLE WORKER WRAPPER
// =======================================================================

/**
 * @typedef {Object} WorkerMessage
 * @property {string} requestId - Correlates to the request.
 * @property {'success' | 'error'} status - Outcome status.
 * @property {any} [data] - Success payload.
 * @property {Object} [error] - Error details.
 * @property {string} [error.message]
 * @property {string} [error.code]
 * @property {string} [streamId]
 */

/**
 * Represents a single Worker Thread instance.
 * Manages the specific "Request Map" for this thread to correlate
 * asynchronous responses back to their original promises.
 */
class SingleWorkerClient extends EventEmitter {
  /**
   * PRIVATE CONSTRUCTOR.
   * @param {Symbol} token - Internal security token.
   * @param {string} workerPath - The absolute or relative path to the worker script.
   * @param {WorkerOptions & {useMutex: boolean}} workerOptions - Options for the Worker instance.
   * @throws {ReferenceError} If called directly without the correct token.
   */
  constructor(token, workerPath, workerOptions) {
    if (token !== kSingleWorker) {
      throw new ReferenceError(
        "Cannot instantiate SingleWorkerClient directly.",
      );
    }
    super();

    const { useMutex, ...nodeWorkerOpts } = workerOptions;

    /** @type {string} Generate a simple lightweight ID for the worker itself. */
    this.id = "w_" + Math.random().toString(36).slice(2, 7);

    /** @type {Worker} The Node.js Worker instance. */
    this.worker = new Worker(workerPath, nodeWorkerOpts);

    /**
     * @type {Map<string, {resolve: Function, reject: Function}>}
     * Stores pending promises mapped by Request ID.
     */
    this.requestMap = new Map();

    /** @type {number} The count of currently processing tasks. */
    this.activeRequests = 0;

    // HIGH PERFORMANCE COUNTER
    // Sufficient for 9 quadrillion requests per worker instance
    /** @type {number} Sequence number for requests. */
    this.sequence = 0;

    /** @type {boolean} Availability status */
    this.pinned = false;

    /** @type {Mutex | null} If useMutex is true, we enforce sequential execution for this worker */
    this.mutex = useMutex ? new Mutex() : null;

    // Bind event handlers to maintain 'this' context
    this.worker.on("message", this._handleMessage.bind(this));
    this.worker.on("error", this._handleError.bind(this));
    this.worker.on("exit", this._handleExit.bind(this));
  }
  /**
   * Static Factory Method.
   * Creates the instance and waits for the worker to be online.
   *
   * @param {string} workerPath
   * @param {WorkerOptions} workerOptions - Options for the Worker instance.
   * @param {Function} [initFn] - Async function(worker) => Promise<void>
   * @returns {Promise<SingleWorkerClient>}
   */
  static create(workerPath, workerOptions = {}, initFn = null) {
    return new Promise((resolve, reject) => {
      // PASSING THE SECRET TOKEN
      const client = new SingleWorkerClient(
        kSingleWorker,
        workerPath,
        workerOptions,
      );

      // Helper to finalize creation
      const finalize = async () => {
        try {
          if (initFn) {
            // Run the custom initialization logic
            await initFn(client);
          }
          resolve(client);
        } catch (err) {
          // If init fails, terminate and reject
          client.terminate();
          reject(err);
        }
      };

      if (client.worker.threadId > 0) {
        return finalize();
      }

      const cleanup = () => {
        client.worker.removeListener("online", onOnline);
        client.worker.removeListener("error", onError);
        client.worker.removeListener("exit", onExit);
      };

      const onOnline = () => {
        cleanup();
        finalize();
      };

      const onError = (err) => {
        cleanup();
        reject(err);
      };

      const onExit = (code) => {
        cleanup();
        reject(new Error(`Worker exited with code ${code} during boot`));
      };

      client.worker.on("online", onOnline);
      client.worker.on("error", onError);
      client.worker.on("exit", onExit);
    });
  }

  pin() {
    this.pinned = true;
  }

  unpin() {
    this.pinned = false;
  }

  /**
   * Checks if the worker is truly available for pruning or load balancing.
   */
  isIdle() {
    return this.activeRequests === 0 && !this.pinned;
  }

  /**
   * Sends a payload to the worker and waits for the response.
   * @param {any} data - The data payload to send.
   * @returns {Promise<any>} Resolves with the worker's result.
   */
  async execute(data) {
    // Acquire Lock (if enabled)
    if (this.mutex) {
      await this.mutex.acquire();
    }

    // Mark as busy immediately
    this.activeRequests++;

    try {
      return await new Promise((resolve, reject) => {
        // OPTIMIZATION: Use incrementing integer (Base36 string is shorter/cleaner)
        // This is ~100x faster than generating a UUID
        const requestId = `${this.id}_${this.sequence++}`;

        // Store the promise controllers
        this.requestMap.set(requestId, { resolve, reject });

        // Send to the actual thread
        this.worker.postMessage({ requestId, data });
      });
    } finally {
      // Release Lock (if enabled)
      // This allows the next queued task to proceed.
      if (this.mutex) {
        this.mutex.release();
      }
    }
  }

  /**
   * Internal handler for incoming messages from the worker thread.
   * @private
   * @param {WorkerMessage} message - The message object { requestId, data, error, status }.
   */
  _handleMessage({ requestId, data, error }) {
    if (this.requestMap.has(requestId)) {
      const { resolve, reject } = this.requestMap.get(requestId);

      // Cleanup map and state
      this.requestMap.delete(requestId);
      this.activeRequests--;

      // Settle the promise
      if (error) {
        reject(new SqliteError(error.message, error.code));
      } else {
        resolve(data);
      }
    }
  }

  /**
   * Handles worker crashes/errors. Rejects all pending promises.
   * @private
   * @param {Error} err
   */
  _handleError(err) {
    console.log("err hand", err);
    this._flushRequests(err);
  }

  /**
   * Handles unexpected worker termination.
   * @private
   * @param {number} code - Exit code.
   */
  _handleExit(code) {
    const err = new Error(`Worker stopped with exit code ${code}`);
    this._flushRequests(err);
    this.emit("exit", code);
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
  }

  /**
   * Gracefully terminates the underlying worker thread.
   * @returns {Promise<number>} Exit code.
   */
  terminate() {
    return this.worker.terminate();
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
   * @param {WorkerOptions | & {useMutex: boolean}} [WorkerOptions] - Options for the worker.
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
    this.workerOptions = workerOptions;

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

    const readyWorkers = await Promise.all(initPromises);
    pool.workers.push(...readyWorkers);

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
        this.workers.push(w);
        newWorkerIds.push(w.id);
      });

      console.log(
        `[Pool] Scale UP Complete. Total Workers: ${this.workers.length}`,
      );
    }

    // Process queue in case the increased limits allow pending tasks to run
    this._processQueue();
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
      const newWorker = await this._spawnWorker();
      return newWorker;
    }

    // 3. Fallback: Select a RANDOM worker (Load distribution for saturated pools)
    // We avoid always picking index 0 to prevent "hot spotting" on the first worker.
    const randomIndex = Math.floor(Math.random() * this.workers.length);
    return this.workers[randomIndex];
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

      // 2. Replay History (Sticky Broadcasts)
      // This ensures new workers have the correct PRAGMAs
      for (const msg of this.broadcastHistory) {
        await worker.execute(msg);
      }

      worker.once("exit", () => this._handleWorkerExit(worker));

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
          .then(() => this._processQueue())
          .catch(console.error);
      }, 100);
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
