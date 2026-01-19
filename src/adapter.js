/**
 * @file lib/adapter.js
 * @description A compatibility adapter that makes 'better-sqlite3-pool' look like the legacy 'sqlite3' driver.
 * This allows ORMs like TypeORM to use this connection pool transparently while maintaining
 * the thread-safety and performance benefits of the worker pool.
 */

const { Database } = require("./lib/database");

// =============================================================================
// TYPE DEFINITIONS
// =============================================================================

/**
 * @typedef {Function} SqliteCallback
 * @description Standard node-sqlite3 callback signature.
 * @param {Error|null} err - Error object if operation failed, null otherwise.
 * @param {any} [rows] - Result rows (for read operations).
 * @this {RunContext} - The context (`this`) contains metadata for write operations.
 */

/**
 * @typedef {Object} RunContext
 * @description The `this` context bound to callbacks for write operations.
 * @property {number|bigint} [lastID] - The ROWID of the last inserted row.
 * @property {number} [changes] - The number of rows affected by the query.
 */

// =============================================================================
// GLOBAL STATE
// =============================================================================

/** * @type {boolean}
 * Module-level flag to toggle verbose logging.
 * This mimics the behavior of `require('sqlite3').verbose()`.
 */
let verboseMode = false;

// =============================================================================
// ADAPTER CLASS
// =============================================================================

/**
 * @class SQLite3Adapter
 * @description Wraps the Promise-based Database pool to expose a standard `sqlite3` callback API.
 * It handles:
 * 1. Callback <-> Promise conversion.
 * 2. Manual transaction locking (since legacy drivers expect serialized connections).
 * 3. Verbose logging injection.
 */
class SQLite3Adapter {
  /**
   * Create a new Adapter instance.
   * Mirrors `new sqlite3.Database(filename, [mode], [callback])`
   */
  constructor(filename, mode, callback) {
    // 1. Argument Shifting
    let cb = callback;
    if (typeof mode === "function") {
      cb = mode;
      mode = null;
    }

    // 2. Internal State
    this.db = null; // The Pool
    this.conn = null; // Exclusive Session (if inside transaction)

    // --- SCHEDULER STATE ---
    this.lastWrite = Promise.resolve(); // Barrier: The last write operation
    this.activeReads = new Set(); // Active Reads: Current reads running in parallel
    this.queue = []; // Init Queue: Operations waiting for DB open
    this.initError = null;

    // 3. Logger
    this.logger = verboseMode ? console.log : null;

    // 4. Async Initialization
    Database.create(filename, { verbose: this.logger })
      .then((pool) => {
        this.db = pool;
        if (cb) cb(null);
        this._flushQueue();
      })
      .catch((err) => {
        this.initError = err;
        if (cb) cb(err);
        this._flushQueue();
      });
  }

  /**
   * Execute a query that does NOT return rows (INSERT, UPDATE, DELETE).
   */
  run(sql, ...args) {
    const { params, callback } = this._parseArgs(args);
    this._schedule("run", sql, params, callback);
    return this;
  }

  /**
   * Execute a query that returns all matching rows (SELECT).
   */
  all(sql, ...args) {
    const { params, callback } = this._parseArgs(args);
    this._schedule("all", sql, params, callback);
    return this;
  }

  /**
   * Execute a query that returns the first row.
   */
  get(sql, ...args) {
    const { params, callback } = this._parseArgs(args);
    this._schedule("get", sql, params, callback);
    return this;
  }

  /**
   * Execute a query and call a callback for EACH row individually.
   */
  each(sql, ...args) {
    const { params, callback, complete } = this._parseArgs(args);
    this._schedule("each", sql, params, callback, complete);
    return this;
  }

  /**
   * Execute a function in a serialized context.
   */
  serialize(callback) {
    if (callback) callback();
  }

  /**
   * Execute a function in parallel.
   */
  parallelize(callback) {
    if (callback) callback();
  }

  /**
   * Close the database.
   */
  close(callback) {
    // Schedule close as a WRITE to ensure all pending ops finish
    const doClose = async () => {
      if (this.db) {
        if (this.conn) {
          try {
            this.conn.release();
          } catch (e) {}
          this.conn = null;
        }
        await this.db.close();
      }
    };

    const barrier = Promise.all([this.lastWrite, ...this.activeReads]);
    const closePromise = barrier.then(doClose);
    this.lastWrite = closePromise.catch(() => {});

    closePromise
      .then(() => callback && callback(null))
      .catch((err) => callback && callback(err));
  }

  on(event, listener) {
    return this;
  }

  // ===========================================================================
  // INTERNAL HELPERS
  // ===========================================================================

  _parseArgs(args) {
    let params = [];
    let callback = null;
    let complete = null;

    if (args.length > 0) {
      if (typeof args[args.length - 1] === "function") {
        callback = args.pop();
      }
      if (args.length > 0 && typeof args[args.length - 1] === "function") {
        complete = callback;
        callback = args.pop();
      }
      if (args.length > 0) {
        params = args[0];
        if (!Array.isArray(params)) {
          params = [params];
        }
      }
    }
    return { params, callback, complete };
  }

  /**
   * Scheduling Facade.
   * Analyzes SQL to prioritize Reads vs Writes.
   */
  _schedule(method, sql, params, cb, complete) {
    if (this.initError) {
      if (cb) cb(this.initError);
      return;
    }

    if (!this.db) {
      this.queue.push({ method, sql, params, cb, complete });
      return;
    }

    // --- DETERMINE SCHEDULING TYPE (READ vs WRITE) ---
    // We use robust heuristics to ensure safety.
    let type = "WRITE";

    const s = sql.trim();
    const upperS = s.toUpperCase();

    // 1. Force WRITE if inside a transaction or changing transaction state
    //    (Strict serialization required by SQLite logic)
    const isTxCmd =
      upperS.startsWith("BEGIN") ||
      upperS.startsWith("COMMIT") ||
      upperS.startsWith("ROLLBACK") ||
      upperS.startsWith("SAVEPOINT") ||
      upperS.startsWith("RELEASE");

    if (this.conn || isTxCmd) {
      type = "WRITE";
    }
    // 2. Analyze SQL content for Read/Write
    else {
      // Matches standard reads: SELECT, EXPLAIN, VALUES
      const isSelect = /^\s*(SELECT|EXPLAIN|VALUES)/i.test(s);

      // Detects Write-with-Return: INSERT...RETURNING
      const hasReturning = /RETURNING\b/i.test(s);

      if (isSelect && !hasReturning) {
        type = "READ";
      } else {
        // INSERT, UPDATE, DELETE, DROP, CREATE, PRAGMA, etc.
        type = "WRITE";
      }
    }

    // --- SCHEDULER LOGIC ---

    const task = async () => {
      await this._execute(method, sql, params, cb, complete);
    };

    if (type === "WRITE") {
      // WRITE BARRIER: Wait for Last Write AND All Active Reads
      // This ensures exclusive access logic at the scheduler level
      const barrier = Promise.all([this.lastWrite, ...this.activeReads]);

      const opPromise = barrier.then(task);

      // Become the new Last Write Barrier
      this.lastWrite = opPromise.catch(() => {});
    } else {
      // READ: Wait for Last Write ONLY
      // Runs in parallel with other pending reads
      const opPromise = this.lastWrite.then(task);

      // Register as active read so future writes wait for us
      this.activeReads.add(opPromise);
      opPromise.finally(() => this.activeReads.delete(opPromise));
    }
  }

  _flushQueue() {
    while (this.queue.length > 0) {
      const task = this.queue.shift();
      if (this.initError) {
        if (task.cb) task.cb(this.initError);
      } else {
        this._schedule(
          task.method,
          task.sql,
          task.params,
          task.cb,
          task.complete,
        );
      }
    }
  }

  /**
   * Core Execution Logic.
   */
  async _execute(method, sql, params, cb, complete) {
    try {
      const s = sql.trim();
      const upperS = s.toUpperCase();

      // 1. Transaction Start
      if (upperS.startsWith("BEGIN")) {
        if (!this.conn) {
          this.conn = await this.db.acquire();
        }
      }

      // 2. Select Executor
      const executor = this.conn || this.db;

      // 3. Run
      let result;

      // Determine the statement method based on the adapter method call
      // Note: We ignore the Read/Write heuristic here and trust the user's method call
      // to determine the *format* of the response (rows vs metadata).
      // The routing safety was already handled by the Scheduler above.

      if (method === "run") {
        result = await executor.prepare(sql).run(...params);
      } else if (method === "all") {
        result = await executor.prepare(sql).all(...params);
      } else if (method === "get") {
        result = await executor.prepare(sql).get(...params);
      } else if (method === "each") {
        const iter = executor.prepare(sql).iterate(...params);
        let count = 0;
        for await (const row of iter) {
          count++;
          if (cb) cb(null, row);
        }
        if (complete) complete(null, count);
        return;
      }

      // 4. Transaction End
      if (this.conn) {
        if (
          upperS.startsWith("COMMIT") ||
          (upperS.startsWith("ROLLBACK") && !upperS.includes("TO "))
        ) {
          this.conn.release();
          this.conn = null;
        }
      }

      // 5. Callback
      if (cb) {
        const ctx = {};
        if (result && method === "run") {
          ctx.lastID = result.lastInsertRowid;
          ctx.changes = result.changes;
        }
        const data = method === "run" ? null : result;
        cb.call(ctx, null, data);
      }
    } catch (err) {
      if (cb) cb(err);

      // Critical Cleanup
      if (this.conn) {
        if (this.conn._released) {
          this.conn = null;
        } else if (upperS.startsWith("BEGIN")) {
          try {
            this.conn.release();
          } catch (e) {}
          this.conn = null;
        }
      }
    }
  }
}

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = {
  /**
   * The Adapter class, aliased as 'Database' to match sqlite3 exports.
   */
  Database: SQLite3Adapter,

  /**
   * Enables verbose logging mode.
   * TypeORM calls this: require('driver').verbose()
   * @returns {Object} The module exports (chainable).
   */
  verbose: () => {
    verboseMode = true;
    return module.exports;
  },
};
