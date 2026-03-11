/**
 * @file lib/adapter.js
 * @description A compatibility adapter that makes 'better-sqlite3-pool' look like the legacy 'sqlite3' driver.
 * This allows ORMs like TypeORM to use this connection pool transparently while maintaining
 * the thread-safety and performance benefits of the worker pool.
 */

const Database = require("./lib/database");

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

      if (args.length === 1) {
        params = args[0];
      } else if (args.length > 1) {
        params = args;
      }
    }

    // 2. Normalization for Named and Positional Parameters
    if (params && typeof params === "object" && !Array.isArray(params)) {
      const keys = Object.keys(params);

      // Check for strictly numeric keys (Legacy positional binding)
      let isNumericPositional = keys.length > 0;
      let maxIndex = 0;
      for (const key of keys) {
        if (!/^\d+$/.test(key)) {
          isNumericPositional = false;
          break;
        }
        maxIndex = Math.max(maxIndex, parseInt(key));
      }

      if (isNumericPositional) {
        // Convert { '1': v, '2': v } to [v, v]
        const arr = new Array(maxIndex);
        for (const key of keys) {
          arr[parseInt(key) - 1] = params[key];
        }
        params = arr;
      } else {
        // Check if any key already has a prefix (:, @, $)
        const hasPrefix = keys.some((k) => /^[:@$]/.test(k));
        if (!hasPrefix) {
          // Standard sqlite3 behavior: { id: 1 } -> { ':id': 1 }
          const normalized = {};
          for (const key of keys) {
            normalized[`:${key}`] = params[key];
          }
          params = normalized;
        } else {
          // If it already has prefixes (like Sequelize's $1, $2), we provide
          // both the prefixed and base name as aliases for better-sqlite3 compatibility.
          const normalized = {};
          for (const key of keys) {
            const val = params[key];
            normalized[key] = val;
            if (key.length > 1 && /^[:@$]/.test(key)) {
              normalized[key.substring(1)] = val;
            }
          }
          params = normalized;
        }
      }
    }

    // 3. Final Wrap-up: Ensure params is always an array or object
    if (params === null || params === undefined) {
      params = [];
    } else if (typeof params !== "object" && !Array.isArray(params)) {
      params = [params];
    } else if (Array.isArray(params)) {
      // Filter out null/undefined placeholders (TypeORM compatibility)
      params = params.filter((p) => p !== null && p !== undefined);
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
        result = await (Array.isArray(params)
          ? executor.prepare(sql).run(...params)
          : executor.prepare(sql).run(params));
      } else if (method === "all") {
        try {
          result = await (Array.isArray(params)
            ? executor.prepare(sql).all(...params)
            : executor.prepare(sql).all(params));
        } catch (err) {
          if (err.message.includes("Use run() instead")) {
            await (Array.isArray(params)
              ? executor.prepare(sql).run(...params)
              : executor.prepare(sql).run(params));
            result = [];
          } else {
            throw err;
          }
        }
      } else if (method === "get") {
        try {
          result = await (Array.isArray(params)
            ? executor.prepare(sql).get(...params)
            : executor.prepare(sql).get(params));
        } catch (err) {
          if (err.message.includes("Use run() instead")) {
            await (Array.isArray(params)
              ? executor.prepare(sql).run(...params)
              : executor.prepare(sql).run(params));
            result = undefined;
          } else {
            throw err;
          }
        }
      } else if (method === "each") {
        const iter = Array.isArray(params)
          ? executor.prepare(sql).iterate(...params)
          : executor.prepare(sql).iterate(params);
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

      // 5. Callback & Metadata mapping
      if (cb) {
        const lastID =
          result && result.lastInsertRowid !== undefined
            ? result.lastInsertRowid
            : undefined;
        const changes =
          result && result.changes !== undefined ? result.changes : 0;

        const ctx = { lastID, changes };

        // for .all() and .get(), we ensure result is exactly what better-sqlite3 would return
        // for .run(), we return the info object
        let data =
          method === "run"
            ? { lastID, lastInsertRowid: lastID, changes }
            : result;

        // Legacy drivers expect an empty array if no rows are found
        if (method === "all" && !Array.isArray(data)) {
          data = [];
        }

        cb.call(ctx, null, data);
      }
    } catch (err) {
      if (cb) cb(err);
      if (complete) complete();

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
