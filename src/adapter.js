/**
 * @file lib/adapter.js
 * @description A compatibility adapter that makes 'better-sqlite3-pool' look like the legacy 'sqlite3' driver.
 * This allows ORMs like TypeORM to use this connection pool transparently while maintaining
 * the thread-safety and performance benefits of the worker pool.
 */

const { Database } = require("./database");

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
   * @param {string} filename - Path to the SQLite database file.
   * @param {number} [mode] - File open mode (ignored for compatibility, handled by internal pool).
   * @param {Function} [callback] - Optional callback invoked when the DB is "open".
   */
  constructor(filename, mode, callback) {
    // Handle variable argument signature: new Database(filename, [mode], [callback])
    const cb = typeof mode === "function" ? mode : callback;

    /** * The underlying thread pool instance.
     * @type {Database}
     */
    this.db = new Database(filename);

    /** * Internal flag to track if we are currently inside a transaction.
     * Used to manage manual locking of the writer mutex.
     * @type {boolean}
     */
    this._inTransaction = false;

    /** * Logger function. If verbose mode is enabled, defaults to console.log.
     * @type {(msg: string, ...args: any[]) => void | null}
     */
    this.logger = verboseMode ? console.log : null;

    // Simulate async "open" event (pool initializes lazily, so we just callback on next tick)
    if (cb) setTimeout(() => cb(null), 0);
  }

  /**
   * Execute a query that does NOT return rows (INSERT, UPDATE, DELETE).
   * @param {string} sql - The SQL statement.
   * @param {Array<any>|SqliteCallback} [params] - Bind parameters or the callback.
   * @param {SqliteCallback} [cb] - Completion callback.
   */
  run(sql, params, cb) {
    this._exec("run", sql, params, cb);
  }

  /**
   * Execute a query that returns all matching rows (SELECT).
   * @param {string} sql - The SQL statement.
   * @param {Array<any>|SqliteCallback} [params] - Bind parameters or the callback.
   * @param {SqliteCallback} [cb] - Completion callback.
   */
  all(sql, params, cb) {
    this._exec("all", sql, params, cb);
  }

  /**
   * Internal execution helper.
   * Handles parameter normalization, transaction locking, logging, and callback binding.
   * @param {'run'|'all'} method - The method to call on the prepared statement.
   * @param {string} sql - The SQL string.
   * @param {Array<any>|Function} [params] - Parameters or callback.
   * @param {Function} [cb] - The final callback.
   * @returns {Promise<void>}
   * @private
   */
  async _exec(method, sql, params, cb) {
    // 1. Normalize Arguments: params is optional in sqlite3 signature
    if (typeof params === "function") {
      cb = params;
      params = [];
    }

    // 2. Verbose Logging
    if (this.logger) {
      this.logger(`[sqlite] ${sql}`, params && params.length ? params : "");
    }

    try {
      const s = sql.trim().toUpperCase();

      // 3. Transaction Handling (CRITICAL)
      // Legacy sqlite3 drivers rely on a single connection being "serialized" during a transaction.
      // Since our pool is multi-threaded/async, we must MANUALLY acquire the Writer lock
      // when a transaction starts (BEGIN) and hold it until it ends (COMMIT/ROLLBACK).
      // This ensures no other query slips into the middle of a transaction block.

      if (s.startsWith("BEGIN")) {
        // Force wait for the write lock
        await this.db.writeMutex.acquire();
        this.db._inTransaction = true;
      }

      // 4. Execute Query via Pool
      let result;
      if (method === "run") {
        // .run() returns metadata (changes, lastInsertRowid)
        result = await this.db.prepare(sql).run(...(params || []));
      } else {
        // .all() returns an array of rows
        result = await this.db.prepare(sql).all(...(params || []));
      }

      // 5. Transaction Unlock Logic
      // Only release the lock if we are fully ending the transaction.
      // We explicitly ignore "ROLLBACK TO SAVEPOINT" because the outer transaction is still active.
      if (
        s.startsWith("COMMIT") ||
        (s.startsWith("ROLLBACK") && !s.includes("TO SAVEPOINT"))
      ) {
        this.db._inTransaction = false;
        this.db.writeMutex.release();
      }

      // 6. Handle Callback (Legacy Context Binding)
      if (cb) {
        /** @type {RunContext} */
        const ctx = {};

        // ORMs like TypeORM expect `this.lastID` and `this.changes` to be set on the callback context
        if (result && result.lastInsertRowid !== undefined) {
          ctx.lastID = result.lastInsertRowid;
        }
        if (result && result.changes !== undefined) {
          ctx.changes = result.changes;
        }

        // Return: context as `this`, null error, and result data (for .all calls)
        cb.call(ctx, null, result);
      }
    } catch (err) {
      // Log errors if verbose mode is on
      if (this.logger) {
        console.error(`[sqlite error] ${err.message}`);
      }
      if (cb) cb(err);
    }
  }

  /**
   * Execute a query and call a callback for EACH row individually.
   * Returns 'this' immediately to allow chaining.
   * * @param {string} sql - SQL statement.
   * @param {Array<any>|Function} [params] - Bind parameters.
   * @param {Function} [callback] - Called for every row: (err, row) => void.
   * @param {Function} [complete] - Called after all rows: (err, count) => void.
   * @returns {this} - Chainable instance.
   */
  each(sql, params, callback, complete) {
    // 1. Normalize Arguments
    if (typeof params === "function") {
      complete = callback;
      callback = params;
      params = [];
    }

    // 2. Verbose Logging
    if (this.logger) {
      this.logger(
        `[sqlite each] ${sql}`,
        params && params.length ? params : "",
      );
    }

    // 3. Detached Async Execution
    // We do NOT await this. We let it run in the background so we can return 'this' immediately.
    (async () => {
      let count = 0;
      try {
        const stmt = this.db.prepare(sql);

        // Stream rows one by one
        for await (const row of stmt.iterate(...(params || []))) {
          count++;
          if (callback) {
            // Standard sqlite3 behavior: callback(null, row)
            callback(null, row);
          }
        }

        // Success Completion
        if (complete) {
          complete(null, count);
        }
      } catch (err) {
        if (this.logger) console.error(`[sqlite error] ${err.message}`);

        // Error Propagation
        // If an error happens, sqlite3 typically calls the row callback with error,
        // and then the complete callback with error.
        if (callback) callback(err);
        if (complete) complete(err, count);
      }
    })();

    // 4. Return immediately for chaining
    return this;
  }

  /**
   * Close the connection pool.
   * Terminates all worker threads.
   * @param {Function} [cb] - Optional callback on completion.
   */
  close(cb) {
    this.db.close().then(() => cb && cb(null));
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
