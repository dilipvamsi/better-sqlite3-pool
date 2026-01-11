/**
 * @file lib/statement.js
 * @description Proxy class that mirrors the API of better-sqlite3's Statement class.
 * It routes queries to the appropriate worker (Writer or Reader) based on context.
 */

const { castRow } = require("./utils");
const { SingleWorkerClient } = require("./worker-pool");
const { SqliteError } = require("better-sqlite3-multiple-ciphers");

// =============================================================================
// TYPE DEFINITIONS
// =============================================================================

/** @typedef {import('./connection')} Connection */
/** @typedef {import('./database').Database} Database */
/** @typedef {import('./worker-pool').SingleWorkerClient} SingleWorkerClient */
/** @typedef {import('better-sqlite3-multiple-ciphers').Database.RunResult} RunResult */

/**
 * @typedef {Object} ExecutionOptions
 * @property {boolean} pluck - If true, returns only the first column.
 * @property {boolean} raw - If true, returns raw arrays instead of objects.
 * @property {boolean} expand - If true, expands dot-notation keys into nested objects.
 * @property {boolean} [safeIntegers] - If true, returns integers as BigInts.
 */

/**
 * @typedef {Object} QueryPayload
 * @property {string} sql - The SQL source.
 * @property {Array<any>} params - Bind parameters.
 * @property {ExecutionOptions} options - Formatting options.
 */

// =============================================================================
// INTERNAL HELPERS
// =============================================================================

/**
 * Validates parameters to ensure they are compatible with better-sqlite3 constraints.
 * Specifically checks for custom class instances which are strictly forbidden.
 *
 * NOTE: We do NOT check for parameter counts (RangeError) or missing named keys here.
 * That requires knowing the SQL schema, so we let the Worker handle that.
 *
 * @param {Array<any>} params
 * @throws {TypeError} If a parameter is an invalid object type.
 */
function validateParams(params) {
  // REMOVED: Strict Arity Check for Named Parameters.
  // We allow the worker (native better-sqlite3) to handle argument logic.
  // We only filter types that break IPC serialization.

  for (const p of params) {
    const type = typeof p;

    // 1. Undefined is not supported
    if (type === "undefined") {
      throw new TypeError(
        "better-sqlite3-pool: Data type undefined is not supported.",
      );
    }

    // 2. Functions are not supported (and cannot be cloned)
    if (type === "function") {
      throw new TypeError(
        "better-sqlite3-pool: Data type function is not supported.",
      );
    }

    // 3. Object Validation
    if (p !== null && type === "object") {
      // Buffers are valid
      if (Buffer.isBuffer(p)) continue;

      // Arrays are valid
      if (Array.isArray(p)) continue;

      // Plain Objects are valid
      // We check for Custom Classes (instances of something other than Object)
      const proto = Object.getPrototypeOf(p);
      if (proto !== Object.prototype && proto !== null) {
        throw new TypeError(
          "better-sqlite3-pool: Parameters must be primitives, Buffers, Arrays, or plain Objects.",
        );
      }
    }
  }
}

// =============================================================================
// STATEMENT CLASS
// =============================================================================

/**
 * @class Statement
 * @description Represents a prepared statement.
 * Instances of this class are returned by `db.prepare()`.
 */
class Statement {
  /**
   * Create a new Statement proxy.
   * @param {Database | Connection} dbOrConn - The parent Database instance or Connection instance.
   * @param {string} sql - The raw SQL string.
   */
  constructor(dbOrConn, sql) {
    // Validation to satisfy "expect(() => new stmt.constructor(source)).to.throw(TypeError);"
    // We check if 'db' looks like our Database object (has 'prepare' method)
    const type = dbOrConn ? dbOrConn.constructor.name : "Unknown";
    if (!dbOrConn || (type !== "Database" && type !== "Connection")) {
      throw new TypeError(
        "Expected first argument to be a Database or Connection instance",
      );
    }
    if (typeof sql !== "string") {
      throw new TypeError("Expected second argument to be a string");
    }

    /** @type {Database | Connection} The execution context (DB Pool or Exclusive Session). */
    this.ctx = dbOrConn;

    /** @type {Database} Reference to the main pool instance. */
    this.db = type === "Connection" ? dbOrConn.db : dbOrConn;

    /** @type {string} The SQL source string. */
    this.source = sql;

    /**
     * @type {boolean}
     * Indicates if the statement is "Read-Only" safe.
     *
     * Used for **Connection Routing**:
     * - `true`: Can be safely executed on a load-balanced Reader worker (or the Writer).
     * - `false`: Must be executed on the Writer (modifies data or acquires write locks).
     *
     * Logic:
     * 1. EXCLUDE `BEGIN IMMEDIATE/EXCLUSIVE`: These explicitly acquire write locks.
     * 2. INCLUDE `SELECT`, `EXPLAIN`, `VALUES`, and Transaction controls (`BEGIN`, `COMMIT`, etc).
     * 3. EXCLUDE `RETURNING`: Write operations (INSERT/UPDATE) that return data are NOT read-only.
     */
    this.readonly =
      !/^\s*BEGIN\s+(IMMEDIATE|EXCLUSIVE)/i.test(this.source) &&
      /^\s*(SELECT|EXPLAIN|VALUES|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)/i.test(
        this.source,
      ) &&
      !/RETURNING\b/i.test(this.source);

    /**
     * @type {boolean}
     * Indicates if the statement returns data (Rows).
     *
     * Used for **API Behavior**:
     * - `true`: The statement supports `.get()`, `.all()`, and `.iterate()`.
     * - `false`: The statement typically uses `.run()` (though `.all()` might return empty array).
     *
     * Logic:
     * 1. Standard reads: `SELECT`, `EXPLAIN`, `PRAGMA`, `VALUES`.
     * 2. Write-with-return: Any statement containing a `RETURNING` clause.
     */
    this.reader =
      /^\s*(SELECT|EXPLAIN|PRAGMA|VALUES)/i.test(this.source) ||
      /RETURNING\b/i.test(this.source);

    /** @type {Array<any>} Default parameters bound via .bind() */
    this.boundParams = [];

    /** @type {boolean} Indicates if the statement has been bound at least once. */
    this._bound = false;

    // --- Configuration Flags (Default: False) ---
    /** @type {boolean} Return only the first column value. */
    this._pluck = false;
    /** @type {boolean} Return raw arrays [val, val] instead of objects {col: val}. */
    this._raw = false;
    /** @type {boolean} Expand 'table.col' keys into nested objects { table: { col: val } }. */
    this._expand = false;
    /** @type {boolean | undefined} Safe integers as BigInts. */
    this._safeIntegers = undefined;

    /** @type {import('better-sqlite3-multiple-ciphers').ColumnDefinition[]} */
    this._columns = [];

    /** @type {boolean} */
    this.busy = false;

    /**
     * Flag to track if the statement has been executed at least once.
     * Required because column metadata is fetched asynchronously.
     * @type {boolean}
     */
    this._hasExecuted = false;
  }

  /**
   * The parent database object.
   * Required for better-sqlite3 API compatibility tests.
   */
  get database() {
    return this.db;
  }

  /**
   * Returns the column metadata.
   * @throws {SqliteError} If accessed before the statement has been executed.
   */
  columns() {
    if (!this._hasExecuted) {
      throw new SqliteError(
        "The statement has not been executed yet. Column metadata is only available after the first execution in threaded mode.",
        "SQLITE_MISUSE",
      );
    }
    return this._columns;
  }

  // ===========================================================================
  // CONFIGURATION METHODS (Chainable)
  // ===========================================================================

  /**
   * Toggle returning only the first column of the first row (for .get) or first column of all rows (for .all).
   * @param {boolean} [toggle=true]
   * @returns {this}
   */
  pluck(toggle = true) {
    this._pluck = toggle;
    // Enforce mutual exclusivity: Enabling pluck disables raw/expand
    if (toggle) {
      this._raw = false;
      this._expand = false;
    }
    return this;
  }

  /**
   * Toggle returning rows as arrays instead of objects.
   * @param {boolean} [toggle=true]
   * @returns {this}
   */
  raw(toggle = true) {
    this._raw = toggle;
    // Enforce mutual exclusivity
    if (toggle) {
      this._pluck = false;
      this._expand = false;
    }
    return this;
  }

  /**
   * Toggle expanding dot-notation column names (e.g. "user.id") into nested objects.
   * @param {boolean} [toggle=true]
   * @returns {this}
   */
  expand(toggle = true) {
    this._expand = toggle;
    // Enforce mutual exclusivity
    if (toggle) {
      this._pluck = false;
      this._raw = false;
    }
    return this;
  }

  /**
   * Toggle BigInt support for this statement.
   * @param {boolean} [toggle=true]
   * @returns {this}
   */
  safeIntegers(toggle = true) {
    this._safeIntegers = toggle;
    return this;
  }

  /**
   * Bind parameters to the statement permanently.
   * @param {...any} params - The values to bind.
   * @returns {this}
   */
  bind(...params) {
    this.db._ensureOpen();
    if (this._bound) {
      throw new TypeError(
        "The bind() method can only be invoked once per statement",
      );
    }
    validateParams(params);

    // Snapshot parameters to prevent mutation affecting bound values (Permanent Binding)
    this.boundParams = params.map((p) => {
      if (Buffer.isBuffer(p)) return Buffer.from(p);
      return p;
    });

    this._bound = true;
    return this;
  }

  // ===========================================================================
  // EXECUTION METHODS
  // ===========================================================================

  /**
   * Execute the query and return metadata (changes, lastInsertRowid).
   * Used for INSERT, UPDATE, DELETE.
   * Always routes to the Writer.
   * @param {...any} params - Query parameters (overrides bound params).
   * @returns {Promise<RunResult>}
   */
  async run(...params) {
    this.db._ensureOpen();

    if (params.length > 0) {
      if (this._bound) {
        throw new TypeError(
          "Cannot bind temporary parameters to a statement that already has bound parameters",
        );
      }
      validateParams(params);
    }

    const stmtParams = params.length ? params : this.boundParams;
    const payload = {
      sql: this.source,
      params: stmtParams,
      options: this._getOptions(),
    };

    // SPECIAL HANDLING: ATTACH / DETACH
    // These must be broadcast to ALL workers (Writer + Readers) to ensure
    // the database schema is consistent across the entire pool.
    if (/^\s*(ATTACH|DETACH)\b/i.test(this.source)) {
      // Use _execConfig to broadcast and mark as sticky (persists on worker restart)
      await this.ctx._execConfig({ ...payload, action: "run" });
      // ATTACH/DETACH do not return row counts
      return { changes: 0, lastInsertRowid: 0 };
    }

    // 1. Read-Only Database Handling
    if (this.db.readonly) {
      // In readonly mode, we must send to the Reader Pool.
      await this.db._requestRead("run", payload);
      return { changes: 0, lastInsertRowid: 0 };
    }

    // 2. Standard Write Mode
    // Writers handle locking internally via db.writer.lock()
    return this.ctx._requestWrite("run", payload);
  }

  /**
   * Execute the query and return all matching rows.
   * Routes to Reader or Writer based on context.
   * @param {...any} params - Query parameters.
   * @returns {Promise<any[]>} An array of rows with column definitions.
   */
  async all(...params) {
    this.db._ensureOpen();

    // Validate inputs BEFORE serialization strips the prototype info
    if (params.length > 0) {
      if (this._bound) {
        throw new TypeError(
          "Cannot bind temporary parameters to a statement that already has bound parameters",
        );
      }
      validateParams(params);
    }

    const stmtParams = params.length ? params : this.boundParams;

    /** @type {QueryPayload} */
    const payload = {
      sql: this.source,
      params: stmtParams,
      options: this._getOptions(),
    };

    let result;

    // --- ROUTING LOGIC ---
    // If context is a Connection, it handles routing (always to Writer).
    // If context is Database, it handles routing (Reader vs Writer).
    if (this.ctx.constructor.name === "Connection") {
      // Inside transaction/session: Always write
      result = await this.ctx._requestWrite("all", payload);
    } else {
      // Read only database route to reader pool
      if (this.db.readonly) {
        result = await this.db._requestRead("all", payload);
      }
      // 1. Transaction: Must use Writer (Reader workers don't see uncommitted data).
      // 2. Write Query: Queries with RETURNING (INSERT..RETURNING) must go to Writer.
      // 3. Memory DB: Only Writer exists (Readers are disabled).
      else if (this.db.inTransaction || !this.readonly || this.db.memory) {
        result = await this.db._requestWrite("all", payload);
      } else {
        result = await this.db._requestRead("all", payload);
      }
    }

    // Capture columns from worker response
    if (result.columns) {
      this._columns = result.columns;
    }
    this._hasExecuted = true;

    // --- POST-PROCESSING ---
    // better-sqlite3 returns raw data from workers. We must cast BigInts/Buffers
    // back to their proper types if we are in 'default' object mode.
    // (raw/pluck modes usually don't strictly require this overhead or handle it differently)
    if (result.rows) {
      if (!this._raw && !this._pluck) {
        // Standard Mode: Cast BigInts
        result.rows.forEach((row) => castRow(row, result.columns));
      } else if (this._pluck) {
        // Pluck Mode: Restore Buffers if they became Uint8Array
        for (let i = 0; i < result.rows.length; i++) {
          if (
            result.rows[i] instanceof Uint8Array &&
            !Buffer.isBuffer(result.rows[i])
          ) {
            result.rows[i] = Buffer.from(result.rows[i]);
          }
        }
      } else if (this._raw) {
        // Raw Mode: Restore Buffers inside arrays
        for (const row of result.rows) {
          if (Array.isArray(row)) {
            for (let i = 0; i < row.length; i++) {
              if (row[i] instanceof Uint8Array && !Buffer.isBuffer(row[i])) {
                row[i] = Buffer.from(row[i]);
              }
            }
          }
        }
      }
    }

    return result.rows || [];
  }

  /**
   * Execute the query and return the first matching row.
   * @param {...any} params
   * @returns {Promise<any | undefined>} The first row or undefined.
   */
  async get(...params) {
    this.db._ensureOpen();
    const rows = await this.all(...params);
    return rows ? rows[0] : undefined;
  }

  /**
   * Execute the query and return an Async Iterator.
   * Uses a sticky worker connection and streaming protocol.
   * @param {...any} params
   * @returns {AsyncIterableIterator<any>}
   */
  iterate(...params) {
    // 1. Synchronous Check (Throws immediately if DB is closed)
    this.db._ensureOpen();

    // Validate inputs BEFORE serialization strips the prototype info
    if (params.length > 0) {
      if (this._bound) {
        throw new TypeError(
          "Cannot bind temporary parameters to a statement that already has bound parameters",
        );
      }
      validateParams(params);
    }

    // 2. Delegate to the internal Async Generator
    return this._iterate(...params);
  }

  /**
   * Execute the query and return an Async Iterator.
   * Uses a sticky worker connection and streaming protocol.
   * @param {...any} params
   * @returns {AsyncIterableIterator<any>}
   */
  async *_iterate(...params) {
    this.db._ensureOpen();
    const stmtParams = params.length ? params : this.boundParams;

    // --- FALLBACK: IN-MEMORY / NO POOL ---
    if (this.db.memory || (!this.db.readerPool && this.db.writer)) {
      const rows = await this.all(...stmtParams);
      for (const row of rows) yield row;
      return;
    }

    // --- WORKER ACQUISITION ---
    /** @type {SingleWorkerClient} */
    let workerClient;
    /** @type {boolean} manually lock if it's not a transaction */
    let manualLock = false;

    // 1. Determine Worker
    if (this.ctx.constructor.name === "Connection") {
      // We are bound to an exclusive connection. Use its writer.
      workerClient = this.ctx.writer;
      // Do NOT call lock(), it is already locked by the Connection.
    } else if (this.db.inTransaction || (!this.db.readonly && !this.readonly)) {
      // DB-level transaction wrapper active OR Write query
      if (!this.db.writer) {
        throw new SqliteError("No writer available", "SQLITE_MISUSE");
      }
      workerClient = this.db.writer;
      // We shouldn't need to lock if inTransaction (wrapper handles it),
      // but if it's just a write query iterate(), we might need to lock.
      // Actually, iterate() on a write query is rare/weird, usually implies RETURNING.
      if (!this.db.inTransaction) {
        await workerClient.lock();
        manualLock = true;
      }
    } else {
      // Read-only Pool
      // We assume readerPool exists if we passed the fallback check above
      workerClient = await this.db.readerPool.getWorker();

      // We MUST lock/pin this reader to prevent LB interruption
      await workerClient.lock();
      manualLock = true;
    }

    const iteratorId = Math.random().toString(36).slice(2);
    let columns = null;

    this.busy = true; // Mark busy
    try {
      let result;
      let error;
      try {
        // 1. OPEN STREAM (Get first batch)
        result = await workerClient.noLockExecute({
          action: "iterator_open",
          iteratorId,
          sql: this.source,
          params: stmtParams,
          options: this._getOptions(),
        });
      } catch (err) {
        // Even though error is raised valid rows are returned before the error
        result = err.__data;
        error = err;
      }
      // Capture columns from first batch
      if (result.columns) {
        columns = result.columns;
        this._columns = result.columns;
      }
      this._hasExecuted = true;

      // Process and yield first batch
      if (result.rows && result.rows.length > 0) {
        // Store in list and yield explicit elements
        const rows = result.rows;
        for (const row of rows) {
          // Perform casting inline to avoid iterating twice
          if (!this._raw && !this._pluck && columns) {
            castRow(row, columns);
          }
          yield row;
        }
      }

      if (error) {
        throw error;
      }

      // 2. CONSUME REST
      while (!result.done) {
        try {
          // Request next batch
          result = await workerClient.noLockExecute({
            action: "iterator_next",
            iteratorId,
          });
        } catch (error) {
          // Even though error is raised valid rows are returned before the error
          result = err.__data;
          error = err;
        }
        // console.log(result);
        if (result.rows && result.rows.length > 0) {
          const rows = result.rows;
          for (const row of rows) {
            if (!this._raw && !this._pluck && columns) {
              castRow(row, columns);
            }
            yield row;
          }
        }
        if (error) {
          throw error;
        }
      }
    } finally {
      // 3. CLEANUP
      this.busy = false; // Mark not busy
      workerClient
        .noLockExecute({ action: "iterator_close", iteratorId })
        .catch(() => {});
      if (manualLock) {
        workerClient.unlock();
      }
    }
  }

  // ===========================================================================
  // INTERNAL HELPERS
  // ===========================================================================

  /**
   * Helper to gather current configuration options.
   * @returns {ExecutionOptions}
   * @private
   */
  _getOptions() {
    return {
      pluck: this._pluck,
      raw: this._raw,
      expand: this._expand,
      safeIntegers: this._safeIntegers,
    };
  }
}

module.exports = Statement;
