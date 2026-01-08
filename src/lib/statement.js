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
   * @param {Database} db - The parent Database instance.
   * @param {string} sql - The raw SQL string.
   */
  constructor(db, sql) {
    // Validation to satisfy "expect(() => new stmt.constructor(source)).to.throw(TypeError);"
    // We check if 'db' looks like our Database object (has 'prepare' method)
    if (!db || typeof db.prepare !== "function") {
      throw new TypeError("Expected first argument to be a Database instance");
    }
    if (typeof sql !== "string") {
      throw new TypeError("Expected second argument to be a string");
    }

    /** @type {Database} Reference to the main pool instance. */
    this.database = db;

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
    return this;
  }

  /**
   * Toggle returning rows as arrays instead of objects.
   * @param {boolean} [toggle=true]
   * @returns {this}
   */
  raw(toggle = true) {
    this._raw = toggle;
    return this;
  }

  /**
   * Toggle expanding dot-notation column names (e.g. "user.id") into nested objects.
   * @param {boolean} [toggle=true]
   * @returns {this}
   */
  expand(toggle = true) {
    this._expand = toggle;
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
    this.database._ensureOpen();
    this.boundParams = params;
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
    this.database._ensureOpen();
    const stmtParams = params.length ? params : this.boundParams;
    const payload = {
      sql: this.source,
      params: stmtParams,
      options: this._getOptions(),
    };

    // 1. Read-Only Database Handling
    if (this.database.readonly) {
      // In readonly mode, we must send to the Reader Pool.
      // We cannot use action='run' because the Worker logic throws on 'run'
      // when checking isWriter.
      // We send 'all' instead.
      // - If SQL is SELECT: Succeeded. We discard rows. Return dummy result.
      // - If SQL is INSERT: SQLite engine throws SQLITE_READONLY.
      await this.database._requestRead("all", payload);
      return { changes: 0, lastInsertRowid: 0 };
    }

    // 2. Standard Write Mode
    // Writers handle locking internally via db.writer.lock()
    return this.database._requestWrite("run", payload);
  }

  /**
   * Execute the query and return all matching rows.
   * Routes to Reader or Writer based on context.
   * @param {...any} params - Query parameters.
   * @returns {Promise<any[]>} An array of rows with column definitions.
   */
  async all(...params) {
    this.database._ensureOpen();
    const stmtParams = params.length ? params : this.boundParams;

    /** @type {QueryPayload} */
    const payload = {
      sql: this.source,
      params: stmtParams,
      options: this._getOptions(),
    };

    let result;

    // --- ROUTING LOGIC ---
    // Read only database route to reader pool
    if (this.database.readonly) {
      result = await this.database._requestRead("all", payload);
    }
    // 1. Transaction: Must use Writer (Reader workers don't see uncommitted data).
    // 2. Write Query: Queries with RETURNING (INSERT..RETURNING) must go to Writer.
    // 3. Memory DB: Only Writer exists (Readers are disabled).
    else if (
      this.database.inTransaction ||
      !this.readonly ||
      this.database.memory
    ) {
      result = await this.database._requestWrite("all", payload);
    } else {
      result = await this.database._requestRead("all", payload);
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
    if (!this._raw && !this._pluck && result.rows) {
      // result.columns contains metadata needed for casting (e.g., distinguishing BLOBs)
      result.rows.forEach((row) => castRow(row, result.columns));
    }

    return result.rows || [];
  }

  /**
   * Execute the query and return the first matching row.
   * @param {...any} params
   * @returns {Promise<any | undefined>} The first row or undefined.
   */
  async get(...params) {
    this.database._ensureOpen();
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
    this.database._ensureOpen();

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
    this.database._ensureOpen();
    const stmtParams = params.length ? params : this.boundParams;

    // --- FALLBACK: IN-MEMORY / NO POOL ---
    if (
      this.database.memory ||
      (!this.database.readerPool && this.database.writer)
    ) {
      const rows = await this.all(...stmtParams);
      for (const row of rows) yield row;
      return;
    }

    // --- WORKER ACQUISITION ---
    /** @type {SingleWorkerClient} */
    let workerClient;

    if (this.database.readonly) {
      workerClient = await this.database.readerPool.getWorker();
    } else if (this.database.inTransaction || !this.readonly) {
      if (!this.database.writer) {
        throw new SqliteError("No writer available", "SQLITE_MISUSE");
      }
      workerClient = this.database.writer;
    } else {
      workerClient = await this.database.readerPool.getWorker();
    }

    // LOCK THE WORKER
    if (!this.database.inTransaction) {
      await workerClient.lock();
    }

    const iteratorId = Math.random().toString(36).slice(2);
    let columns = null;

    this.busy = true; // Mark busy

    try {
      // 1. OPEN STREAM (Get first batch)
      let result = await workerClient.noLockExecute({
        action: "iterator_open",
        iteratorId,
        sql: this.source,
        params: stmtParams,
        options: this._getOptions(),
      });

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

      // 2. CONSUME REST
      while (!result.done) {
        // Request next batch
        result = await workerClient.noLockExecute({
          action: "iterator_next",
          iteratorId,
        });

        if (result.rows && result.rows.length > 0) {
          const rows = result.rows;
          for (const row of rows) {
            if (!this._raw && !this._pluck && columns) {
              castRow(row, columns);
            }
            yield row;
          }
        }
      }
    } finally {
      // 3. CLEANUP
      this.busy = false; // Mark not busy
      workerClient
        .noLockExecute({ action: "iterator_close", iteratorId })
        .catch(() => {});
      if (!this.database.inTransaction) {
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
