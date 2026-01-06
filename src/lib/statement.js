/**
 * @file lib/statement.js
 * @description Proxy class that mirrors the API of better-sqlite3's Statement class.
 * It routes queries to the appropriate worker (Writer or Reader) based on context.
 */

const { castRow } = require("./utils");

// =============================================================================
// TYPE DEFINITIONS
// =============================================================================

/**
 * @typedef {Object} ExecutionOptions
 * @property {boolean} pluck - If true, returns only the first column.
 * @property {boolean} raw - If true, returns raw arrays instead of objects.
 * @property {boolean} expand - If true, expands dot-notation keys into nested objects.
 */

/**
 * @typedef {Object} QueryPayload
 * @property {string} sql - The SQL source.
 * @property {Array<any>} params - Bind parameters.
 * @property {ExecutionOptions} options - Formatting options.
 */

/**
 * @typedef {Object} RunResult
 * @property {number} changes - The number of rows modified.
 * @property {number | bigint} lastInsertRowid - The rowid of the last inserted row.
 */

 /**
 * We import the Database class definition from the parent module.
 * @typedef {import('../database'.Database)} Database
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
    /** @type {Database} Reference to the main pool instance. */
    this.db = db;

    /** @type {string} The SQL source string. */
    this.source = sql;

    /**
     * @type {boolean}
     * Heuristic to determine if this is a Read-Only query.
     * We exclude 'RETURNING' clauses because they write data (INSERT/UPDATE/DELETE)
     * but return rows like a SELECT.
     */
    this.reader = /^\s*(SELECT|EXPLAIN)/i.test(sql) && !/RETURNING/i.test(sql);

    /** @type {Array<any>} Default parameters bound via .bind() */
    this.boundParams = [];

    // --- Configuration Flags (Default: False) ---
    /** @type {boolean} Return only the first column value. */
    this._pluck = false;
    /** @type {boolean} Return raw arrays [val, val] instead of objects {col: val}. */
    this._raw = false;
    /** @type {boolean} Expand 'table.col' keys into nested objects { table: { col: val } }. */
    this._expand = false;
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
   * Bind parameters to the statement permanently.
   * @param {...any} params - The values to bind.
   * @returns {this}
   */
  bind(...params) {
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
    const p = params.length ? params : this.boundParams;

    // Writers handle locking internally via db.writeMutex
    return this.db._requestWrite("run", {
      sql: this.source,
      params: p,
      options: this._getOptions(),
    });
  }

  /**
   * Execute the query and return all matching rows.
   * Routes to Reader or Writer based on context.
   * @param {...any} params - Query parameters.
   * @returns {Promise<any[]>} An array of rows.
   */
  async all(...params) {
    const p = params.length ? params : this.boundParams;

    /** @type {QueryPayload} */
    const payload = {
      sql: this.source,
      params: p,
      options: this._getOptions(),
    };

    let result;

    // --- ROUTING LOGIC ---
    // 1. Transaction: Must use Writer (Reader workers don't see uncommitted data).
    // 2. Write Query: Queries with RETURNING (INSERT..RETURNING) must go to Writer.
    // 3. Memory DB: Only Writer exists (Readers are disabled).
    if (this.db._inTransaction || !this.reader || this.db.memory) {
      result = await this.db._requestWrite("all", payload);
    } else {
      result = await this.db._requestRead("all", payload);
    }

    // --- POST-PROCESSING ---
    // better-sqlite3 returns raw data from workers. We must cast BigInts/Buffers
    // back to their proper types if we are in 'default' object mode.
    // (raw/pluck modes usually don't strictly require this overhead or handle it differently)
    if (!this._raw && !this._pluck && result.rows) {
      // result.columns contains metadata needed for casting (e.g., distinguishing BLOBs)
      result.rows.forEach((row) => castRow(row, result.columns));
    }

    return result.rows;
  }

  /**
   * Execute the query and return the first matching row.
   * @param {...any} params
   * @returns {Promise<any | undefined>} The first row or undefined.
   */
  async get(...params) {
    const rows = await this.all(...params);
    return rows ? rows[0] : undefined;
  }

  /**
   * Execute the query and return an Async Iterator.
   * Uses a streaming protocol with backpressure control.
   * @param {...any} params
   * @returns {AsyncIterableIterator<any>}
   */
  async *iterate(...params) {
    const p = params.length ? params : this.boundParams;

    // --- FALLBACK: IN-MEMORY MODE ---
    if (this.db.memory) {
      // Streaming requires locking a specific worker statefully.
      // Since the Writer handles all requests in memory mode, locking it for a stream
      // would block the entire DB.
      // Fallback: Fetch ALL rows into RAM and yield them.
      const rows = await this.all(...p);
      for (const row of rows) yield row;
      return;
    }

    // --- STREAM SETUP ---
    const streamId = Math.random().toString(36).slice(2);

    // 1. Acquire a Reader
    // We strictly need a 'free' reader to lock it. If all are busy, we wait or take one.
    // Note: This simple logic takes the first available or the first one if all busy.
    // In a production refined version, you might want to wait for a free slot.
    const worker = this.db.readers.find((r) => !r.busy) || this.db.readers[0];

    // Mark as busy so the Load Balancer doesn't assign other 'all/get' tasks to it
    worker.busy = true;

    const queue = [];
    let resolver = null;
    let active = true;
    let columns = null; // Store column metadata for casting

    /**
     * @typedef {Object} StreamMessage
     * @property {'stream_data'} status
     * @property {string} streamId
     * @property {string} [error]
     * @property {any[]} [data]
     * @property {any[]} [columns]
     * @property {boolean} [done]
     */

    /**
     * @param {StreamMessage} msg
     */
    const onMsg = (msg) => {
      // Ensure we only listen to OUR stream's messages
      if (msg.status === "stream_data" && msg.streamId === streamId) {
        if (msg.error) {
          active = false;
          if (resolver) resolver.reject(new Error(msg.error));
          return;
        }

        // First packet might contain column metadata
        if (msg.columns) {
          columns = msg.columns;
        }

        if (msg.data) {
          queue.push(...msg.data);
        }

        if (msg.done) {
          active = false;
          queue.push(null); // EOF Sentinel
        }

        // Wake up the generator
        if (resolver) {
          const r = resolver;
          resolver = null;
          r.resolve();
        }
      }
    };

    worker.worker.on("message", onMsg);

    try {
      // 2. Start the Stream
      worker.worker.postMessage({
        id: streamId,
        action: "stream_open",
        streamId,
        sql: this.source,
        params: p,
        options: this._getOptions(), // Pass options like raw/pluck to worker
      });

      // 3. Consume Loop
      while (true) {
        // If queue is empty, wait for data
        if (queue.length === 0) {
          if (!active) break;
          await new Promise(
            (res, rej) => (resolver = { resolve: res, reject: rej }),
          );
        }

        const row = queue.shift();
        if (row === null) break; // EOF

        // Apply Casting (same logic as .all())
        if (!this._raw && !this._pluck && columns) {
          castRow(row, columns);
        }

        yield row;

        // 4. Backpressure / Flow Control
        // If the local buffer is getting low, ask the worker for more data.
        if (queue.length < 25 && active) {
          worker.worker.postMessage({
            id: streamId,
            action: "stream_ack",
            streamId,
          });
        }
      }
    } finally {
      // 5. Cleanup
      worker.worker.off("message", onMsg);
      // Tell worker to kill the iterator if we stopped early (e.g. break inside for-await-of)
      worker.worker.postMessage({ action: "stream_close", streamId });
      worker.busy = false;
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
    return { pluck: this._pluck, raw: this._raw, expand: this._expand };
  }
}

module.exports = Statement;
