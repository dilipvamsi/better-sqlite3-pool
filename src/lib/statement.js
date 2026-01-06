/**
 * @file lib/statement.js
 * @description Proxy class for Prepared Statements.
 */
const { castRow } = require("./utils");

class Statement {
  /**
   * @param {import('../index')} db
   * @param {string} sql
   */
  constructor(db, sql) {
    this.db = db;
    this.source = sql;
    // Heuristic: Is this strictly a read? (Excludes RETURNING)
    this.reader =
      /^\s*(SELECT|PRAGMA|EXPLAIN)/i.test(sql) && !/RETURNING/i.test(sql);
    this.boundParams = [];

    // Row Modes
    this._pluck = false;
    this._raw = false;
    this._expand = false;
  }

  // --- Configuration ---

  pluck(toggle = true) {
    this._pluck = toggle;
    return this;
  }
  raw(toggle = true) {
    this._raw = toggle;
    return this;
  }
  expand(toggle = true) {
    this._expand = toggle;
    return this;
  }

  bind(...params) {
    this.boundParams = params;
    return this;
  }

  // --- Execution ---

  async run(...params) {
    const p = params.length ? params : this.boundParams;
    return this.db._requestWrite("run", {
      sql: this.source,
      params: p,
      options: this._getOptions(),
    });
  }

  async all(...params) {
    const p = params.length ? params : this.boundParams;
    const payload = {
      sql: this.source,
      params: p,
      options: this._getOptions(),
    };

    let result;
    // ROUTING LOGIC UPDATE:
    // Route to Writer if:
    // 1. Inside a Transaction
    // 2. It is a Write query (!reader)
    // 3. Database is In-Memory (Must use single shared writer)
    if (this.db._inTransaction || !this.reader || this.db.memory) {
      result = await this.db._requestWrite("all", payload);
    } else {
      result = await this.db._requestRead("all", payload);
    }

    // Smart Casting (Only for default object mode)
    if (!this._raw && !this._pluck && result.rows) {
      result.rows.forEach((row) => castRow(row, result.columns));
    }
    return result.rows;
  }

  async get(...params) {
    const rows = await this.all(...params);
    return rows ? rows[0] : undefined;
  }

  /**
   * Returns an async iterator that streams results.
   * Uses Backpressure to prevent memory issues.
   */
  async *iterate(...params) {
    const p = params.length ? params : this.boundParams;

    // MEMORY MODE CHECK
    if (this.db.memory) {
      // We cannot stream easily from the Writer because the Writer handles one message at a time
      // and streaming requires "locking" a worker statefully.
      // Fallback: Fetch ALL rows (standard execution) and yield them one by one.
      // This is acceptable for :memory: because RAM is already the limit.
      const rows = await this.all(...p);
      for (const row of rows) yield row;
      return;
    }

    const streamId = Math.random().toString(36).slice(2);

    // Must lock a specific reader for the stream duration
    const worker = this.db.readers.find((r) => !r.busy) || this.db.readers[0];
    worker.busy = true;

    const queue = [];
    let resolver = null;
    let active = true;

    const onMsg = (msg) => {
      // In production, ensure msg.streamId matches
      if (msg.status === "stream_data") {
        if (msg.error) {
          active = false;
          if (resolver) resolver.reject(new Error(msg.error));
          return;
        }
        if (msg.data) queue.push(...msg.data);
        if (msg.done) {
          active = false;
          queue.push(null); // EOF
        }
        if (resolver) {
          const r = resolver;
          resolver = null;
          r.resolve();
        }
      }
    };

    worker.worker.on("message", onMsg);

    try {
      worker.worker.postMessage({
        id: streamId,
        action: "stream_open",
        streamId,
        sql: this.source,
        params: p,
      });

      while (true) {
        if (queue.length === 0) {
          if (!active) break;
          await new Promise(
            (res, rej) => (resolver = { resolve: res, reject: rej }),
          );
        }

        const row = queue.shift();
        if (row === null) break;

        // Note: Casting logic omitted for stream brevity, but follows same pattern
        yield row;

        if (queue.length < 25 && active) {
          worker.worker.postMessage({
            id: streamId,
            action: "stream_ack",
            streamId,
          });
        }
      }
    } finally {
      worker.worker.off("message", onMsg);
      worker.worker.postMessage({ action: "stream_close", streamId });
      worker.busy = false;
    }
  }

  _getOptions() {
    return { pluck: this._pluck, raw: this._raw, expand: this._expand };
  }
}

module.exports = Statement;
