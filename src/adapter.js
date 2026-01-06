/**
 * @file adapter.js
 * @description SQLite3-compatible interface for TypeORM.
 */
const Database = require("./index");

class SQLite3Adapter {
  constructor(filename, mode, callback) {
    this.db = new Database(filename);
    this._inTransaction = false;
    if (callback) setTimeout(() => callback(null), 0);
  }

  run(sql, params, cb) {
    this._exec("run", sql, params, cb);
  }
  all(sql, params, cb) {
    this._exec("all", sql, params, cb);
  }

  async _exec(method, sql, params, cb) {
    if (typeof params === "function") {
      cb = params;
      params = [];
    }

    try {
      const s = sql.trim().toUpperCase();

      // Transaction Handling: Lock on BEGIN, Unlock on COMMIT/ROLLBACK
      if (s.startsWith("BEGIN")) {
        await this.db.writeMutex.acquire();
        this.db._inTransaction = true;
      }

      let result;
      if (method === "run") {
        result = await this.db.prepare(sql).run(...params);
      } else {
        result = await this.db.prepare(sql).all(...params);
      }

      // Unlock logic (Avoid unlocking on SAVEPOINT rollback)
      if (
        s.startsWith("COMMIT") ||
        (s.startsWith("ROLLBACK") && !s.includes("TO SAVEPOINT"))
      ) {
        this.db._inTransaction = false;
        this.db.writeMutex.release();
      }

      if (cb) {
        const ctx = {};
        // TypeORM LastID Context
        if (result && result.lastInsertRowid !== undefined) {
          //Pass raw value (Number or BigInt)
          ctx.lastID = result.lastInsertRowid;
        }
        if (result && result.changes) ctx.changes = result.changes;

        // Pass result (Smart Cast already applied by Database)
        cb.call(ctx, null, result);
      }
    } catch (err) {
      if (cb) cb(err);
    }
  }

  close(cb) {
    this.db.close().then(() => cb && cb(null));
  }
}

module.exports = {
  Database: SQLite3Adapter,
  // If .verbose() didn't exist, TypeORM would crash here.
  verbose: () => module.exports,
};
