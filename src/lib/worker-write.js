/**
 * @file lib/worker-write.js
 * @description Dedicated Worker for all WRITE operations.
 */
const { parentPort, workerData } = require("node:worker_threads");
const Database = require("better-sqlite3-multiple-ciphers");
const { deserializeFunction } = require("./utils");

const db = new Database(workerData.filename);

// WAL Mode allows readers to work while we write.
db.pragma("journal_mode = WAL");

// Wait 5s for external locks (backups, etc) before crashing.
db.pragma("busy_timeout = 5000");

// Always read BigInts initially. Main thread handles casting.
db.defaultSafeIntegers(true);

// SIGNAL READY: Tell main thread the file exists
parentPort.postMessage({ status: 'ready' });

parentPort.on(
  "message",
  ({ id, action, sql, params, options, fnName, fnString }) => {
    try {
      // 1. Handle UDF Registration
      if (action === "function") {
        const fn = deserializeFunction(fnString);
        db.function(fnName, fn);
        if (id !== -1) parentPort.postMessage({ id, status: "success" });
        return;
      }

      // 2. Handle Pragma/Exec
      if (action === "exec") {
        db.exec(sql);
        if (id !== -1) parentPort.postMessage({ id, status: "success" });
        return;
      }

      // 3. Handle Standard Queries (run/get/all)
      const stmt = db.prepare(sql);

      // Apply Row Modes
      if (options) {
        if (options.pluck) stmt.pluck(true);
        if (options.raw) stmt.raw(true);
        if (options.expand) stmt.expand(true);
      }

      let data;
      if (action === "run") {
        const info = stmt.run(...params);
        data = {
          lastInsertRowid: info.lastInsertRowid,
          changes: info.changes,
        };
      } else {
        // Writes can also read (RETURNING clauses or reads inside transactions)
        const rows = stmt[action](...params); // action = 'all' | 'get'

        // Only fetch columns if we need to map types (not needed for raw/pluck usually)
        const columns =
          options && (options.raw || options.pluck) ? [] : stmt.columns();
        data = { rows, columns };
      }

      parentPort.postMessage({ id, status: "success", data });
    } catch (err) {
      if (id !== -1) {
        parentPort.postMessage({
          id,
          status: "error",
          error: {
            message: err.message,
            code: err.code, // CRITICAL: Pass the SQLITE_ code
          },
        });
      }
    }
  },
);
