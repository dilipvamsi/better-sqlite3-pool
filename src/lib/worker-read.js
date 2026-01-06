/**
 * @file lib/worker-read.js
 * @description Dedicated Worker for READ operations with Streaming support.
 */
const { parentPort, workerData } = require("node:worker_threads");
const Database = require("better-sqlite3-multiple-ciphers");
const { deserializeFunction } = require("./utils");

const db = new Database(workerData.filename, { readonly: true });
db.pragma("busy_timeout = 5000");
db.defaultSafeIntegers(true);

// Store active iterators for streaming
const activeStreams = new Map();

parentPort.on(
  "message",
  ({ id, action, sql, params, streamId, options, fnName, fnString }) => {
    try {
      // 1. Handle UDF Registration (Synced from Manager)
      if (action === "function") {
        const fn = deserializeFunction(fnString);
        db.function(fnName, fn);
        if (id !== -1) parentPort.postMessage({ id, status: "success" });
        return;
      }

      // 2. Handle Pragma (Synced from Manager)
      if (action === "exec") {
        db.exec(sql);
        if (id !== -1) parentPort.postMessage({ id, status: "success" });
        return;
      }

      // 3. Handle Streaming
      if (action === "stream_open") {
        const stmt = db.prepare(sql);
        const iterator = stmt.iterate(...params);
        activeStreams.set(streamId, iterator);
        pumpStream(id, streamId, iterator); // Send first batch
        return;
      }

      if (action === "stream_ack") {
        const iterator = activeStreams.get(streamId);
        if (iterator) pumpStream(id, streamId, iterator);
        return;
      }

      if (action === "stream_close") {
        const iterator = activeStreams.get(streamId);
        if (iterator && iterator.return) iterator.return();
        activeStreams.delete(streamId);
        return;
      }

      // 4. Handle Standard Reads
      const stmt = db.prepare(sql);

      // Apply Row Modes
      if (options) {
        if (options.pluck) stmt.pluck(true);
        if (options.raw) stmt.raw(true);
        if (options.expand) stmt.expand(true);
      }

      const rows = stmt.all(...params);
      const columns =
        options && (options.raw || options.pluck) ? [] : stmt.columns();

      parentPort.postMessage({
        id,
        status: "success",
        data: { rows, columns },
      });
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

/**
 * Reads a small batch from the iterator and pushes to main thread.
 */
function pumpStream(msgId, streamId, iterator) {
  const BATCH_SIZE = 50; // Keep RAM usage low
  const rows = [];
  let done = false;

  try {
    for (let i = 0; i < BATCH_SIZE; i++) {
      const next = iterator.next();
      if (next.done) {
        done = true;
        break;
      }
      rows.push(next.value);
    }

    if (done) activeStreams.delete(streamId);

    // Send batch with special status 'stream_data'
    parentPort.postMessage({
      id: msgId,
      status: "stream_data",
      data: rows,
      done,
    });
  } catch (err) {
    parentPort.postMessage({ id: msgId, status: "error", error: err.message });
  }
}
