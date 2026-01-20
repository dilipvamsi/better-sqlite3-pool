/**
 * @file lib/connection.js
 * @description Represents an exclusive, locked session on the Writer worker.
 *
 * This class is the core of the Transaction/Session architecture.
 * It acts as a proxy to the Writer worker but bypasses the standard Mutex
 * acquisition because the lock is already held by the `Database.acquire()` call
 * that created this instance.
 *
 * SAFETY MECHANISMS:
 * 1. Hard Timeout (Max Life): Limits total duration of a transaction.
 * 2. Idle Timeout (Heartbeat): Limits time between commands (Client-side).
 * 3. Worker Heartbeat Sync: Reacts if the Worker kills the transaction.
 * 4. Leak Detection (GC): Emergency unlock if object goes out of scope.
 */

const Statement = require("./statement");
const { serializeAggregateOptions } = require("./utils");

// =============================================================================
// LEAK DETECTION REGISTRY
// =============================================================================

/**
 * Monitors Connection objects. If a Connection is Garbage Collected before
 * .release() is called, this callback fires to force-unlock the Writer.
 */
const leakRegistry = new FinalizationRegistry(({ writer, stack, listener }) => {
  console.error(
    `\n[better-sqlite3-pool] CRITICAL LEAK DETECTED!
    A write connection was acquired but never released.
    The object has been Garbage Collected.

    Automatic Recovery initiated (Rollback + Unlock).

    Origin of leak:
    ${stack}\n`,
  );

  // 0. Cleanup Listeners
  if (listener) {
    writer.off("transaction_timeout", listener);
  }

  // 1. Emergency Rollback (Fire and Forget)
  // We attempt to clean up the SQLite transaction state.
  // We use noLockExecute because the mutex is technically still held by the ghost connection.
  try {
    writer
      .noLockExecute({ action: "exec", sql: "ROLLBACK" })
      .catch((err) =>
        console.error(
          "[better-sqlite3-pool] Leak recovery rollback failed:",
          err.message,
        ),
      );
  } catch (e) {
    /* Ignore sync errors */
  }

  // 2. Force Unlock Mutex
  // This frees the writer for other requests.
  if (writer.lock && writer.isLocked()) {
    writer.unlock();
  }
});

// =============================================================================
// CONNECTION CLASS
// =============================================================================

/**
 * Internal token to prevent public constructor usage.
 */
const kConnectionInternal = Symbol("ConnectionInternal");

class Connection {
  /**
   * Factory method to acquire a lock and create a Connection instance.
   * This is the ONLY public way to get a connection.
   *
   * @param {import('./database').Database} db
   * @param {number} [maxLife=20000] - Hard limit (ms) for the session duration.
   * @param {number} [idleTimeout=5000] - Limit (ms) for inactivity between queries.
   */
  static async create(db, maxLife, idleTimeout) {
    // 1. Acquire the Writer Lock
    // We do this BEFORE instantiating the class.
    // If this hangs (deadlock), no object is created, no timers start.
    await db.writer.lock();

    // 2. Create Instance
    return new Connection(kConnectionInternal, db, maxLife, idleTimeout);
  }

  /**
   * Creates a new exclusive connection session.
   * **Internal Use Only**: Should be created via `db.acquire()`.
   *
   * @param {Symbol} token - Internal token to prevent public constructor usage.
   * @param {import('./database').Database} db - The parent database instance.
   * @param {number} [maxLife=20000] - Hard limit (ms) for the session duration.
   * @param {number} [idleTimeout=5000] - Limit (ms) for inactivity between queries.
   */
  constructor(token, db, maxLife = 20000, idleTimeout = 5000) {
    if (token !== kConnectionInternal) {
      throw new Error("Use 'await db.acquire()' to create a connection.");
    }

    /** @type {import('./database').Database} */
    this.db = db;

    /** @type {import('./worker-pool').SingleWorkerClient} */
    this.writer = db.writer;

    /** @type {boolean} Flag indicating if the connection is closed. */
    this._released = false;

    // Configuration
    this.idleLimit = idleTimeout;

    // --- 1. WORKER EVENT SYNC ---
    // If the Worker kills the transaction (due to its own internal heartbeat),
    // we must immediately unlock the mutex locally.
    this._onWorkerTimeout = this._handleWorkerTimeout.bind(this);
    this.writer.on("transaction_timeout", this._onWorkerTimeout);

    // --- 2. HARD LIMIT (Max Transaction Time) ---
    // Kills the transaction if it takes too long overall (e.g. infinite loop).
    this._lifeTimer = setTimeout(() => {
      this._forceRelease("Connection max life exceeded");
    }, maxLife);
    // Unref ensures this timer doesn't prevent Node from exiting
    if (this._lifeTimer.unref) this._lifeTimer.unref();

    // --- 3. IDLE LIMIT (Client Heartbeat) ---
    // Kills the transaction if the user stops sending commands (e.g. forgot release).
    this._idleTimer = null;
    this._resetIdleTimer();

    // --- 4. LEAK DETECTION ---
    // Register this instance. If it gets GC'd, we know we leaked.
    // We store the creation stack trace to help the user debug.
    leakRegistry.register(
      this,
      {
        writer: this.writer,
        stack: new Error().stack,
        listener: this._onWorkerTimeout,
      },
      this,
    );
  }

  /**
   * Modern Resource Management API.
   * Enables syntax: `await using conn = await db.acquire();`
   */
  async [Symbol.asyncDispose]() {
    this.release();
  }

  /**
   * Validates that the connection is still open and resets the heartbeat.
   * @throws {Error} If connection is released or timed out.
   * @private
   */
  _ensureActive() {
    if (this._released) {
      throw new Error("Connection has been released or timed out");
    }
    this.db._ensureOpen();

    // HEARTBEAT: Activity detected, reset the idle timer.
    this._resetIdleTimer();
  }

  /**
   * Resets the idle timer. Called on every interaction.
   * @private
   */
  _resetIdleTimer() {
    if (this._idleTimer) clearTimeout(this._idleTimer);

    // If connection is already released, don't start a new timer
    if (this._released) return;

    this._idleTimer = setTimeout(() => {
      this._forceRelease("Connection idle timeout (forgot to release?)");
    }, this.idleLimit);

    if (this._idleTimer.unref) this._idleTimer.unref();
  }

  // ===========================================================================
  // PUBLIC API (Mirrors Database)
  // ===========================================================================

  /**
   * Creates a prepared statement bound to this specific connection.
   * @param {string} sql - The SQL query.
   * @param {object} options - Additional options for the statement.
   * @param {boolean} [options.readonly] - Force route to Reader (true) or Writer (false)
   * @returns {Statement} A statement instance bound to this connection context.
   */
  prepare(sql, options) {
    this._ensureActive();
    // We pass 'this' (the Connection) as the context instead of the Database
    return new Statement(this, sql, options);
  }

  /**
   * Execute a simple SQL query (no result retrieval).
   * @param {string} sql - The SQL statement.
   * @returns {Promise<void>}
   */
  async exec(sql) {
    this._ensureActive();
    // Use noLockExecute because this Connection OWNS the lock
    return this.writer.noLockExecute({ action: "exec", sql });
  }

  // ===========================================================================
  // INTERNAL ROUTING (Used by Statement)
  // ===========================================================================

  /**
   * Routes a Write request (INSERT/UPDATE/DELETE/PRAGMA).
   * @param {string} action - The action type (e.g., 'run', 'exec').
   * @param {string|Object} sqlOrPayload - The SQL or statement payload.
   * @param {Array} params - Query parameters.
   * @returns {Promise<any>}
   * @private
   */
  async _requestWrite(action, sqlOrPayload, params) {
    this._ensureActive();
    const payload =
      typeof sqlOrPayload === "object"
        ? { action, ...sqlOrPayload }
        : { action, sql: sqlOrPayload, params };

    return this.writer.noLockExecute(payload);
  }

  /**
   * Routes a Read request (SELECT).
   * @param {'all'|'get'} action - The action type (e.g., 'all', 'get').
   * @param {string|Object} sqlOrPayload - The SQL or statement payload.
   * @param {Array} params - Query parameters.
   * @returns {Promise<any>}
   * @private
   */
  async _requestRead(action, sqlOrPayload, params) {
    this._ensureActive();
    const isObj = typeof sqlOrPayload === "object" && sqlOrPayload !== null;
    const payload = isObj
      ? { action, ...sqlOrPayload }
      : { action, sql: sqlOrPayload, params, options: undefined };
    return this.writer.noLockExecute(payload);
  }

  // ===========================================================================
  // LIFECYCLE MANAGEMENT
  // ===========================================================================

  /**
   * Releases the connection and unlocks the Writer.
   * Must be called when work is done.
   */
  release() {
    if (this._released) return;

    // Good Citizen: Unregister from leak detection since we closed properly
    leakRegistry.unregister(this);

    this._cleanup();
    this.writer.unlock();
  }

  /**
   * Handler for when the Worker thread forcibly closes the transaction.
   * @param {Error} error
   * @private
   */
  _handleWorkerTimeout(error) {
    if (this._released) return;

    // Unregister leak detection
    leakRegistry.unregister(this);

    console.warn(
      `[better-sqlite3-pool] Worker forcibly closed transaction: ${error.message}`,
    );

    // 1. Cleanup Timers & Listeners
    // We do NOT send ROLLBACK, because the worker already did it.
    this._cleanup();

    // 2. Unlock Node.js Mutex
    // This allows the next waiting request to use the worker
    this.writer.unlock();
  }

  /**
   * Emergency release method called by CLIENT-SIDE timers.
   * Logs a warning, attempts rollback, and unlocks.
   * @param {string} reason - The cause of the forced release.
   * @private
   */
  _forceRelease(reason) {
    if (this._released) return;

    // Unregister leak detection (we are handling it now)
    leakRegistry.unregister(this);

    console.warn(`[better-sqlite3-pool] ${reason}. Auto-releasing.`);

    // 1. Attempt Rollback
    // Fire-and-forget logic to clean up SQLite state
    this.writer
      .noLockExecute({ action: "exec", sql: "ROLLBACK" })
      .catch(() => {});

    // 2. Unlock Node.js Mutex
    this._cleanup();
    this.writer.unlock();
  }

  /**
   * Cleans up internal state, timers, and event listeners.
   * @private
   */
  _cleanup() {
    this._released = true;

    // Stop listening to the worker
    if (this.writer && this._onWorkerTimeout) {
      this.writer.off("transaction_timeout", this._onWorkerTimeout);
    }

    if (this._lifeTimer) {
      clearTimeout(this._lifeTimer);
      this._lifeTimer = null;
    }
    if (this._idleTimer) {
      clearTimeout(this._idleTimer);
      this._idleTimer = null;
    }
  }

  // ===========================================================================
  // CONFIGURATION & PLUGINS
  // ===========================================================================

  /**
   * Helper to execute configuration on the Writer (no lock) and Broadcast to Readers.
   */
  async _execConfig(payload) {
    this._ensureActive();
    const promises = [];

    // 1. Broadcast to Readers (Sticky to keep pool consistent)
    if (this.db.readerPool) {
      promises.push(this.db.readerPool.broadcast(payload, true));
    }

    // 2. Execute on Writer (No Lock, Non-Sticky)
    // We cannot easily make it sticky here without accessing private worker state,
    // and config changes inside transactions/sessions are often ephemeral.
    promises.push(this.writer.noLockExecute(payload));

    await Promise.all(promises);
  }

  /**
   * Set the encryption key for the database.
   * Must be called immediately after creation.
   * @param {string|Buffer} key
   */
  async key(key) {
    if (!Buffer.isBuffer(key) && typeof key !== "string") {
      throw new TypeError("Expected first argument to be a Buffer or String");
    }
    const payload = { action: "key", key };
    await this._execConfig(payload);
  }

  /**
   * Change the encryption key.
   * @param {string|Buffer} key
   */
  async rekey(key) {
    if (!Buffer.isBuffer(key) && typeof key !== "string") {
      throw new TypeError("Expected first argument to be a Buffer or String");
    }
    // 1. Writer: Executes 'rekey' (rewrites database)
    if (this.writer) {
      await this.writer.noLockExecute({ action: "rekey", key });
      // On the writer restart, we need key for the writer
      await this.writer.noLockExecute({ action: "key", key }, true);
    }

    // 2. Readers: Execute 'key' (update internal handle to read new format)
    // Readers cannot 'rekey' (write), so we just give them the new key.
    if (this.readerPool) {
      await this.readerPool.broadcast({ action: "key", key }, true);
    }
  }

  /**
   * Toggle default BigInt support for the database.
   * Broadcasts the setting to all workers.
   * @param {boolean} [toggleState=true]
   */
  async defaultSafeIntegers(toggleState = true) {
    const payload = { action: "default_safe_integers", state: toggleState };
    await this._execConfig(payload);
  }

  /**
   * Loads a compiled SQLite extension.
   * @param {string} path
   */
  async loadExtension(path) {
    const payload = { action: "load_extension", path };
    await this._execConfig(payload);
  }

  /** @typedef {import('better-sqlite3-multiple-ciphers').Database.RegistrationOptions} RegistrationOptions */

  /**
   * Register a User Defined Function (UDF).
   * Broadcasts the function to the Writer and all Readers.
   * Waits for acknowledgement from all workers to ensure consistency.
   * @param {string} name - The name of the SQL function.
   * @param {Function | RegistrationOptions} options - Function Registration Options.
   * @param {Function} [fn] - The JavaScript function to execute.
   * @returns {Promise<this>} The Database instance.
   */
  async function(name, options, fn) {
    // Argument shuffling to support optional 'options'
    let callback = fn;
    let opts = options;

    if (typeof options === "function") {
      callback = options;
      opts = {};
    }

    if (typeof name !== "string")
      throw new TypeError("Expected first argument to be a string");
    if (typeof callback !== "function")
      throw new TypeError("Expected second argument to be a function");

    if (!opts.varargs) {
      const len = callback.length;

      // Check for non-integers or negative numbers
      if (!Number.isInteger(len) || len < 0) {
        throw new TypeError(
          "Expected function.length to be a non-negative integer",
        );
      }

      // Check SQLite limit (max 100 arguments for UDFs)
      if (len > 100) {
        throw new RangeError(
          "User-defined functions cannot have more than 100 arguments",
        );
      }
    }

    const fnString = fn.toString();
    // Note: We don't push to db._initFunctions here because Connection is ephemeral.
    // If you want permanent functions, register them on the DB instance, not the Connection.

    const payload = {
      action: "function",
      fnName: name,
      fnString,
      fnOptions: opts,
    };

    await this._execConfig(payload);
    return this;
  }

  /**
   * Register a custom Aggregate Function.
   * Broadcasts to all workers.
   *
   * @param {string} aggName - Name of the aggregate function (e.g. 'MEDIAN').
   * @param {AggregateOptions} options - Configuration object (start, step, inverse, result).
   * @returns {Promise<this>}
   */
  async aggregate(aggName, options) {
    if (typeof aggName !== "string")
      throw new TypeError("Expected first argument to be a string");
    if (typeof options !== "object" || options === null)
      throw new TypeError("Expected second argument to be an options object");
    if (!options.step)
      throw new TypeError("Expected options.step to be a function");

    // Prepare payload
    const payload = {
      action: "aggregate",
      aggName,
      aggOptions: serializeAggregateOptions(options),
    };
    await this._execConfig(payload);
    return this;
  }

  /**
   * Serialize the database to a Buffer.
   * @param {SerializeOptions} [options] - { attached: string }
   * @returns {Promise<Buffer>}
   */
  async serialize(options) {
    this._ensureActive();
    // Always use the writer to get the most up-to-date state
    let result;
    if (this.readonly) {
      result = await this._requestRead("serialize", { options });
    } else {
      result = await this._requestWrite("serialize", { options });
    }
    return Buffer.from(result); // The worker returns the Buffer
  }

  /**
   * Execute a PRAGMA statement.
   * Broadcasts the setting to the Writer and all Readers.
   * Waits for acknowledgement from all workers.
   * @param {string} sql - The PRAGMA statement (e.g., "journal_mode = WAL").
   * @param {Object} [options] - Options.
   * @param {boolean} [options.simple] - If true, returns the first value of the first row.
   * @returns {Promise<void>} The result of the PRAGMA execution.
   */
  async pragma(sql, options = {}) {
    this._ensureActive(); // Use ensureActive for Connection context

    if (!sql) {
      throw new TypeError("SQL statement is required");
    } else if (typeof sql !== "string") {
      throw new TypeError("SQL statement must be a string");
    }

    if (options !== undefined && typeof options !== "object") {
      throw new TypeError("Options must be an object");
    } else if (
      options.simple !== undefined &&
      typeof options.simple !== "boolean"
    ) {
      throw new TypeError("Options.simple must be a boolean");
    }

    const trimmedSql = sql.trim();

    // Use specific 'pragma' action so worker uses db.pragma() instead of db.exec()
    const payload = { action: "pragma", sql, options };

    // READONLY MODE
    if (this.db.readonly) {
      // In readonly mode, we can only affect readers (e.g. cache_size)
      if (this.db.readerPool) {
        // Sticky broadcast ensuring new readers get this pragma
        const results = await this.db.readerPool.broadcast(payload, true);
        return results[0]?.pragma;
      }
      return;
    }

    // SPECIAL HANDLING FOR 'REKEY' PRAGMA
    if (/^rekey\s*=/i.test(trimmedSql)) {
      // 1. Readers: Convert 'rekey' to 'key'
      const readerSql = trimmedSql.replace(/^rekey/i, "key");
      const readerPayload = { action: "pragma", sql: readerSql, options };

      // 2. Writer: Execute as-is (Perform rewrite)
      const writerRes = await this.writer.noLockExecute(payload);

      // On the writer restart, we need key for the writer
      await this.writer.noLockExecute(readerPayload, true);

      if (this.readerPool) {
        await this.readerPool.broadcast(readerPayload, true);
      }

      return writerRes.pragma;
    }

    // SPECIAL HANDLING FOR 'JOURNAL_MODE' PRAGMA
    // ONLY Execute on Writer. DO NOT Broadcast.
    // Readers automatically pick up the mode from the file header.
    // Broadcasting causes errors (readers cannot change mode) or redundancy.
    if (/^journal_mode\s*=/i.test(trimmedSql)) {
      // Note: Not 'Sticky' because the file header persists this setting.
      const writerRes = await this.writer.noLockExecute(payload, false);
      return writerRes.pragma;
    }

    // WRITE MODE
    // 1. Execute on Writer (Primary) - This returns the actual pragma result
    const writerRes = await this.writer.noLockExecute(payload);

    // 2. Sync Readers (Sticky)
    if (this.db.readerPool) {
      await this.db.readerPool.broadcast(payload, true);
    }

    return writerRes.pragma;
  }

  /**
   * Register a Virtual Table.
   * Broadcasts the table definition to the Writer and all Reader workers.
   *
   * @param {string} name - The name of the virtual table.
   * @param {Function} factory - A function that returns the VirtualTableOptions object.
   * @returns {Promise<this>} The Database instance.
   */
  async table(name, factory) {
    this._ensureActive();

    if (typeof name !== "string")
      throw new TypeError("Expected first argument to be a string");
    if (name.length === 0)
      throw new TypeError("Expected table name to be a non-empty string");
    if (
      typeof factory !== "function" &&
      (typeof factory !== "object" || factory === null)
    ) {
      throw new TypeError(
        "Expected second argument to be a function or a module object",
      );
    }

    let factoryString;
    let isEponymous = false;

    if (typeof factory === "object") {
      // 1. Synchronous Validation (Mimic native better-sqlite3 strictness)
      if (!Array.isArray(factory.columns)) {
        if (factory.columns === undefined)
          throw new TypeError("Expected columns to be an array");
        throw new TypeError("Expected columns to be an array");
      }
      if (factory.columns.length === 0)
        throw new RangeError("Expected columns to be a non-empty array");

      const seen = new Set();
      for (const col of factory.columns) {
        if (typeof col !== "string")
          throw new TypeError("Expected column names to be strings");
        if (seen.has(col)) throw new TypeError("Duplicate column name");
        seen.add(col);
      }

      // If it is present, it MUST be an array. undefined/null are not arrays.
      if ("parameters" in factory && !Array.isArray(factory.parameters)) {
        throw new TypeError("Expected parameters to be an array");
      }

      if (factory.parameters !== undefined) {
        if (!Array.isArray(factory.parameters))
          throw new TypeError("Expected parameters to be an array");
        if (factory.parameters.length > 32)
          throw new RangeError("Too many parameters");
        const seenParams = new Set();
        for (const param of factory.parameters) {
          if (typeof param !== "string")
            throw new TypeError("Expected parameter names to be strings");
          if (seenParams.has(param))
            throw new TypeError("Duplicate parameter name");
          seenParams.add(param);
        }
      }

      if (typeof factory.rows !== "function")
        throw new TypeError("Expected rows to be a generator function");

      const len = factory.rows.length;
      if (!Number.isInteger(len) || len < 0)
        throw new TypeError(
          "Expected function.length to be a non-negative integer",
        );
      if (len > 32)
        throw new RangeError("Virtual table module has too many parameters");

      // 2. Manual Serialization for Objects (Eponymous Wrapper)
      const cols = JSON.stringify(factory.columns);
      // CRITICAL FIX: Only include 'parameters' if it exists.
      // better-sqlite3 throws if 'parameters' is present but undefined.
      const paramsLine = factory.parameters
        ? `parameters: ${JSON.stringify(factory.parameters)},`
        : "";
      const rowsStr = factory.rows.toString().trim();

      // FIX: Correctly determine if rowsStr is a Method Definition or Function Expression
      let methodPart;

      // If it starts with 'function', '(', 'async function', or 'async (', it needs a key "rows:"
      // If it starts with 'rows', '*rows', 'async rows', it INCLUDES the key (Method Shorthand).
      if (
        rowsStr.startsWith("function") ||
        rowsStr.startsWith("(") ||
        rowsStr.startsWith("async function") ||
        rowsStr.startsWith("async (")
      ) {
        methodPart = `rows: ${rowsStr}`;
      } else {
        // Assume Method Shorthand (e.g. "*rows() {}", "rows() {}")
        // This must be pasted AS IS into the object literal.
        methodPart = rowsStr;
      }

      factoryString = `() => ({
         columns: ${cols},
         ${paramsLine}
         ${methodPart}
       })`;

      isEponymous = true;
    } else {
      // Function (Module)
      factoryString = factory.toString();
      isEponymous = false;
    }

    const payload = { action: "table", name, factoryString, isEponymous };
    await this._execConfig(payload);
    return this;
  }

  /**
   * Attaches an external database within this connection context.
   * NOTE: This broadcasts the ATTACH command to the entire pool to maintain schema consistency.
   *
   * @param {string} filename - Path to the database file.
   * @param {string} alias - Alias name for the attached database.
   * @param {Object} [options] - Configuration options.
   * @param {string} [options.journalMode] - Optional. Sets the journal_mode for the attached DB.
   * @param {boolean} [options.fileMustExist=false] - If true, throws if the file does not exist.
   */
  async attach(filename, alias, options = {}) {
    this._ensureActive();
    if (typeof filename !== "string")
      throw new TypeError("Filename must be a string");
    if (typeof alias !== "string")
      throw new TypeError("Alias must be a string");

    if (options.fileMustExist) {
      const exists = await fileExists(filename);
      if (!exists) {
        throw new SqliteError(
          `Attached database file "${filename}" does not exist`,
          "SQLITE_CANTOPEN",
        );
      }
    }

    const sql = `ATTACH DATABASE '${filename}' AS ${alias}`;

    // Broadcast via _execConfig (Sticky)
    await this._execConfig({ action: "exec", sql });

    if (options.journalMode) {
      if (typeof options.journalMode !== "string") {
        throw new TypeError("options.journalMode must be a string");
      }
      const mode = options.journalMode.toUpperCase();
      const pragmaSql = `PRAGMA ${alias}.journal_mode = ${mode}`;

      // Execute on the specific writer connection this object holds
      await this.writer.noLockExecute({ action: "exec", sql: pragmaSql });
    }
  }

  /**
   * Detaches a database.
   * NOTE: This broadcasts the DETACH command to the entire pool.
   *
   * @param {string} alias
   */
  async detach(alias) {
    this._ensureActive();
    if (typeof alias !== "string")
      throw new TypeError("Alias must be a string");

    const sql = `DETACH DATABASE ${alias}`;
    await this._execConfig({ action: "exec", sql });
  }
}

module.exports = Connection;
