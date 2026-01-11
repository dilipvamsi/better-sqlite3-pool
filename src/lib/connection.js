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
 * 2. Idle Timeout (Heartbeat): limits time between commands.
 * 3. Leak Detection (GC): Emergency unlock if object goes out of scope.
 */

const Statement = require("./statement");

// =============================================================================
// LEAK DETECTION REGISTRY
// =============================================================================

/**
 * Monitors Connection objects. If a Connection is Garbage Collected before
 * .release() is called, this callback fires to force-unlock the Writer.
 */
const leakRegistry = new FinalizationRegistry(({ writer, stack }) => {
  console.error(
    `\n[better-sqlite3-pool] CRITICAL LEAK DETECTED!
    A write connection was acquired but never released.
    The object has been Garbage Collected.

    Automatic Recovery initiated (Rollback + Unlock).

    Origin of leak:
    ${stack}\n`,
  );

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
   * @param {import('./database').Database} db - The parent database instance.
   * @param {number} [maxLife=60000] - Hard limit (ms) for the session duration.
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
   * @param {number} [maxLife=60000] - Hard limit (ms) for the session duration.
   * @param {number} [idleTimeout=5000] - Limit (ms) for inactivity between queries.
   */
  constructor(token, db, maxLife = 60000, idleTimeout = 5000) {
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

    // --- 1. HARD LIMIT (Max Transaction Time) ---
    // Kills the transaction if it takes too long overall (e.g. infinite loop).
    this._lifeTimer = setTimeout(() => {
      this._forceRelease("Connection max life exceeded");
    }, maxLife);
    // Unref ensures this timer doesn't prevent Node from exiting
    if (this._lifeTimer.unref) this._lifeTimer.unref();

    // --- 2. IDLE LIMIT (Heartbeat) ---
    // Kills the transaction if the user stops sending commands (e.g. forgot release).
    this._idleTimer = null;
    this._resetIdleTimer();

    // --- 3. LEAK DETECTION ---
    // Register this instance. If it gets GC'd, we know we leaked.
    // We store the creation stack trace to help the user debug.
    leakRegistry.register(
      this,
      {
        writer: this.writer,
        stack: new Error().stack,
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
   * @returns {Statement} A statement instance bound to this connection context.
   */
  prepare(sql) {
    this._ensureActive();
    // We pass 'this' (the Connection) as the context instead of the Database
    return new Statement(this, sql);
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
   * Emergency release method called by timers.
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
   * Cleans up internal state and timers.
   * @private
   */
  _cleanup() {
    this._released = true;
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
    const payload = { action: "key", key };
    await this._execConfig(payload);
  }

  /**
   * Change the encryption key.
   * @param {string|Buffer} key
   */
  async rekey(key) {
    const payload = { action: "rekey", key };
    await this._execConfig(payload);
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

    const fnString = fn.toString();
    this._initFunctions.push({ name, fnString });
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
    this._ensureOpen();
    // Always use the writer to get the most up-to-date state
    const result = await this._requestWrite("serialize", { options });
    return result; // The worker returns the Buffer
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
    this._ensureOpen();

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

    // Use specific 'pragma' action so worker uses db.pragma() instead of db.exec()
    const payload = { action: "pragma", sql, options };

    // READONLY MODE
    if (this.readonly) {
      if (this.readerPool) {
        // Sticky broadcast ensuring new readers get this pragma
        const results = await this.db.readerPool.broadcast(payload, true);
        // Return the result from the first worker (they should all be identical)
        return results[0].pragma;
      }
      return;
    }

    // WRITE MODE
    // 1. Execute on Writer (Primary) - This returns the actual pragma result
    const writerRes = await this.writer.noLockExecute(payload);

    // Sync Readers (Sticky)
    if (this.readerPool) {
      await this.readerPool.broadcast(payload, true);
    }

    return writerRes.pragma;
  }

  /**
   * Register a Virtual Table.
   * Broadcasts the table definition to the Writer and all Reader workers.
   *
   * @warning The `factory` function is serialized to a string and executed inside the worker threads.
   * Therefore, it **MUST be strictly self-contained** (pure). It cannot reference variables
   * from the parent scope (closures) or external libraries not available in the worker context.
   *
   * @example
   * await db.table('my_vtab', function() {
   *   return {
   *     rows: function* () { yield [1, 'a']; },
   *     columns: ['id', 'name']
   *   };
   * });
   *
   * @param {string} name - The name of the virtual table.
   * @param {Function} factory - A function that returns the VirtualTableOptions object.
   * @returns {Promise<this>} The Database instance.
   */
  async table(name, factory) {
    const factoryString = factory.toString();
    const payload = { action: "table", name, factoryString };
    await this._execConfig(payload);
    return this;
  }
}

module.exports = Connection;
