/**
 * @file index.d.ts
 * @description TypeScript definitions for better-sqlite3-pool.
 * A thread-safe connection pool for SQLite using Node.js Worker Threads.
 */

/// <reference types="node" />

import { EventEmitter } from "events";
import * as _adapter from "./adapter";

/**
 * The main Database class that acts as a connection pool manager.
 * It manages a dedicated Writer worker and a pool of Reader workers.
 */
export class Database extends EventEmitter {
  /** The absolute path to the database file. */
  readonly name: string;
  /** Indicates if the database connection pool is currently open. */
  readonly open: boolean;
  /** Indicates if the database was opened in read-only mode. */
  readonly readonly: boolean;
  /** Indicates if the current async execution context is inside a transaction. */
  readonly inTransaction: boolean;

  /**
   * Asynchronously creates and initializes a new Database connection pool.
   * This spawns workers and waits for them to be ready.
   *
   * @param filename - Path to the SQLite database file or ':memory:'.
   * @param options - Configuration options for the pool and SQLite connection.
   * @returns A promise that resolves to the initialized Database instance.
   */
  static create(filename: string, options?: Database.Options): Promise<Database>;

  /**
   * @private
   * Private constructor to prevent direct instantiation.
   * Use `await Database.create(filename, options)` instead.
   */
  private constructor();

  /**
   * Creates a new prepared Statement.
   *
   * @template BindParameters The tuple type of parameters expected by the statement.
   * @param sql - The SQL statement string.
   * @param options - specific configuration for this statement.
   * @returns A proxy Statement instance that routes queries to the appropriate worker.
   */
  prepare<BindParameters extends any[] | {} = any[]>(
    sql: string,
    options?: Database.PrepareOptions
  ): Database.Statement<BindParameters>;

  /**
   * Wraps a function to execute within a database transaction.
   *
   * - If called inside an existing transaction, it creates a SAVEPOINT (nested transaction).
   * - If called at the top level, it acquires an exclusive lock on the Writer worker.
   * - Automatically commits if the function returns successfully, or rolls back if it throws.
   *
   * @param fn - The async function to execute.
   * @returns A wrapper function that matches the signature of `fn`.
   */
  transaction<F extends (...args: any[]) => any>(fn: F): Database.Transaction<F>;

  /**
   * Executes a simple SQL string (INSERT, UPDATE, DELETE, DDL).
   * This method does **not** return rows. Use `.prepare().all()` for retrieving data.
   *
   * @param sql - The SQL string to execute.
   */
  exec(sql: string): Promise<void>;

  /**
   * Executes a PRAGMA statement.
   * Broadcasts the setting to the Writer and all Reader workers to ensure consistency.
   *
   * @param sql - The PRAGMA string (e.g., "journal_mode = WAL").
   * @param options - Formatting options.
   * @returns The result of the PRAGMA execution.
   */
  pragma(sql: string, options?: Database.PragmaOptions): Promise<any>;

  /**
   * Acquires an exclusive lock on the Writer worker.
   * Returns a `Connection` object that holds the lock until `release()` is called.
   *
   * Use this for:
   * 1. Manual transaction control (BEGIN/COMMIT).
   * 2. Performing multiple write operations that must be sequential.
   * 3. Using `await using` (AsyncDisposable).
   *
   * @returns An exclusive connection session.
   */
  acquire(): Promise<Database.Connection>;

  /**
   * Attaches an external database to the current connection pool.
   *
   * - The ATTACH command is broadcast to the Writer and all Reader workers.
   * - The configuration is marked as "sticky", meaning new workers created later will also attach this DB.
   *
   * @param filename - Path to the secondary database file.
   * @param alias - The alias name to use in SQL queries (e.g. `SELECT * FROM alias.table`).
   * @param options - Attachment options.
   */
  attach(filename: string, alias: string, options?: Database.AttachOptions): Promise<void>;

  /**
   * Detaches a secondary database from the connection pool.
   * Broadcasts the command to all workers.
   *
   * @param alias - The alias of the database to detach.
   */
  detach(alias: string): Promise<void>;

  /**
   * Registers a User Defined Function (UDF).
   * The function is serialized and broadcast to all workers.
   *
   * @param name - The name of the function in SQL.
   * @param cb - The JavaScript function to execute.
   */
  function(name: string, cb: (...args: any[]) => any): Promise<this>;
  /**
   * Registers a User Defined Function (UDF) with options.
   *
   * @param name - The name of the function in SQL.
   * @param options - Registration options (deterministic, varargs, etc).
   * @param cb - The JavaScript function to execute.
   */
  function(name: string, options: Database.RegistrationOptions, cb: (...args: any[]) => any): Promise<this>;

  /**
   * Registers a custom Aggregate Function.
   *
   * @param name - The name of the aggregate function in SQL.
   * @param options - Configuration object containing step/result functions.
   */
  aggregate(name: string, options: Database.AggregateOptions): Promise<this>;

  /**
   * Registers a Virtual Table module.
   *
   * @param name - The name of the virtual table.
   * @param factory - A factory function or module object defining the table behavior.
   */
  table(name: string, factory: Database.VirtualTableFactory): Promise<this>;

  /**
   * Creates a backup of the database file.
   *
   * @param destination - The destination file path.
   * @param options - Backup configuration (progress callback, attached DB name).
   */
  backup(destination: string, options?: Database.BackupOptions): Promise<Database.BackupMetadata>;

  /**
   * Serializes the database (or an attached database) to a Buffer.
   */
  serialize(options?: Database.SerializeOptions): Promise<Buffer>;

  /**
   * Sets the encryption key for the database.
   * Must be called immediately after opening if the database is encrypted.
   *
   * @param key - The encryption key or passphrase.
   */
  key(key: string | Buffer): Promise<void>;

  /**
   * Changes the encryption key for the database.
   *
   * @param newKey - The new encryption key or passphrase.
   */
  rekey(newKey: string | Buffer): Promise<void>;

  /**
   * Loads a compiled SQLite extension.
   *
   * @param path - The path to the extension file (e.g. .so, .dll, .dylib).
   */
  loadExtension(path: string): Promise<void>;

  /**
   * Enables or disables unsafe mode (e.g. `writable_schema`).
   * Use with caution.
   *
   * @param unsafe - Whether to enable unsafe mode. Default `true`.
   */
  unsafeMode(unsafe?: boolean): Promise<this>;

  /**
   * Dynamically resizes the reader worker pool.
   *
   * @param min - New minimum number of readers.
   * @param max - New maximum number of readers.
   */
  pool(min: number, max: number): Promise<void>;

  /**
   * Closes the database pool.
   * - Waits for pending queries to finish (graceful shutdown).
   * - Terminates all worker threads.
   * - Checkpoints WAL file.
   */
  close(): Promise<this>;

  /**
   * Configures the default behavior for handling large integers.
   *
   * @param toggle - If true, SQLite integers that exceed 53-bit precision are returned as JavaScript `BigInt`.
   */
  defaultSafeIntegers(toggle?: boolean): Promise<void>;
}

/**
 * Merged namespace for auxiliary types.
 */
export namespace Database {
  /**
   * Access to the sqlite3 compatibility adapter.
   */
  export const adapter: typeof _adapter;

  /**
   * Error thrown by SQLite operations.
   */
  export class SqliteError extends Error {
    code: string;
    constructor(message: string, code: string);
  }

  /**
   * Configuration options for `Database.create()`.
   */
  export interface Options {
    /** Minimum number of reader workers to keep alive. Default: 1 */
    minWorkers?: number;
    /** Maximum number of reader workers allowed. Default: 2 */
    maxWorkers?: number;
    /** Open the database in read-only mode. Default: false */
    readonly?: boolean;
    /** If true, throws an error if the database file does not exist. Default: false */
    fileMustExist?: boolean;
    /**
     * The number of milliseconds to wait when locking the database.
     * Default: 5000
     */
    timeout?: number;
    /**
     * Max duration (ms) for an idle transaction before auto-rollback by the worker.
     * Default: 30000
     */
    transactionTimeout?: number;
    /** Path to the `better-sqlite3` native binding if not in standard location. */
    nativeBinding?: string;
    /** Function to handle verbose logging from SQLite (e.g. `console.log`). */
    verbose?: (message?: any, ...additionalArgs: any[]) => void;
    /**
     * Max duration (ms) of inactivity for a manual `Connection` before it is forcibly closed.
     * Default: 5000
     */
    connectionIdleTimeout?: number;
    /**
     * Max total duration (ms) for a manual `Connection` lifecycle.
     * Default: 60000
     */
    connectionMaxLife?: number;
  }

  /**
   * Options for `db.prepare()`.
   */
  export interface PrepareOptions {
    /**
     * Manually route the query.
     * - `true`: Force execution on a Reader worker.
     * - `false`: Force execution on the Writer worker.
     * - `undefined`: Auto-detect based on SQL (e.g. SELECT -> Reader, INSERT -> Writer).
     */
    readonly?: boolean;
  }

  /**
   * Options for `db.attach()`.
   */
  export interface AttachOptions {
    /**
     * Optional journal mode to set immediately on the attached database (e.g. 'WAL').
     * Executed only on the Writer to perform the file header change.
     */
    journalMode?: string;
    /**
     * If true, throws an error if the attached file does not exist.
     * If false (default), SQLite creates the file if missing.
     */
    fileMustExist?: boolean;
  }

  /**
   * Result of a write operation (`run()`).
   */
  export interface RunResult {
    /** The number of rows modified. */
    changes: number;
    /** The ROWID of the last inserted row. */
    lastInsertRowid: number | bigint;
  }

  /**
   * Options for `db.pragma()`.
   */
  export interface PragmaOptions {
    /** If true, returns only the first value of the first row (simplifies result). */
    simple?: boolean;
  }

  /**
   * Options for `db.function()`.
   */
  export interface RegistrationOptions {
    /** Allow the function to accept a variable number of arguments. */
    varargs?: boolean;
    /** Hint that the function result depends only on its arguments (optimizes query planner). */
    deterministic?: boolean;
    /** Arguments are passed as BigInt if they exceed 53-bit precision. */
    safeIntegers?: boolean;
    /** Function can only be used in direct SQL, not triggers/views (security). */
    directOnly?: boolean;
  }

  /**
   * Options for `db.aggregate()`.
   */
  export interface AggregateOptions {
    /** Initial value for the accumulator. */
    start?: any;
    /** Function called for each row. `(accumulator, value) => newAccumulator` */
    step: (total: any, next: any) => any;
    /** Function called to remove a row from the window (for window functions). */
    inverse?: (total: any, dropped: any) => any;
    /** Function called to compute the final result from the accumulator. */
    result?: (total: any) => any;
    varargs?: boolean;
    deterministic?: boolean;
    safeIntegers?: boolean;
  }

  /**
   * Options for `db.backup()`.
   */
  export interface BackupOptions {
    /** Callback for progress updates. Return non-zero to continue, 0 to cancel. */
    progress?: (info: BackupMetadata) => number;
    /** The alias of the attached database to backup (default is 'main'). */
    attached?: string;
  }

  /**
   * Info passed to the backup progress callback.
   */
  export interface BackupMetadata {
    totalPages: number;
    remainingPages: number;
  }

  /**
   * Options for `db.serialize()`.
   */
  export interface SerializeOptions {
    /** The alias of the attached database to serialize. */
    attached?: string;
  }

  /**
   * Factory type for Virtual Tables.
   */
  export type VirtualTableFactory = (this: void, ...args: any[]) => | { rows: GeneratorFunction; columns: string[]; parameters?: string[]; } | any;

  /**
   * A wrapper function returned by `db.transaction()`.
   */
  export interface Transaction<F extends (...args: any[]) => any> {
    /** Executes the transaction. */
    (...args: Parameters<F>): Promise<ReturnType<F>>;
    /** Uses 'DEFERRED' behavior (default). Locks when the first write occurs. */
    default: Transaction<F>;
    /** Uses 'DEFERRED' behavior. */
    deferred: Transaction<F>;
    /** Uses 'IMMEDIATE' behavior. Locks the DB immediately for writing. */
    immediate: Transaction<F>;
    /** Uses 'EXCLUSIVE' behavior. Prevents other readers immediately. */
    exclusive: Transaction<F>;
    /** Access to the parent database instance. */
    database: Database;
  }

  /**
   * A prepared statement proxy.
   */
  export class Statement<BindParameters extends any[] | {} = any[]> {
    readonly database: Database;
    /** The original SQL source string. */
    readonly source: string;
    /** Whether this statement is classified as a read operation. */
    readonly reader: boolean;
    /** Whether this statement allows running on Read-Only connections. */
    readonly readonly: boolean;
    /** Whether the statement is currently executing/streaming. */
    readonly busy: boolean;

    /**
     * Executes the statement (INSERT, UPDATE, DELETE).
     * @param params - Bind parameters.
     */
    run(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): Promise<RunResult>;

    /**
     * Executes the statement and returns the first row (SELECT).
     * @param params - Bind parameters.
     */
    get<T = any>(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): Promise<T | undefined>;

    /**
     * Executes the statement and returns all rows (SELECT).
     * @param params - Bind parameters.
     */
    all<T = any>(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): Promise<T[]>;

    /**
     * Executes the statement and returns an async iterator (Streaming SELECT).
     * @param params - Bind parameters.
     */
    iterate<T = any>(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): AsyncIterableIterator<T>;

    /**
     * Binds parameters to the statement permanently.
     */
    bind(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): this;

    /** If true, `.get()` returns the first column value, `.all()` returns array of values. */
    pluck(toggle?: boolean): this;
    /** If true, 'table.col' fields are nested into objects. */
    expand(toggle?: boolean): this;
    /** If true, returns raw arrays `[val, val]` instead of objects `{col: val}`. */
    raw(toggle?: boolean): this;
    /** If true, huge integers are returned as BigInts. */
    safeIntegers(toggle?: boolean): this;
    /** Returns the column metadata (available after first execution). */
    columns(): ColumnDefinition[];
  }

  /**
   * Metadata for a single column in a result set.
   */
  export interface ColumnDefinition {
    name: string;
    column: string | null;
    table: string | null;
    database: string | null;
    type: string | null;
  }

  /**
   * An exclusive session on the Writer worker.
   * Obtained via `db.acquire()`.
   */
  export class Connection {
    readonly db: Database;
    /**
     * Releases the exclusive lock and returns the worker to the pool.
     * **Must be called** when work is done.
     */
    release(): void;
    /**
     * Support for `await using` (AsyncDisposable).
     * Automatically calls `release()` when scope ends.
     */
    [Symbol.asyncDispose](): Promise<void>;

    /**
     * Creates a new prepared Statement.
     */
    prepare(sql: string): Statement;
    /**
     * Executes a simple SQL string.
     */
    exec(sql: string): Promise<void>;
    /**
     * Executes a PRAGMA statement.
     */
    pragma(sql: string, options?: PragmaOptions): Promise<any>;
    /**
     * Attaches an external database within this connection context.
     */
    attach(filename: string, alias: string, options?: AttachOptions): Promise<void>;
    /**
     * Detaches a database within this connection context.
     */
    detach(alias: string): Promise<void>;

    // Configuration methods that mirror Database but execute on this connection
    key(key: string | Buffer): Promise<void>;
    rekey(key: string | Buffer): Promise<void>;
    loadExtension(path: string): Promise<void>;
    function(name: string, cb: (...args: any[]) => any): Promise<this>;
    function(name: string, options: RegistrationOptions, cb: (...args: any[]) => any): Promise<this>;
    aggregate(name: string, options: AggregateOptions): Promise<this>;
    table(name: string, factory: VirtualTableFactory): Promise<this>;
    serialize(options?: SerializeOptions): Promise<Buffer>;
  }
}

// Named exports for convenient direct imports
export const adapter: typeof _adapter;
export const SqliteError: typeof Database.SqliteError;
export const Connection: typeof Database.Connection;
export const Statement: typeof Database.Statement;

// Default export
export default Database;
