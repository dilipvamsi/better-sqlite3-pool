/**
 * @file index.d.ts
 * @description TypeScript definitions for better-sqlite3-pool.
 */

/// <reference types="node" />

import { EventEmitter } from "events";

declare module "better-sqlite3-pool" {
  export class Database extends EventEmitter {
    /**
     * The path to the database file.
     */
    readonly name: string;

    /**
     * Whether the database is open.
     */
    readonly open: boolean;

    /**
     * Whether the database is in read-only mode.
     */
    readonly readonly: boolean;

    /**
     * Whether the database is currently in a transaction.
     */
    readonly inTransaction: boolean;

    /**
     * Creates and initializes a new Database connection pool.
     * @param filename Path to the database file or ':memory:'.
     * @param options Configuration options.
     */
    static create(
      filename: string,
      options?: Database.Options,
    ): Promise<Database>;

    /**
     * Private constructor. Use `Database.create()` instead.
     */
    private constructor();

    /**
     * Creates a new prepared Statement.
     * @param sql The SQL statement string.
     * @param options Statement configuration options.
     */
    prepare<BindParameters extends any[] | {} = any[]>(
      sql: string,
      options?: Database.PrepareOptions,
    ): Database.Statement<BindParameters>;

    /**
     * Executes a transaction function.
     * Returns a wrapper function that automatically begins/commits a transaction.
     * @param fn The function to wrap in a transaction.
     */
    transaction<F extends (...args: any[]) => any>(
      fn: F,
    ): Database.Transaction<F>;

    /**
     * Execute a simple SQL string (INSERT, UPDATE, DELETE, DDL).
     * Does not return rows.
     */
    exec(sql: string): Promise<void>;

    /**
     * Execute a PRAGMA statement.
     * Broadcasts the setting to all workers.
     * @param sql The PRAGMA string (e.g., "journal_mode = WAL").
     * @param options formatting options.
     */
    pragma(sql: string, options?: Database.PragmaOptions): Promise<any>;

    /**
     * Acquires a lock on the Writer worker and returns an exclusive Connection.
     * Useful for manual transactions or sequential operations.
     * @returns An exclusive connection session.
     */
    acquire(): Promise<Database.Connection>;

    /**
     * Register a User Defined Function (UDF).
     * Broadcasts to all workers.
     */
    function(name: string, cb: (...args: any[]) => any): Promise<this>;
    function(
      name: string,
      options: Database.RegistrationOptions,
      cb: (...args: any[]) => any,
    ): Promise<this>;

    /**
     * Register a custom Aggregate Function.
     */
    aggregate(name: string, options: Database.AggregateOptions): Promise<this>;

    /**
     * Register a Virtual Table.
     * @param name Table name.
     * @param factory Factory function or module object.
     */
    table(name: string, factory: Database.VirtualTableFactory): Promise<this>;

    /**
     * Create a backup of the database file.
     */
    backup(
      destination: string,
      options?: Database.BackupOptions,
    ): Promise<Database.BackupMetadata>;

    /**
     * Serialize the database to a Buffer.
     */
    serialize(options?: Database.SerializeOptions): Promise<Buffer>;

    /**
     * Set the encryption key.
     * @param key Buffer or string passphrase.
     */
    key(key: string | Buffer): Promise<void>;

    /**
     * Change the encryption key.
     * @param newKey Buffer or string passphrase.
     */
    rekey(newKey: string | Buffer): Promise<void>;

    /**
     * Load a compiled SQLite extension.
     */
    loadExtension(path: string): Promise<void>;

    /**
     * Enable or disable unsafe mode (writable schema, etc).
     */
    unsafeMode(unsafe?: boolean): Promise<this>;

    /**
     * Resize the reader worker pool.
     */
    pool(min: number, max: number): Promise<void>;

    /**
     * Close the database pool and terminate all workers.
     */
    close(): Promise<this>;

    /**
     * Set default BigInt handling behavior.
     */
    defaultSafeIntegers(toggle?: boolean): Promise<void>;
  }

  namespace Database {
    export class SqliteError extends Error {
      code: string;
      constructor(message: string, code: string);
    }

    export interface Options {
      /** Minimum number of reader workers (default: 1) */
      minWorkers?: number;
      /** Maximum number of reader workers (default: 2) */
      maxWorkers?: number;
      /** Open in read-only mode (default: false) */
      readonly?: boolean;
      /** Throw if file does not exist (default: false) */
      fileMustExist?: boolean;
      /** SQLite busy timeout in ms (default: 5000) */
      timeout?: number;
      /** Max transaction duration in ms (default: 30000) */
      transactionTimeout?: number;
      /** Path to native binding */
      nativeBinding?: string;
      /** Verbose logging function */
      verbose?: (message?: any, ...additionalArgs: any[]) => void;
      /** Max inactivity for manual connection (default: 5000) */
      connectionIdleTimeout?: number;
      /** Max life for manual connection (default: 60000) */
      connectionMaxLife?: number;
    }

    export interface PrepareOptions {
      /** Force routing to Writer (false) or Reader (true). Default: Auto-detected. */
      readonly?: boolean;
    }

    export interface RunResult {
      changes: number;
      lastInsertRowid: number | bigint;
    }

    export interface PragmaOptions {
      simple?: boolean;
    }

    export interface RegistrationOptions {
      varargs?: boolean;
      deterministic?: boolean;
      safeIntegers?: boolean;
      directOnly?: boolean;
    }

    export interface AggregateOptions {
      start?: any;
      step: (total: any, next: any) => any;
      inverse?: (total: any, dropped: any) => any;
      result?: (total: any) => any;
      varargs?: boolean;
      deterministic?: boolean;
      safeIntegers?: boolean;
    }

    export interface BackupOptions {
      progress?: (info: BackupMetadata) => number;
      attached?: string;
    }

    export interface BackupMetadata {
      totalPages: number;
      remainingPages: number;
    }

    export interface SerializeOptions {
      attached?: string;
    }

    export type VirtualTableFactory = (
      this: void,
      ...args: any[]
    ) =>
      | {
          rows: GeneratorFunction;
          columns: string[];
          parameters?: string[];
        }
      | any; // Module object

    export interface Transaction<F extends (...args: any[]) => any> {
      (...args: Parameters<F>): Promise<ReturnType<F>>;
      default: Transaction<F>;
      deferred: Transaction<F>;
      immediate: Transaction<F>;
      exclusive: Transaction<F>;
      database: Database;
    }

    export class Statement<BindParameters extends any[] | {} = any[]> {
      readonly database: Database;
      readonly source: string;
      readonly reader: boolean;
      readonly readonly: boolean;
      readonly busy: boolean;

      run(
        ...params: BindParameters extends any[]
          ? BindParameters
          : [BindParameters]
      ): Promise<RunResult>;
      get<T = any>(
        ...params: BindParameters extends any[]
          ? BindParameters
          : [BindParameters]
      ): Promise<T | undefined>;
      all<T = any>(
        ...params: BindParameters extends any[]
          ? BindParameters
          : [BindParameters]
      ): Promise<T[]>;
      iterate<T = any>(
        ...params: BindParameters extends any[]
          ? BindParameters
          : [BindParameters]
      ): AsyncIterableIterator<T>;

      bind(
        ...params: BindParameters extends any[]
          ? BindParameters
          : [BindParameters]
      ): this;
      pluck(toggle?: boolean): this;
      expand(toggle?: boolean): this;
      raw(toggle?: boolean): this;
      safeIntegers(toggle?: boolean): this;
      columns(): ColumnDefinition[];
    }

    export interface ColumnDefinition {
      name: string;
      column: string | null;
      table: string | null;
      database: string | null;
      type: string | null;
    }

    export class Connection {
      readonly db: Database;

      /** Release the lock and return connection to pool */
      release(): void;

      /** Async dispose support */
      [Symbol.asyncDispose](): Promise<void>;

      prepare(sql: string): Statement;
      exec(sql: string): Promise<void>;

      pragma(sql: string, options?: PragmaOptions): Promise<any>;

      // Connection also supports these for convenience within session
      key(key: string | Buffer): Promise<void>;
      rekey(key: string | Buffer): Promise<void>;
      loadExtension(path: string): Promise<void>;
    }
  }

  export = Database;
}

// =============================================================================
// ADAPTER MODULE (sqlite3 compatibility)
// =============================================================================

declare module "better-sqlite3-pool/adapter" {
  import { EventEmitter } from "events";
  import MainDatabase from "better-sqlite3-pool";

  /**
   * Standard callback for SQLite3 operations.
   */
  export type SqliteCallback<T = any> = (
    this: RunContext,
    err: Error | null,
    row?: T,
  ) => void;

  /**
   * Context bound to the 'this' keyword in callbacks for write operations.
   */
  export interface RunContext {
    lastID?: number | bigint;
    changes?: number;
  }

  /**
   * The Adapter class that mimics node-sqlite3's Database class.
   */
  export class Database extends EventEmitter {
    /**
     * Opens the database connection pool.
     * @param filename Path to database file or ':memory:'
     * @param mode Optional mode (ignored, strictly for compatibility)
     * @param callback Optional callback when opened
     */
    constructor(filename: string, callback?: (err: Error | null) => void);
    constructor(
      filename: string,
      mode?: number,
      callback?: (err: Error | null) => void,
    );

    /** Access the underlying better-sqlite3-pool instance */
    readonly db: MainDatabase;

    run(sql: string, callback?: SqliteCallback<void>): this;
    run(sql: string, params: any, callback?: SqliteCallback<void>): this;

    get<T = any>(sql: string, callback?: SqliteCallback<T>): this;
    get<T = any>(sql: string, params: any, callback?: SqliteCallback<T>): this;

    all<T = any>(
      sql: string,
      callback?: (this: RunContext, err: Error | null, rows: T[]) => void,
    ): this;
    all<T = any>(
      sql: string,
      params: any,
      callback?: (this: RunContext, err: Error | null, rows: T[]) => void,
    ): this;

    each<T = any>(
      sql: string,
      callback?: (this: RunContext, err: Error | null, row: T) => void,
      complete?: (err: Error | null, count: number) => void,
    ): this;
    each<T = any>(
      sql: string,
      params: any,
      callback?: (this: RunContext, err: Error | null, row: T) => void,
      complete?: (err: Error | null, count: number) => void,
    ): this;

    exec(sql: string, callback?: (err: Error | null) => void): this;

    prepare(
      sql: string,
      params?: any,
      callback?: (err: Error | null) => void,
    ): any; // Statement wrapper

    serialize(callback?: () => void): void;
    parallelize(callback?: () => void): void;

    close(callback?: (err: Error | null) => void): void;

    on(event: "trace" | "profile", listener: (...args: any[]) => void): this;
    on(event: string, listener: (...args: any[]) => void): this;
  }

  export const verbose: () => { Database: typeof Database };
}
