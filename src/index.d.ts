/**
 * @package better-sqlite3-pool
 * Main TypeScript definitions.
 */

declare module "better-sqlite3-pool" {
  // 1. The Class Definition
  class Database {
    constructor(filename: string, options?: Database.Options);

    prepare(sql: string): Database.Statement;

    exec(sql: string): Promise<void>;

    /** Dynamically resize the reader pool. */
    pool(min: number, max: number): Promise<void>;

    /** execute a PRAGMA. Broadcasts to all workers. */
    pragma(sql: string, options?: { simple?: boolean }): Promise<any>;

    /** Register a User Defined Function. Broadcasts to all workers. */
    function(name: string, fn: (...args: any[]) => any): Promise<this>;

    close(): Promise<void>;
  }

  // 2. The Namespace (For inner types and classes)
  namespace Database {
    export interface Options {
      /** Minimum number of reader workers (default: 2) */
      min?: number;
      /** Maximum number of reader workers (default: 8) */
      max?: number;
    }

    export interface RunResult {
      changes: number;
      lastInsertRowid: number | bigint;
    }

    export class SqliteError extends Error {
      constructor(message: string, code: string);
      code: string;
    }

    export class Statement {
      /** Toggle returning raw arrays instead of objects */
      raw(toggle?: boolean): this;
      /** Toggle returning only the first column value */
      pluck(toggle?: boolean): this;
      /** Toggle nested result objects */
      expand(toggle?: boolean): this;

      bind(...params: any[]): this;

      all<T = any>(...params: any[]): Promise<T[]>;
      get<T = any>(...params: any[]): Promise<T | undefined>;
      run(...params: any[]): Promise<RunResult>;
      iterate<T = any>(...params: any[]): AsyncIterableIterator<T>;
    }
  }

  // 3. The Export Assignment
  export = Database;
}

// Type definitions for the SQLite3Adapter compatibility layer
// This matches the node-sqlite3 API structure.
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

    /**
     * Close the database connection.
     */
    close(callback?: (err: Error | null) => void): void;

    /**
     * Execute a query that does NOT return rows (INSERT, UPDATE, DELETE).
     */
    run(sql: string, callback?: SqliteCallback<void>): this;
    run(sql: string, params: any, callback?: SqliteCallback<void>): this;

    /**
     * Execute a query and return ALL rows (SELECT).
     */
    all<T = any>(
      sql: string,
      callback?: (this: RunContext, err: Error | null, rows: T[]) => void,
    ): this;
    all<T = any>(
      sql: string,
      params: any,
      callback?: (this: RunContext, err: Error | null, rows: T[]) => void,
    ): this;

    /**
     * Execute a query and execute a callback for EACH row.
     * This streams data from the worker pool.
     */
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

    /**
     * Execute a query and return only the FIRST row.
     * (Optional: You must implement .get() in adapter.js if you want strict compatibility)
     */
    get<T = any>(
      sql: string,
      callback?: (this: RunContext, err: Error | null, row: T) => void,
    ): this;
    get<T = any>(
      sql: string,
      params: any,
      callback?: (this: RunContext, err: Error | null, row: T) => void,
    ): this;
  }

  export const verbose: () => { Database: typeof Database };
}
