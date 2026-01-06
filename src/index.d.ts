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

    /** The SQLiteError class used by this database */
    static SqliteError: typeof Database.SqliteError;
    /** The Statement class used by this database */
    static Statement: typeof Database.Statement;
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

declare module "better-sqlite3-pool/adapter" {
  import MainDatabase = require("better-sqlite3-pool");

  export interface RunResult {
    changes: number;
    lastID: number | bigint;
  }

  export class Database {
    /** Access the underlying better-sqlite3-pool instance */
    readonly db: MainDatabase;

    constructor(
      filename: string,
      mode?: number,
      callback?: (err: Error | null) => void
    );

    run(
      sql: string,
      callback?: (this: RunResult, err: Error | null) => void
    ): void;

    run(
      sql: string,
      params: any[],
      callback?: (this: RunResult, err: Error | null) => void
    ): void;

    all(sql: string, callback?: (err: Error | null, rows: any[]) => void): void;
    all(
      sql: string,
      params: any[],
      callback?: (err: Error | null, rows: any[]) => void
    ): void;

    close(callback?: (err: Error | null) => void): void;
  }

  export function verbose(): any;
}
