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

    /** execute a PRAGMA. Broadcasts to all workers. */
    pragma(sql: string, options?: { simple?: boolean }): Promise<any>;

    /** Register a User Defined Function. Broadcasts to all workers. */
    function(name: string, fn: (...args: any[]) => any): this;

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

  // 3. The Export Assignment (Matches module.exports = Database)
  export = Database;
}

declare module "better-sqlite3-pool/adapter" {
  // 1. What is this?
  // It defines the metadata returned after an INSERT/UPDATE/DELETE.
  export interface RunResult {
    changes: number;
    lastID: number | bigint;
  }

  export class Database {
    constructor(
      filename: string,
      mode?: number,
      callback?: (err: Error | null) => void
    );

    // 2. Where is it used?
    // It is used as the 'this' context in the callback for run().
    // We explicitly type 'this' so TypeScript knows about .lastID and .changes

    run(
      sql: string,
      callback?: (this: RunResult, err: Error | null) => void
    ): void;

    run(
      sql: string,
      params: any[],
      callback?: (this: RunResult, err: Error | null) => void
    ): void;

    // 'all' (SELECT) uses a second argument 'rows', not 'this' context.
    all(sql: string, callback?: (err: Error | null, rows: any[]) => void): void;
    all(
      sql: string,
      params: any[],
      callback?: (err: Error | null, rows: any[]) => void
    ): void;

    close(callback?: (err: Error | null) => void): void;
  }

  // Shim for compatibility
  export function verbose(): any;
}
