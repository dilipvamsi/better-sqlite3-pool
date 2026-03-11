/**
 * @file adapter.d.ts
 * @description Type definitions for the sqlite3 compatibility adapter.
 */

import { EventEmitter } from "events";
import { IDatabase } from "./lib/types";

declare namespace adapter {
  /**
   * Standard callback for SQLite3 operations.
   * @template T The expected result type.
   */
  export type SqliteCallback<T> = (this: RunContext, err: Error | null, result?: T) => void;

  /**
   * The 'this' context for callbacks in write operations (run).
   * Contains metadata about the operation results.
   */
  export interface RunContext {
    /** The ID of the last row inserted into the database. */
    lastID: number | bigint;
    /** The number of rows modified by the operation. */
    changes: number;
  }

  /**
   * Compatibility adapter class that mimics the `sqlite3.Database` API.
   * Internally uses the better-sqlite3-pool connection pool for better performance.
   */
  export class Database extends EventEmitter {
    /**
     * Creates a new compatibility database instance.
     * @param filename Path to the database file.
     * @param callback Optional callback called when the database is opened.
     */
    constructor(filename: string, callback?: (err: Error | null) => void);
    /**
     * Creates a new compatibility database instance with opening mode.
     * @param filename Path to the database file.
     * @param mode SQLite opening mode flags.
     * @param callback Optional callback called when the database is opened.
     */
    constructor(filename: string, mode?: number, callback?: (err: Error | null) => void);

    /**
     * The underlying better-sqlite3-pool Database instance.
     */
    readonly db: IDatabase;

    /**
     * Executes a SQL query that does not return results (INSERT, UPDATE, DELETE).
     * @param sql The SQL query string.
     * @param callback Optional callback with run metadata available in 'this'.
     */
    run(sql: string, callback?: SqliteCallback<void>): this;
    /**
     * Executes a SQL query with parameters.
     * @param sql The SQL query string.
     * @param params Binding parameters (array or object).
     * @param callback Optional callback.
     */
    run(sql: string, params: any, callback?: SqliteCallback<void>): this;

    /**
     * Executes a SQL query and returns the first result row.
     * @param sql The SQL query string.
     * @param callback Callback called with the result row.
     */
    get<T = any>(sql: string, callback?: SqliteCallback<T>): this;
    /**
     * Executes a SQL query with parameters and returns the first result row.
     * @param sql The SQL query string.
     * @param params Binding parameters.
     * @param callback Callback called with the result row.
     */
    get<T = any>(sql: string, params: any, callback?: SqliteCallback<T>): this;

    /**
     * Executes a SQL query and returns all result rows.
     * @param sql The SQL query string.
     * @param callback Callback called with an array of result rows.
     */
    all<T = any>(sql: string, callback?: SqliteCallback<T[]>): this;
    /**
     * Executes a SQL query with parameters and returns all result rows.
     * @param sql The SQL query string.
     * @param params Binding parameters.
     * @param callback Callback called with an array of result rows.
     */
    all<T = any>(sql: string, params: any, callback?: SqliteCallback<T[]>): this;

    /**
     * Executes a SQL query and calls a callback for each result row.
     * @param sql The SQL query string.
     * @param callback Callback called once for each row.
     * @param complete Optional callback called when the query is finished.
     */
    each<T = any>(sql: string, callback?: SqliteCallback<T>, complete?: (err: Error | null, count: number) => void): this;
    /**
     * Executes a SQL query with parameters and calls a callback for each row.
     * @param sql The SQL query string.
     * @param params Binding parameters.
     * @param callback Callback called once for each row.
     * @param complete Optional callback called when the query is finished.
     */
    each<T = any>(sql: string, params: any, callback?: SqliteCallback<T>, complete?: (err: Error | null, count: number) => void): this;

    /**
     * Executes multiple SQL statements in a single string.
     * @param sql The SQL statements.
     * @param callback Optional callback called when finished.
     */
    exec(sql: string, callback?: (err: Error | null) => void): this;

    /**
     * Closes the database.
     * @param callback Optional callback called when the database is closed.
     */
    close(callback?: (err: Error | null) => void): void;

    /**
     * Forces sequential execution of queries within the callback.
     * Note: In this adapter, this is largely a no-op as consistency is managed by the pool.
     */
    serialize(callback?: () => void): void;

    /**
     * Allows parallel execution of queries within the callback.
     * Note: In this adapter, this is always true.
     */
    parallelize(callback?: () => void): void;
  }

  /**
   * Enables verbose mode for the adapter, providing more detailed error messages.
   * Returns the adapter namespace to mimic the sqlite3 behavior.
   */
  export function verbose(): typeof adapter;
}

export = adapter;
