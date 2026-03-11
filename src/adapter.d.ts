/**
 * @file adapter.d.ts
 * @description TypeScript definitions for the sqlite3 compatibility adapter.
 */

import { EventEmitter } from "events";
import { Database as PoolDatabase } from "./index";

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
 * The Adapter class that mimics node-sqlite3's Database class API.
 * Useful for using better-sqlite3-pool with libraries designed for sqlite3.
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
  readonly db: PoolDatabase;

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
  ): any;

  serialize(callback?: () => void): void;
  parallelize(callback?: () => void): void;

  close(callback?: (err: Error | null) => void): void;

  on(event: "trace" | "profile", listener: (...args: any[]) => void): this;
  on(event: string, listener: (...args: any[]) => void): this;
}

export const verbose: () => { Database: typeof Database };
