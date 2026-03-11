/**
 * @file statement.d.ts
 * @description Type definitions for prepared statements in better-sqlite3-pool.
 */

import { Database } from "../index";
import { RunResult, ColumnDefinition } from "./types";

/**
 * A prepared statement proxy.
 * Statements are executed on worker threads to prevent blocking the event loop.
 */
declare class Statement<BindParameters extends any[] | {} = any[]> {
  /** The database connection manager that created this statement. */
  readonly database: Database;
  /** The SQL source string used to create this statement. */
  readonly source: string;
  /** Whether the statement is purely for reading data. */
  readonly reader: boolean;
  /** Whether the statement was opened in read-only mode. */
  readonly readonly: boolean;
  /** Whether the statement is currently executing a query. */
  readonly busy: boolean;

  /**
   * Executes the statement and returns metadata about the result.
   * Best for INSERT, UPDATE, or DELETE operations.
   */
  run(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): Promise<RunResult>;

  /**
   * Executes the statement and returns the first result row.
   * If the query returns no rows, returns undefined.
   */
  get<T = any>(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): Promise<T | undefined>;

  /**
   * Executes the statement and returns an array of all result rows.
   */
  all<T = any>(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): Promise<T[]>;

  /**
   * Executes the statement and returns an async iterator over the result rows.
   */
  iterate<T = any>(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): AsyncIterableIterator<T>;

  /**
   * Binds the given parameters to the statement and returns it.
   * Subsequent executions will use these parameters.
   */
  bind(...params: BindParameters extends any[] ? BindParameters : [BindParameters]): this;

  /**
   * Configures the statement to return only the first column of each row.
   */
  pluck(toggle?: boolean): this;

  /**
   * Configures the statement to return results as expanded objects (namespaced by table).
   */
  expand(toggle?: boolean): this;

  /**
   * Configures the statement to return results as arrays instead of objects.
   */
  raw(toggle?: boolean): this;

  /**
   * Configures whether the statement should return BigInts or regular numbers for 64-bit integers.
   */
  safeIntegers(toggle?: boolean): this;

  /**
   * Returns an array of ColumnDefinition objects describing the result set columns.
   */
  columns(): ColumnDefinition[];
}

export = Statement;
