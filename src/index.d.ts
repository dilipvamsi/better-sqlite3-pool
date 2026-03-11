/**
 * @file index.d.ts
 * @description Main entry point for better-sqlite3-pool type definitions.
 */

/// <reference types="node" />

import { EventEmitter } from "events";
import _Statement = require("./lib/statement");
import _Connection = require("./lib/connection");
import * as _adapter from "./adapter";
import * as _types from "./lib/types";

/**
 * The main Database class that acts as a connection pool manager.
 * It manages a single Writer worker and multiple Reader workers to achieve
 * high concurrency without blocking the main event loop.
 */
declare class Database extends EventEmitter {
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
   * @param filename Path to the database file.
   * @param options Configuration options for the pool.
   * @returns A promise that resolves to the initialized Database instance.
   */
  static create(filename: string, options?: Database.Options): Promise<Database>;

  /** Private constructor to prevent direct instantiation. Use Database.create() instead. */
  private constructor();

  /**
   * Creates a new prepared Statement.
   * Statements are proxies that execute queries on worker threads.
   * @param sql The SQL query string.
   * @param options Preparation options.
   */
  prepare<BindParameters extends any[] | {} = any[]>(
    sql: string,
    options?: Database.PrepareOptions
  ): Database.Statement<BindParameters>;

  /**
   * Wraps a function to execute within a database transaction.
   * Multiple calls to the returned function will be queued and executed serially on the writer worker.
   * @param fn The function to wrap in a transaction.
   * @returns An async function that executes the transaction.
   */
  transaction<F extends (...args: any[]) => any>(fn: F): Database.Transaction<F>;

  /**
   * Executes a simple SQL string (INSERT, UPDATE, DELETE, DDL) that doesn't return results.
   * @param sql The SQL string to execute.
   */
  exec(sql: string): Promise<void>;

  /**
   * Executes a PRAGMA statement.
   * @param sql The PRAGMA statement.
   * @param options Pragma options.
   */
  pragma(sql: string, options?: Database.PragmaOptions): Promise<any>;

  /**
   * Acquires an exclusive lock on the Writer worker.
   * Returns a Connection object that can be used for sensitive or multi-step operations.
   */
  acquire(): Promise<Database.Connection>;

  /**
   * Attaches an external database to the current connection pool.
   * @param filename Path to the database file to attach.
   * @param alias The alias name for the attached database.
   * @param options Attachment options.
   */
  attach(filename: string, alias: string, options?: Database.AttachOptions): Promise<void>;

  /**
   * Detaches a secondary database from the connection pool.
   * @param alias The alias of the database to detach.
   */
  detach(alias: string): Promise<void>;

  /**
   * Registers a User Defined Function (UDF).
   * @param name Name of the function in SQL.
   * @param cb The Javascript function implementation.
   */
  function(name: string, cb: (...args: any[]) => any): Promise<this>;
  /**
   * Registers a User Defined Function (UDF) with options.
   * @param name Name of the function in SQL.
   * @param options Registration options.
   * @param cb The Javascript function implementation.
   */
  function(name: string, options: Database.RegistrationOptions, cb: (...args: any[]) => any): Promise<this>;

  /**
   * Registers a custom Aggregate Function.
   * @param name Name of the aggregate function in SQL.
   * @param options Aggregate implementation options (step, result, etc.).
   */
  aggregate(name: string, options: Database.AggregateOptions): Promise<this>;

  /**
   * Registers a Virtual Table module.
   * @param name Name of the virtual table module.
   * @param factory Factory function that produces the virtual table implementation.
   */
  table(name: string, factory: Database.VirtualTableFactory): Promise<this>;

  /**
   * Creates a backup of the database file.
   * @param destination Path to the destination backup file.
   * @param options Backup options.
   */
  backup(destination: string, options?: Database.BackupOptions): Promise<Database.BackupMetadata>;

  /**
   * Serializes the database (or an attached database) to a Buffer.
   * @param options Serialization options.
   */
  serialize(options?: Database.SerializeOptions): Promise<Buffer>;

  /**
   * Sets the encryption key for the database.
   * @param key The encryption pass-phrase or buffer.
   */
  key(key: string | Buffer): Promise<void>;

  /**
   * Changes the encryption key for the database.
   * @param newKey The new encryption pass-phrase or buffer.
   */
  rekey(newKey: string | Buffer): Promise<void>;

  /**
   * Loads a compiled SQLite extension.
   * @param path Path to the extension file.
   */
  loadExtension(path: string): Promise<void>;

  /**
   * Enables or disables unsafe mode (allowing write operations in readers, etc.).
   * Use with extreme caution.
   */
  unsafeMode(unsafe?: boolean): Promise<this>;

  /**
   * Dynamically resizes the reader worker pool.
   * @param min Minimum number of reader workers.
   * @param max Maximum number of reader workers.
   */
  pool(min: number, max: number): Promise<void>;

  /**
   * Closes the database pool and terminates all worker threads.
   */
  close(): Promise<this>;

  /**
   * Configures the default behavior for handling large integers (returning BigInt vs Number).
   * @param toggle Whether to enable safe integers by default.
   */
  defaultSafeIntegers(toggle?: boolean): Promise<void>;
}

declare namespace Database {
  export type Options = _types.Options;
  export type PrepareOptions = _types.PrepareOptions;
  export type PragmaOptions = _types.PragmaOptions;
  export type AttachOptions = _types.AttachOptions;
  export type RegistrationOptions = _types.RegistrationOptions;
  export type AggregateOptions = _types.AggregateOptions;
  export type VirtualTableFactory = _types.VirtualTableFactory;
  export type SerializeOptions = _types.SerializeOptions;
  export type BackupOptions = _types.BackupOptions;
  export type BackupMetadata = _types.BackupMetadata;
  export type RunResult = _types.RunResult;
  export type ColumnDefinition = _types.ColumnDefinition;

  /**
   * A wrapped transaction function.
   */
  export interface Transaction<F extends (...args: any[]) => any> {
    (...args: Parameters<F>): Promise<ReturnType<F>>;
    /** Executes the transaction with standard (default) behavior. */
    default: Transaction<F>;
    /** Executes a DEFERRED transaction. */
    deferred: Transaction<F>;
    /** Executes an IMMEDIATE transaction. */
    immediate: Transaction<F>;
    /** Executes an EXCLUSIVE transaction. */
    exclusive: Transaction<F>;
    /** Reference to the Database instance that created this transaction. */
    database: Database;
  }

  /**
   * Statement class for prepared queries.
   */
  export const Statement: typeof _Statement;
  export type Statement<BindParameters extends any[] | {} = any[]> = _Statement<BindParameters>;

  /**
   * Connection class for exclusive sessions.
   */
  export const Connection: typeof _Connection;
  export type Connection = _Connection;

  /**
   * Access to the sqlite3 compatibility adapter.
   */
  export const adapter: typeof _adapter;

  /**
   * Error thrown by SQLite operations.
   */
  export class SqliteError extends Error {
    /** The error code (e.g., 'SQLITE_CONSTRAINT'). */
    code: string;
    constructor(message: string, code: string);
  }

  /**
   * Re-export Database as a member of itself to support { Database } named import.
   */
  export { Database };
}

/**
 * Standard CommonJS-compatible export.
 * Supports:
 *   const Database = require("better-sqlite3-pool");
 *   import Database from "better-sqlite3-pool";
 *   import { Database } from "better-sqlite3-pool";
 *   import * as Database from "better-sqlite3-pool";
 */
export = Database;
