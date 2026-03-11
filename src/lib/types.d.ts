/**
 * @file types.d.ts
 * @description Shared interfaces and types for better-sqlite3-pool.
 * This file helps break circular dependencies between components.
 */

/// <reference types="node" />

import { EventEmitter } from "events";

/**
 * Result of a database modification operation (INSERT, UPDATE, DELETE).
 */
export interface RunResult {
  /** The number of rows modified by the operation. */
  changes: number;
  /** The ID of the last row inserted into the database. */
  lastInsertRowid: number | bigint;
}

/**
 * Metadata about a column in a result set.
 */
export interface ColumnDefinition {
  /** The name (or alias) of the column. */
  name: string;
  /** The original name of the column in the database table. */
  column: string | null;
  /** The name of the table that the column belongs to. */
  table: string | null;
  /** The name of the database that the column belongs to. */
  database: string | null;
  /** The declared type of the column. */
  type: string | null;
}

/**
 * Configuration options for creating a new Database connection pool.
 */
export interface Options {
  /** Minimum number of reader workers to keep alive in the pool. Defaults to 1. */
  minWorkers?: number;
  /** Maximum number of reader workers allowed in the pool. Defaults to CPU count or 2. */
  maxWorkers?: number;
  /** Whether to open the database in read-only mode. Defaults to false. */
  readonly?: boolean;
  /** If true, throws an error if the database file does not exist. Defaults to false. */
  fileMustExist?: boolean;
  /** The number of milliseconds to wait for a database lock before throwing a timeout error. Defaults to 5000ms. */
  timeout?: number;
  /** How long a transaction can remain idle before being automatically rolled back. Defaults to 30000ms. */
  transactionTimeout?: number;
  /** Path to a custom native binding for better-sqlite3. */
  nativeBinding?: string;
  /** A function called with every SQL string executed by the database. */
  verbose?: (message?: any, ...additionalArgs: any[]) => void;
  /** How long a manual connection can remain idle before being returned to the pool. Defaults to 5000ms. */
  connectionIdleTimeout?: number;
  /** Maximum total lifespan of a manual connection before it must be closed. Defaults to 60000ms. */
  connectionMaxLife?: number;
}

/**
 * Options for preparing a new SQL statement.
 */
export interface PrepareOptions {
  /** Whether the statement should be executed in read-only mode. Defaults to false. */
  readonly?: boolean;
}

/**
 * Options for executing PRAGMA statements.
 */
export interface PragmaOptions {
  /** If true, returns the first column of the first row directly instead of an object. Defaults to false. */
  simple?: boolean;
}

/**
 * Options for attaching a secondary database file.
 */
export interface AttachOptions {
  /** The journal mode for the attached database (e.g., 'DELETE', 'WAL'). */
  journalMode?: string;
  /** If true, throws if the database file to be attached does not exist. Defaults to false. */
  fileMustExist?: boolean;
}

/**
 * Options for registering a custom function.
 */
export interface RegistrationOptions {
  /** Whether the function can accept a variable number of arguments. Defaults to false. */
  varargs?: boolean;
  /** Whether the function is deterministic (returns the same output for the same input). Defaults to false. */
  deterministic?: boolean;
  /** Whether the function should handle 64-bit integers safely. Defaults to false. */
  safeIntegers?: boolean;
  /** If true, prohibits the function from being used inside triggers or views. Defaults to false. */
  directOnly?: boolean;
}

/**
 * Options for registering a custom aggregate function.
 */
export interface AggregateOptions {
  /** Initial value of the aggregate state. */
  start?: any;
  /** Function called for each row to update the aggregate state. */
  step: (total: any, next: any) => any;
  /** Function called to remove a row's contribution from the state (for window functions). */
  inverse?: (total: any, dropped: any) => any;
  /** Function called to produce the final result from the accumulated state. */
  result?: (total: any) => any;
  /** Whether the function can accept a variable number of arguments. Defaults to false. */
  varargs?: boolean;
  /** Whether the function is deterministic. Defaults to false. */
  deterministic?: boolean;
  /** Whether to use safe 64-bit integers. Defaults to false. */
  safeIntegers?: boolean;
}

/**
 * Options for the database backup operation.
 */
export interface BackupOptions {
  /** Callback function to monitor backup progress. */
  progress?: (info: BackupMetadata) => number;
  /** The name of the attached database to backup. Defaults to 'main'. */
  attached?: string;
}

/**
 * Metadata about an ongoing or completed backup.
 */
export interface BackupMetadata {
  /** Total number of pages in the source database. */
  totalPages: number;
  /** Number of pages remaining to be copied. */
  remainingPages: number;
}

/**
 * Options for serializing a database to a buffer.
 */
export interface SerializeOptions {
  /** The name of the attached database to serialize. Defaults to 'main'. */
  attached?: string;
}

/**
 * Factory function for creating a virtual table module.
 */
export type VirtualTableFactory = (this: void, ...args: any[]) => | { rows: GeneratorFunction; columns: string[]; parameters?: string[]; } | any;

/**
 * Core interface for the Database class, used to break circular dependencies.
 */
export interface IDatabase extends EventEmitter {
  /** The absolute path to the database file. */
  readonly name: string;
  /** Whether the database connection pool is currently open. */
  readonly open: boolean;
  /** Whether the database was opened in read-only mode. */
  readonly readonly: boolean;
  /** Whether the current async execution context is inside a transaction. */
  readonly inTransaction: boolean;

  /** Creates a new prepared Statement. */
  prepare<BindParameters extends any[] | {} = any[]>(sql: string, options?: PrepareOptions): any;
  /** Wraps a function to execute within a database transaction. */
  transaction<F extends (...args: any[]) => any>(fn: F): any;
  /** Executes a simple SQL string. */
  exec(sql: string): Promise<void>;
  /** Executes a PRAGMA statement. */
  pragma(sql: string, options?: PragmaOptions): Promise<any>;
  /** Acquires an exclusive lock on the Writer worker. */
  acquire(): Promise<any>;
  /** Attaches an external database to the current connection pool. */
  attach(filename: string, alias: string, options?: AttachOptions): Promise<void>;
  /** Detaches a secondary database from the connection pool. */
  detach(alias: string): Promise<void>;
  /** Registers a User Defined Function (UDF). */
  function(name: string, cb: (...args: any[]) => any): Promise<this>;
  function(name: string, options: RegistrationOptions, cb: (...args: any[]) => any): Promise<this>;
  /** Registers a custom Aggregate Function. */
  aggregate(name: string, options: AggregateOptions): Promise<this>;
  /** Registers a Virtual Table module. */
  table(name: string, factory: VirtualTableFactory): Promise<this>;
  /** Creates a backup of the database file. */
  backup(destination: string, options?: BackupOptions): Promise<BackupMetadata>;
  /** Serializes the database to a Buffer. */
  serialize(options?: SerializeOptions): Promise<Buffer>;
  /** Sets the encryption key for the database. */
  key(key: string | Buffer): Promise<void>;
  /** Changes the encryption key for the database. */
  rekey(newKey: string | Buffer): Promise<void>;
  /** Loads a compiled SQLite extension. */
  loadExtension(path: string): Promise<void>;
  /** Enables or disables unsafe mode. */
  unsafeMode(unsafe?: boolean): Promise<this>;
  /** Dynamically resizes the reader worker pool. */
  pool(min: number, max: number): Promise<void>;
  /** Closes the database pool. */
  close(): Promise<this>;
  /** Configures default behavior for large integers. */
  defaultSafeIntegers(toggle?: boolean): Promise<void>;
}
