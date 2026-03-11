/**
 * @file connection.d.ts
 * @description Type definitions for exclusive connection sessions in better-sqlite3-pool.
 */

import { Database } from "../index";
import Statement = require("./statement");
import {
  PragmaOptions,
  AttachOptions,
  RegistrationOptions,
  AggregateOptions,
  VirtualTableFactory,
  SerializeOptions
} from "./types";

/**
 * An exclusive session on the Writer worker.
 * Use this for sequences of operations that must occur on the same database connection,
 * such as non-standard transaction patterns or using temporary tables.
 */
declare class Connection {
  /** The database manager that owns this connection. */
  readonly db: Database;

  /**
   * Releases the exclusive lock and returns the worker to the pool.
   * After calling this, the connection object should no longer be used.
   */
  release(): void;

  /**
   * Compatibility with asynchronous resource management (using statement).
   */
  [Symbol.asyncDispose](): Promise<void>;

  /**
   * Creates a new prepared Statement on this specific connection.
   */
  prepare(sql: string): Statement;

  /**
   * Executes a simple SQL string (INSERT, UPDATE, DELETE, DDL).
   */
  exec(sql: string): Promise<void>;

  /**
   * Executes a PRAGMA statement.
   */
  pragma(sql: string, options?: PragmaOptions): Promise<any>;

  /**
   * Attaches an external database to this specific connection.
   */
  attach(filename: string, alias: string, options?: AttachOptions): Promise<void>;

  /**
   * Detaches a secondary database from this connection.
   */
  detach(alias: string): Promise<void>;

  /**
   * Sets the encryption key for this connection.
   */
  key(key: string | Buffer): Promise<void>;

  /**
   * Changes the encryption key for this connection.
   */
  rekey(key: string | Buffer): Promise<void>;

  /**
   * Loads a compiled SQLite extension into this connection.
   */
  loadExtension(path: string): Promise<void>;

  /**
   * Registers a User Defined Function (UDF) on this connection.
   */
  function(name: string, cb: (...args: any[]) => any): Promise<this>;
  function(name: string, options: RegistrationOptions, cb: (...args: any[]) => any): Promise<this>;

  /**
   * Registers a custom Aggregate Function on this connection.
   */
  aggregate(name: string, options: AggregateOptions): Promise<this>;

  /**
   * Registers a Virtual Table module on this connection.
   */
  table(name: string, factory: VirtualTableFactory): Promise<this>;

  /**
   * Serializes the database to a Buffer via this connection.
   */
  serialize(options?: SerializeOptions): Promise<Buffer>;
}

export = Connection;
