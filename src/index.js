/**
 * @file index.js
 * @description Main entry point for better-sqlite3-pool.
 */

const Connection = require("./lib/connection");
const Database = require("./lib/database");
const Statement = require("./lib/statement");
const { SqliteError } = require("./lib/utils");

// --- STATIC EXPORTS ---
// 1. Expose SqliteError for instanceof checks
Database.SqliteError = SqliteError;

// 2. Expose Statement class
Database.Statement = Statement;

// 3. Expose Connection class
Database.Connection = Connection;

module.exports = Database;
