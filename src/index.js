/**
 * @file index.js
 * @description Main entry point for better-sqlite3-pool.
 */

const { Database } = require("./lib/database");
const Statement = require("./lib/statement");
const { SqliteError } = require("./lib/utils");

// --- STATIC EXPORTS ---
// 1. Expose SqliteError for instanceof checks
Database.SqliteError = SqliteError;

// 2. Expose Statement class so users can use it as a type or value
Database.Statement = Statement;

module.exports = Database;
