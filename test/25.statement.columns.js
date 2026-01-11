"use strict";
const Database = require("../src"); // Updated path
const SqliteError = Database.SqliteError;

describe("Statement#columns()", function () {
  beforeEach(async function () {
    this.db = await Database.create(util.next());
    await this.db
      .prepare("CREATE TABLE entries (a TEXT, b INTEGER, c WHATthe)")
      .run();
  });
  afterEach(async function () {
    await this.db.close();
  });

  it("should throw an exception if invoked before execution", async function () {
    const stmt = this.db.prepare("INSERT INTO entries VALUES (?, ?, ?)");

    // In better-sqlite3-pool, metadata is fetched asynchronously.
    // columns() throws SQLITE_MISUSE if called before the first execution.
    try {
      stmt.columns();
      throw new Error("Should have thrown SqliteError");
    } catch (err) {
      expect(err).to.be.instanceof(SqliteError);
      expect(err.code).to.equal("SQLITE_MISUSE");
    }
  });

  it("should return an array of column descriptors", async function () {
    const stmt1 = this.db.prepare("SELECT 5.0 as d, * FROM entries");

    // Must execute first to populate metadata
    await stmt1.get();

    expect(stmt1.columns()).to.deep.equal([
      { name: "d", column: null, table: null, database: null, type: null },
      {
        name: "a",
        column: "a",
        table: "entries",
        database: "main",
        type: "TEXT",
      },
      {
        name: "b",
        column: "b",
        table: "entries",
        database: "main",
        type: "INTEGER",
      },
      {
        name: "c",
        column: "c",
        table: "entries",
        database: "main",
        type: "WHATthe",
      },
    ]);

    const stmt2 = this.db.prepare("SELECT a, c as b, b FROM entries");
    await stmt2.get(); // Execute to populate

    expect(stmt2.columns()).to.deep.equal([
      {
        name: "a",
        column: "a",
        table: "entries",
        database: "main",
        type: "TEXT",
      },
      {
        name: "b",
        column: "c",
        table: "entries",
        database: "main",
        type: "WHATthe",
      },
      {
        name: "b",
        column: "b",
        table: "entries",
        database: "main",
        type: "INTEGER",
      },
    ]);
  });

  it("should not return stale column descriptors after being recompiled", async function () {
    const stmt = this.db.prepare("SELECT * FROM entries");

    // 1. Initial execution
    await stmt.get();

    expect(stmt.columns()).to.deep.equal([
      {
        name: "a",
        column: "a",
        table: "entries",
        database: "main",
        type: "TEXT",
      },
      {
        name: "b",
        column: "b",
        table: "entries",
        database: "main",
        type: "INTEGER",
      },
      {
        name: "c",
        column: "c",
        table: "entries",
        database: "main",
        type: "WHATthe",
      },
    ]);

    // 2. Modify Schema
    await this.db.prepare("ALTER TABLE entries ADD COLUMN d FOOBAR").run();

    // 3. Re-execute (Worker handles recompilation, returns new columns)
    await stmt.get();

    expect(stmt.columns()).to.deep.equal([
      {
        name: "a",
        column: "a",
        table: "entries",
        database: "main",
        type: "TEXT",
      },
      {
        name: "b",
        column: "b",
        table: "entries",
        database: "main",
        type: "INTEGER",
      },
      {
        name: "c",
        column: "c",
        table: "entries",
        database: "main",
        type: "WHATthe",
      },
      {
        name: "d",
        column: "d",
        table: "entries",
        database: "main",
        type: "FOOBAR",
      },
    ]);
  });
});
