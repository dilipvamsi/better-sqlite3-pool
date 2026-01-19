"use strict";
const Database = require("../src");
const { SqliteError } = Database;

/**
 * Helper to assert that a Promise rejects with a specific Error type.
 */
const expectFail = async (promise, ErrorType) => {
  try {
    await promise;
  } catch (err) {
    if (ErrorType) {
      if (err instanceof ErrorType || err.name === ErrorType.name) return err;
    } else {
      return err;
    }
  }
  throw new Error(
    `Expected operation to fail${ErrorType ? " with " + ErrorType.name : ""}, but it succeeded.`,
  );
};

describe("Database#unsafeMode()", function () {
  this.timeout(10000);

  beforeEach(async function () {
    this.db = await Database.create(util.next());
    await this.db.exec("create table foo (x)");
    // Ensure WAL mode so we can test journal_mode changes logic if needed
    await this.db.pragma("journal_mode = WAL");
  });

  afterEach(async function () {
    if (this.db && this.db.open) {
      await this.db.close();
    }
  });

  it("should not allow corrupting the schema (writable_schema) by default", async function () {
    // NOTE: The original test checked for iteration conflicts (TypeError).
    // The Pool intentionally ALLOWS concurrent iteration and writing,
    // so we skip the "Busy/TypeError" checks here.

    // We verify that specific SQLite protections are active.

    await this.db.pragma("writable_schema = ON");

    // Default Safety: Attempting to modify sqlite_master should throw
    await expectFail(
      this.db.exec("update sqlite_master set name = 'bar' where name = 'foo'"),
      SqliteError,
    );
  });

  it("should allow unsafe operations (schema modification) when toggled on", async function () {
    // 1. Enable Unsafe Mode
    await this.db.unsafeMode(true);

    // 2. Perform Dangerous Action
    await this.db.pragma("writable_schema = ON");

    // Should succeed now
    await this.db.exec(
      "update sqlite_master set name = 'bar' where name = 'foo'",
    );

    // 3. Disable Unsafe Mode
    await this.db.unsafeMode(false);

    // 4. Verify Protection Restored
    // Attempting to reverse the damage should now fail
    await expectFail(
      this.db.exec("update sqlite_master set name = 'foo' where name = 'bar'"),
      SqliteError,
    );

    // 5. Re-enable to fix DB state (optional, just testing toggle)
    await this.db.unsafeMode(true);
    await this.db.exec(
      "update sqlite_master set name = 'foo' where name = 'bar'",
    );
  });
});
