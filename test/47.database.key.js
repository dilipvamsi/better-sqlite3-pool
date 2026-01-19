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
      expect(err).to.be.instanceof(ErrorType);
    }
    return err; // Return error for further assertions (like property checks)
  }
  throw new Error(
    `Expected operation to fail${ErrorType ? " with " + ErrorType.name : ""}, but it succeeded.`,
  );
};

describe("Database#key()", function () {
  this.timeout(10000);

  afterEach(async function () {
    if (this.db && this.db.open) {
      await this.db.close();
    }
  });

  it("should throw error if a Buffer or String is not provided", async function () {
    this.db = await Database.create(util.next());

    // Validation happens in the main thread (usually) or worker.
    // Assuming Database#key() validates input types before sending.
    await expectFail(this.db.key(123), TypeError);
    await expectFail(this.db.key(0), TypeError);
    await expectFail(this.db.key(null), TypeError);
    await expectFail(this.db.key(), TypeError);
    // new String() is an object, not a primitive string, usually rejected
    await expectFail(this.db.key(new String("cache_size")), TypeError);
  });

  it("should execute key() without errors", async function () {
    // Create a fresh DB
    this.db = await Database.create(util.next());

    // Set cipher configuration via Pragma
    await this.db.pragma(`cipher='aes256cbc'`);

    // Set Key
    await this.db.rekey(Buffer.from("OkPassword"));
    await this.db.key(Buffer.from("OkPassword"));

    // Verify by writing to it (if key failed, this might throw or fail silently depending on implementation)
    await this.db.exec("CREATE TABLE entries (a TEXT, b INTEGER)");

    // Note: The Pool implementation of .key() usually returns void (undefined)
    // unlike the native driver which might return status.
    // We verify success by the ability to execute subsequent commands.
  });

  it("should throw error when an incorrect key is provided", async function () {
    // Re-open the existing DB
    this.db = await Database.create(util.current());

    await this.db.pragma(`cipher='aes256cbc'`);

    // Provide WRONG key
    await this.db.key(Buffer.from("WrongPassword"));

    // Attempting to access the DB should now fail
    const err = await expectFail(
      this.db.exec("select * from sqlite_schema"),
      SqliteError,
    );

    // Check for specific SQLite error code indicating not a DB / encryption failure
    // Native better-sqlite3-multiple-ciphers throws SQLITE_NOTADB for bad keys
    expect(err.code).to.equal("SQLITE_NOTADB");
  });

  it("should not throw error when the correct key is provided", async function () {
    // Re-open the existing DB
    this.db = await Database.create(util.current());

    await this.db.pragma(`cipher='aes256cbc'`);

    // Provide CORRECT key
    await this.db.key(Buffer.from("OkPassword"));

    // Should succeed
    await this.db.exec("select * from sqlite_schema");
  });
});
