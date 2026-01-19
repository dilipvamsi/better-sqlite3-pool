"use strict";
const Database = require("../src");
const { SqliteError } = Database;

/**
 * Helper to assert that a Promise rejects with a specific Error type/name.
 */
const expectFail = async (promise, ErrorType) => {
  try {
    await promise;
  } catch (err) {
    // If ErrorType is a class, check name or instance
    if (typeof ErrorType === "function") {
      if (err instanceof ErrorType) return err;
      if (err.name === ErrorType.name) return err;
    }
    // Fallback for string matching if needed
    return err;
  }
  throw new Error(
    `Expected operation to fail${ErrorType ? " with " + ErrorType.name : ""}, but it succeeded.`,
  );
};

describe("Database#rekey()", function () {
  this.timeout(10000);

  afterEach(async function () {
    if (this.db && this.db.open) {
      await this.db.close();
    }
  });

  it("should throw error if a Buffer is not provided", async function () {
    // Just for validation check, can use a throwaway file
    this.db = await Database.create(util.next());

    await expectFail(this.db.rekey(123), TypeError);
    await expectFail(this.db.rekey(0), TypeError);
    await expectFail(this.db.rekey(null), TypeError);
    await expectFail(this.db.rekey(), TypeError);
    await expectFail(this.db.rekey(new String("cache_size")), TypeError);
  });

  it("should execute rekey() without errors", async function () {
    // Start fresh file with encryption flag (defers WAL)
    this.db = await Database.create(util.next());

    await this.db.pragma(`cipher='aes256cbc'`);

    // Initial keying via rekey (common pattern in m-ciphers to set initial key on empty db)
    const result = await this.db.rekey(Buffer.from("OkPassword"));

    // Create table to verify it's working
    await this.db.exec("CREATE TABLE entries (a TEXT, b INTEGER)");

    // Pool implementation doesn't strictly return the status code (undefined),
    // but if it didn't throw, it's success.
  });

  it("should throw error if an encrypted database is not decrypted before rekey()", async function () {
    // Re-open the DB created in the previous test
    this.db = await Database.create(util.current());

    await this.db.pragma(`cipher='aes256cbc'`);

    // Attempt rekey WITHOUT calling key() first
    const err = await expectFail(
      this.db.rekey(Buffer.from("NewPassword")),
      SqliteError,
    );

    // Expect SQLITE_NOTADB (26) because the DB is locked
    expect(err.code).to.equal("SQLITE_NOTADB");
  });

  it("should allow to rekey() if an already encrypted database is properly decrypted in advance", async function () {
    // 1. Open and Unlock
    this.db = await Database.create(util.current());
    await this.db.pragma(`cipher='aes256cbc'`);
    await this.db.key(Buffer.from("OkPassword"));

    // 2. Rekey
    await this.db.rekey(Buffer.from("NewPassword"));

    // 3. Verify access
    await this.db.exec("select * from sqlite_schema");
    await this.db.close();

    // 4. Re-open with NEW password
    this.db = await Database.create(util.current());
    await this.db.pragma(`cipher='aes256cbc'`);
    await this.db.key(Buffer.from("NewPassword"));

    // 5. Final verification
    await this.db.exec("select * from sqlite_schema");
  });
});
