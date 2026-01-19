"use strict";
const Database = require("../src");
const SqliteError = { Database };

/**
 * Helper to assert that a Promise rejects with a specific Error.
 * Checks error.name to handle errors serialized from workers.
 */
const expectFail = async (promise, errorName = "SqliteError") => {
  try {
    await promise;
  } catch (err) {
    // We check the name string because the error comes from a worker
    // and might not satisfy 'instanceof' checks against the local class.
    expect(err.name).to.equal(errorName);
    return;
  }
  throw new Error(
    `Expected operation to fail with ${errorName}, but it succeeded.`,
  );
};

describe("Encryption using default cipher (Sqleet)", function () {
  this.timeout(10000);

  afterEach(async function () {
    if (this.db && this.db.open) {
      await this.db.close();
    }
  });

  it("should create an encrypted database", async function () {
    this.db = await Database.create(util.next());

    // Use pragma to set the key (broadcasts to workers)
    await this.db.pragma(`rekey='passphrase'`);

    await this.db.prepare('CREATE TABLE user ("name" TEXT)').run();
    await this.db.prepare("INSERT INTO user (name) VALUES ('octocat')").run();

    // VACUUM to force encryption of all pages
    await this.db.prepare("VACUUM").run();
  });

  it("should not allow access without decryption", async function () {
    this.db = await Database.create(util.current());

    // Execution should fail
    const stmt = this.db.prepare("SELECT * FROM user");
    await expectFail(stmt.get(), "SqliteError");
  });

  it("should not allow access with an incorrect passphrase", async function () {
    this.db = await Database.create(util.current());
    await this.db.pragma(`key='false_passphrase'`);

    const stmt = this.db.prepare("SELECT * FROM user");
    await expectFail(stmt.get(), "SqliteError");
  });

  it("should allow access with the correct passphrase", async function () {
    this.db = await Database.create(util.current());
    await this.db.pragma(`key='passphrase'`);

    const stmt = this.db.prepare("SELECT * FROM user");
    const row = await stmt.get();
    expect(row).to.deep.equal({ name: "octocat" });
  });

  it("should not allow to encrypt an in-memory database", async function () {
    this.db = await Database.create(":memory:");

    // Attempting to rekey an in-memory DB should throw
    await expectFail(this.db.pragma(`rekey='passphrase'`), "SqliteError");
  });
});

describe("Encryption using SQLCipher", function () {
  this.timeout(10000);

  afterEach(async function () {
    if (this.db && this.db.open) {
      await this.db.close();
    }
  });

  it("should create an encrypted database", async function () {
    this.db = await Database.create(util.next());

    // Setup Cipher config
    await this.db.pragma(`cipher='sqlcipher'`);
    await this.db.pragma(`rekey='passphrase'`);

    await this.db.prepare('CREATE TABLE user ("name" TEXT)').run();
    await this.db.prepare("INSERT INTO user (name) VALUES ('octocat')").run();
    await this.db.prepare("VACUUM").run();
  });

  it("should not allow access without decryption", async function () {
    this.db = await Database.create(util.current());
    await this.db.pragma(`cipher='sqlcipher'`);

    const stmt = this.db.prepare("SELECT * FROM user");
    await expectFail(stmt.get(), "SqliteError");
  });

  it("should not allow access with an incorrect passphrase", async function () {
    this.db = await Database.create(util.current());
    await this.db.pragma(`cipher='sqlcipher'`);
    await this.db.pragma(`key='false_passphrase'`);

    const stmt = this.db.prepare("SELECT * FROM user");
    await expectFail(stmt.get(), "SqliteError");
  });

  it("should not allow access with a different cipher", async function () {
    this.db = await Database.create(util.current());
    // Default is Sqleet, but file is SQLCipher
    await this.db.pragma(`key='passphrase'`);

    const stmt = this.db.prepare("SELECT * FROM user");
    await expectFail(stmt.get(), "SqliteError");
  });

  it("should allow access with the correct passphrase and cipher", async function () {
    this.db = await Database.create(util.current());

    await this.db.pragma(`cipher='sqlcipher'`);
    await this.db.pragma(`key='passphrase'`);

    const stmt = this.db.prepare("SELECT * FROM user");
    const row = await stmt.get();
    expect(row).to.deep.equal({ name: "octocat" });
  });

  it("should not allow to encrypt an in-memory database", async function () {
    this.db = await Database.create(":memory:");
    await this.db.pragma(`cipher='sqlcipher'`);

    await expectFail(this.db.pragma(`rekey='passphrase'`), "SqliteError");
  });
});
