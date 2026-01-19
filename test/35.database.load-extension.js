"use strict";
const fs = require("fs");
const path = require("path");
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
      // Check name for errors serialized from worker, or instance for local
      if (err instanceof ErrorType || err.name === ErrorType.name) return err;
    } else {
      return err;
    }
  }
  throw new Error(
    `Expected operation to fail${ErrorType ? " with " + ErrorType.name : ""}, but it succeeded.`,
  );
};

describe("Database#loadExtension()", function () {
  this.timeout(10000);
  let filepath;

  before(function () {
    // Locate the extension binary built by node-gyp
    filepath = path.join(__dirname, "test-extension", "test_extension.node");
    try {
      fs.accessSync(filepath);
    } catch (e) {
      // Skip tests if extension not built (standard behavior for this test suite)
      this.skip();
    }
  });

  beforeEach(async function () {
    this.db = await Database.create(util.next());
  });

  afterEach(async function () {
    if (this.db && this.db.open) {
      await this.db.close();
    }
  });

  it("should throw an exception if a string argument is not given", async function () {
    // These checks happen either in main thread (if validation added) or worker
    await expectFail(this.db.loadExtension(), TypeError);
    await expectFail(this.db.loadExtension(undefined), TypeError);
    await expectFail(this.db.loadExtension(null), TypeError);
    await expectFail(this.db.loadExtension(123), TypeError);
    await expectFail(this.db.loadExtension(new String(filepath)), TypeError);
    await expectFail(this.db.loadExtension([filepath]), TypeError);
  });

  it("should allow loading extension while iterating (Pool handles concurrency)", async function () {
    // Original test expected "Busy" error.
    // In Pool, iterator operates via message passing (pull-based).
    // Interleaving a loadExtension call between iterator.next() is valid and safe.

    let invoked = false;
    const iterator = this.db.prepare("select 555").pluck().iterate();

    for await (const value of iterator) {
      expect(value).to.equal(555);

      // This should SUCCEED in the pool, effectively pausing iteration,
      // loading extension on worker, then resuming.
      await this.db.loadExtension(filepath);
      invoked = true;
    }
    expect(invoked).to.be.true;

    // Verify extension is loaded
    const result = await this.db
      .prepare("SELECT testExtensionFunction(NULL, 2)")
      .pluck()
      .get();
    expect(result).to.equal(2);
  });

  it("should throw an exception if the extension is not found", async function () {
    const err = await expectFail(
      this.db.loadExtension(filepath + "x"),
      SqliteError,
    );

    expect(err.message).to.be.a("string");
    expect(err.message.length).to.be.above(0);
    expect(err.message).to.not.equal("not an error");
    expect(err.code).to.equal("SQLITE_ERROR");
  });

  it("should register the specified extension", async function () {
    // Returns promise resolving to undefined (void) in pool, or check if it returns 'this' wrapper?
    // Pool implementation usually returns void or result data. database.js returns result.
    // But let's just check functionality.

    await this.db.loadExtension(filepath);

    const res1 = await this.db
      .prepare("SELECT testExtensionFunction(NULL, 123, 99, 2)")
      .pluck()
      .get();
    expect(res1).to.equal(4);

    const res2 = await this.db
      .prepare("SELECT testExtensionFunction(NULL, 2)")
      .pluck()
      .get();
    expect(res2).to.equal(2);
  });

  it("should not allow registering extensions with SQL", async function () {
    // 1. Verify SQL load_extension is disabled by default
    await expectFail(
      this.db.prepare("SELECT load_extension(?)").get(filepath),
      SqliteError,
    );

    // 2. Load via API
    await this.db.loadExtension(filepath);

    // 3. SQL load should STILL be disabled (API loading doesn't enable SQL access)
    await expectFail(
      this.db.prepare("SELECT load_extension(?)").get(filepath),
      SqliteError,
    );

    // 4. Verify behavior on new connection (file persistence?)
    // Extension loading is transient per connection, not persistent in file.
    await this.db.close();
    this.db = await Database.create(util.next());

    // Should still fail via SQL
    await expectFail(
      this.db.prepare("SELECT load_extension(?)").get(filepath),
      SqliteError,
    );
  });
});
