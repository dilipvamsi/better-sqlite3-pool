"use strict";
const Database = require("../src");

describe("Database#pragma()", function () {
  beforeEach(async function () {
    this.db = await Database.create(util.next());
  });
  afterEach(async function () {
    await this.db.close();
  });
  const expectRejection = async (promise, ExpectedError) => {
    try {
      await promise;
      throw new Error("Expected promise to reject, but it resolved");
    } catch (err) {
      // console.log(err);
      expect(err).to.be.instanceof(ExpectedError);
    }
  };

  it("should throw an exception if a string is not provided", async function () {
    await expectRejection(this.db.pragma(123), TypeError);
    await expectRejection(this.db.pragma(0), TypeError);
    await expectRejection(this.db.pragma(null), TypeError);
    await expectRejection(this.db.pragma(), TypeError);
    await expectRejection(this.db.pragma(new String("cache_size")), TypeError);
  });
  it("should throw an exception if boolean options are provided as non-booleans", async function () {
    await expectRejection(
      this.db.pragma("cache_size", { simple: undefined }),
      TypeError,
    );
  });
  it("should throw an exception if invalid/redundant SQL is provided", async function () {
    await expectRejection(
      this.db.pragma("PRAGMA cache_size"),
      Database.SqliteError,
    );
    await expectRejection(
      this.db.pragma("cache_size; PRAGMA cache_size"),
      RangeError,
    );
  });
  it("should execute the pragma, returning rows of results", async function () {
    const rows = await this.db.pragma("cache_size");
    expect(rows).to.be.an("array");
    expect(rows[0]).to.be.an("object");
    expect(rows[0].cache_size).to.be.a("number");
    expect(rows[0].cache_size).to.equal(-16000);
  });
  it("should optionally return simpler results", async function () {
    expect(await this.db.pragma("cache_size", { simple: false })).to.be.an(
      "array",
    );
    const cache_size = await this.db.pragma("cache_size", { simple: true });
    expect(cache_size).to.be.a("number");
    expect(cache_size).to.equal(-16000);
    await expectRejection(this.db.pragma("cache_size", true), TypeError);
    await expectRejection(this.db.pragma("cache_size", 123), TypeError);
    await expectRejection(
      this.db.pragma("cache_size", function () {}),
      TypeError,
    );
    await expectRejection(this.db.pragma("cache_size", NaN), TypeError);
    await expectRejection(this.db.pragma("cache_size", "true"), TypeError);
  });
  it("should obey PRAGMA changes", async function () {
    expect(await this.db.pragma("cache_size", { simple: true })).to.equal(
      -16000,
    );
    await this.db.pragma("cache_size = -8000");
    expect(await this.db.pragma("cache_size", { simple: true })).to.equal(
      -8000,
    );
    // original will fail as writer is running with wal mode
    // expect(await this.db.pragma("journal_mode", { simple: true })).to.equal(
    //   "delete",
    // );
    expect(await this.db.pragma("journal_mode", { simple: true })).to.equal(
      "wal",
    );
    await this.db.pragma("journal_mode = wal");
    expect(await this.db.pragma("journal_mode", { simple: true })).to.equal(
      "wal",
    );
  });
  it("should respect readonly connections", async function () {
    // 1. Revert to DELETE mode so the file on disk matches the expectation
    // (Our library defaults to WAL, so we must manually unset it for this specific test)
    await this.db.pragma("journal_mode = delete");
    await this.db.close();
    this.db = await Database.create(util.current(), {
      readonly: true,
      fileMustExist: true,
    });
    expect(await this.db.pragma("cache_size", { simple: true })).to.equal(
      -16000,
    );
    await this.db.pragma("cache_size = -8000");
    expect(await this.db.pragma("cache_size", { simple: true })).to.equal(
      -8000,
    );
    // original will fail because when database is opened it was already open in wal mode
    expect(await this.db.pragma("journal_mode", { simple: true })).to.equal(
      "delete",
    );
    await expectRejection(
      this.db.pragma("journal_mode = wal"),
      Database.SqliteError,
    );
  });
  it("should return undefined if no rows exist and simpler results are desired", async function () {
    expect(await this.db.pragma("table_info", { simple: true })).to.be
      .undefined;
  });
});
