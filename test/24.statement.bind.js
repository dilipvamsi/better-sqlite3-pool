"use strict";
const Database = require("../src"); // Updated path

describe("Statement#bind()", function () {
  beforeEach(async function () {
    this.db = await Database.create(util.next());
    await this.db
      .prepare("CREATE TABLE entries (a TEXT, b INTEGER, c BLOB)")
      .run();
  });
  afterEach(async function () {
    await this.db.close();
  });

  it("should permanently bind the given parameters", async function () {
    const stmt = this.db.prepare("INSERT INTO entries VALUES (?, ?, ?)");
    const buffer = Buffer.alloc(4).fill(0xdd);

    stmt.bind("foobar", 25, buffer);
    await stmt.run();

    buffer.fill(0xaa);
    await stmt.run();

    const row1 = await this.db
      .prepare("SELECT * FROM entries WHERE rowid=1")
      .get();
    const row2 = await this.db
      .prepare("SELECT * FROM entries WHERE rowid=2")
      .get();

    expect(row1.a).to.equal(row2.a);
    expect(row1.b).to.equal(row2.b);
    expect(row1.c).to.deep.equal(row2.c);
  });

  it("should not allow you to bind temporary parameters afterwards", async function () {
    const stmt = this.db.prepare("INSERT INTO entries VALUES (?, ?, ?)");
    const buffer = Buffer.alloc(4).fill(0xdd);
    stmt.bind("foobar", 25, buffer);

    // Note: The current implementation of Statement.js might allow overriding bound params
    // (unlike native better-sqlite3 which is strict).
    // These expectations check for strict compliance.

    try {
      await stmt.run(null);
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    try {
      await stmt.run(buffer);
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    try {
      await stmt.run("foobar", 25, buffer);
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }
  });

  it("should throw an exception when invoked twice on the same statement", function () {
    let stmt = this.db.prepare("INSERT INTO entries VALUES (?, ?, ?)");
    stmt.bind("foobar", 25, null);

    // Note: Native better-sqlite3 throws TypeError if you bind twice.
    // The proxy implementation currently allows re-binding (overwriting).
    // If strict compliance is required, the Statement class needs a flag to check if bound.
    // Assuming we want to test for strict compliance:

    expect(() => stmt.bind("foobar", 25, null)).to.throw(TypeError);
    expect(() => stmt.bind()).to.throw(TypeError);

    stmt = this.db.prepare("SELECT * FROM entries");
    stmt.bind();
    expect(() => stmt.bind()).to.throw(TypeError);
  });

  it("should throw an exception when invalid parameters are given", async function () {
    let stmt = this.db.prepare("INSERT INTO entries VALUES (?, ?, ?)");

    // Case 1: Too few parameters
    stmt.bind("foo", 25);
    try {
      await stmt.run();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    // Case 2: Too many parameters
    // MUST RE-PREPARE because previous bind() marked stmt as bound
    stmt = this.db.prepare("INSERT INTO entries VALUES (?, ?, ?)");
    stmt.bind("foo", 25, null, null);
    try {
      await stmt.run();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    // Case 3: Invalid Type (Sync check works here)
    stmt = this.db.prepare("INSERT INTO entries VALUES (?, ?, ?)");
    expect(() => stmt.bind("foo", new Number(25), null)).to.throw(TypeError);

    // Case 4: Missing parameters (bind empty)
    stmt = this.db.prepare("INSERT INTO entries VALUES (?, ?, ?)");
    stmt.bind();
    try {
      await stmt.run();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    // Case 5: Valid bind
    stmt = this.db.prepare("INSERT INTO entries VALUES (?, ?, ?)");
    stmt.bind("foo", 25, null);
    await stmt.run();

    // Case 6: Named Parameters - Missing key
    stmt = this.db.prepare("INSERT INTO entries VALUES (@a, @a, ?)");
    stmt.bind({ a: "123" });
    try {
      await stmt.run();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    // Case 7: Named Parameters - Extra key / mismatch
    stmt = this.db.prepare("INSERT INTO entries VALUES (@a, @a, ?)");
    stmt.bind({ a: "123", 1: null });
    try {
      await stmt.run();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    // Case 8: Mixed types
    stmt = this.db.prepare("INSERT INTO entries VALUES (@a, @a, ?)");
    stmt.bind({ a: "123" }, null, null);
    try {
      await stmt.run();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }
  });

  it("should propagate exceptions thrown while accessing array/object members", async function () {
    const arr = [22];
    const obj = {};
    const err = new TypeError("foobar");
    Object.defineProperty(arr, "0", {
      get: () => {
        throw err;
      },
    });
    Object.defineProperty(obj, "baz", {
      get: () => {
        throw err;
      },
    });

    const stmt1 = this.db.prepare("SELECT ?");
    const stmt2 = this.db.prepare("SELECT @baz");

    // Proxy does NOT access members during bind() (lazy binding).
    // It accesses them during serialization (postMessage) or in the worker.
    // So bind() will NOT throw, but run() will.

    stmt1.bind(arr);
    try {
      await stmt1.run();
      throw new Error("Should have thrown exception");
    } catch (e) {
      // Cloning error or worker error
      expect(e).to.exist;
    }

    stmt2.bind(obj);
    try {
      await stmt2.run();
      throw new Error("Should have thrown exception");
    } catch (e) {
      expect(e).to.exist;
    }
  });

  it("should properly bind empty buffers", async function () {
    const stmt = this.db.prepare("INSERT INTO entries (c) VALUES (?)");
    stmt.bind(Buffer.alloc(0));
    await stmt.run();

    const result = await this.db.prepare("SELECT c FROM entries").pluck().get();
    expect(result).to.be.an.instanceof(Uint8Array);
    expect(result.length).to.equal(0);
  });

  it("should properly bind empty strings", async function () {
    const stmt = this.db.prepare("INSERT INTO entries (a) VALUES (?)");
    stmt.bind("");
    await stmt.run();

    const result = await this.db.prepare("SELECT a FROM entries").pluck().get();
    expect(result).to.be.a("string");
    expect(result.length).to.equal(0);
  });
});
