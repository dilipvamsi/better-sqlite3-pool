"use strict";
const Database = require("../src");

describe("verbose mode", function () {
  afterEach(async function () {
    if (this.db) await this.db.close();
  });

  it("should throw when not given a function or null/undefined", async function () {
    await expectAsyncError(
      () => Database.create(util.next(), { verbose: false }),
      TypeError,
    );
    await expectAsyncError(
      () => Database.create(util.next(), { verbose: true }),
      TypeError,
    );
    await expectAsyncError(
      () => Database.create(util.next(), { verbose: 123 }),
      TypeError,
    );
    await expectAsyncError(
      () => Database.create(util.next(), { verbose: "null" }),
      TypeError,
    );
    await expectAsyncError(
      () => Database.create(util.next(), { verbose: {} }),
      TypeError,
    );
    await expectAsyncError(
      () => Database.create(util.next(), { verbose: [] }),
      TypeError,
    );
  });

  it("should allow explicit null or undefined as a no-op", async function () {
    for (const verbose of [undefined, null]) {
      const db = (this.db = await Database.create(util.next(), { verbose }));
      await db.exec("select 5");
      await db.close();
    }
  });

  it("should invoke the given function with all executed SQL", async function () {
    let calls = [];
    function verbose(...args) {
      const str = String(args[0]);
      calls.push([this, ...args]);
    }

    const db = (this.db = await Database.create(util.next(), { verbose }));
    await this.db.pragma("journal_mode = WAL"); // Explicitly enable WAL

    const stmt = db.prepare("select ?");

    await db.exec("select 5");
    await db.prepare("create table data (x)").run();

    await stmt.get(BigInt(10));
    await stmt.all(BigInt(15));

    const iter1 = stmt.iterate(BigInt(20));
    // FIX: Await next() to trigger execution/logging in worker, then close.
    await iter1.next();
    await iter1.return();

    for await (const x of stmt.iterate(BigInt(25))) {
    }

    await db.pragma("cache_size");
    await db.prepare("insert into data values ('hi')").run();
    await db.prepare("insert into data values ('bye')").run();

    const rows = [];
    for await (const x of db
      .prepare("select x from data order by rowid")
      .pluck()
      .iterate()) {
      rows.push(x);
    }
    expect(rows).to.deep.equal(["hi", "bye"]);

    // console.log("calls:", calls);

    expect(calls).to.deep.equal([
      [db, "PRAGMA journal_mode = WAL"], // this called as we are running starting the writer worker with PRAGMA
      [db, "select 5"],
      [db, "create table data (x)"],
      [db, "select 10"],
      [db, "select 15"],
      [db, "select 20"],
      [db, "select 25"],
      [db, "PRAGMA cache_size"], // 1 for writer
      [db, "PRAGMA cache_size"], // 1 for reader - 1
      [db, "PRAGMA cache_size"], // 1 for reader - 2
      [db, "insert into data values ('hi')"],
      [db, "insert into data values ('bye')"],
      [db, "select x from data order by rowid"],
    ]);
  });

  it("should not fully expand very long bound parameter", async function () {
    let calls = [];
    function verbose(...args) {
      const str = String(args[0]);
      if (/PRAGMA\s+journal_mode\s*=\s*WAL/i.test(str)) return;
      calls.push([this, ...args]);
    }
    const db = (this.db = await Database.create(util.next(), { verbose }));
    const stmt = db.prepare("select ?");

    await stmt.get("this is a fairly short parameter");
    await stmt.get("this is a slightly longer parameter");
    await stmt.get(
      "this is surely a very long bound parameter value that doesnt need to be logged in its entirety",
    );

    expect(calls).to.deep.equal([
      [db, "select 'this is a fairly short parameter'"],
      [db, "select 'this is a slightly longer parame'/*+3 bytes*/"],
      [db, "select 'this is surely a very long bound'/*+62 bytes*/"],
    ]);
  });

  it("should reject the execution promise if the logger function throws", async function () {
    let fail = false;
    const errMessage = "foo";

    // Define verbose logger that conditionally throws
    const verbose = (msg) => {
      if (fail) throw new Error(errMessage);
    };

    const db = (this.db = await Database.create(util.next(), { verbose }));

    // Setup table
    await db.prepare("create table data (x)").run();

    // Create a simple UDF (Stateless, as worker cannot share state vars)
    await db.function("fn", (value) => {
      return value;
    });

    // Helper to verify rejection behavior without plugins
    const shouldThrow = async (fn) => {
      // 1. Should succeed (fail=false)
      fail = false;
      try {
        await fn();
      } catch (e) {
        throw new Error(
          "Expected function to succeed, but it failed: " + e.message,
        );
      }

      // 2. Should reject (fail=true)
      fail = true;
      let caughtError = null;
      try {
        await fn();
      } catch (e) {
        caughtError = e;
      } finally {
        fail = false;
      }

      if (!caughtError) {
        throw new Error("Expected function to throw, but it succeeded");
      }
      expect(caughtError.message).to.equal(errMessage);

      // 3. Should succeed again
      try {
        await fn();
      } catch (e) {
        throw new Error(
          "Expected function to succeed again, but it failed: " + e.message,
        );
      }
    };

    // Define usage patterns
    const runExec = () => db.exec("select fn(5)");
    const runInsert = () => db.prepare("insert into data values (fn(5))").run();
    const runBoundInsert = () =>
      db.prepare("insert into data values (fn(?))").run(5);
    const runGet = () => db.prepare("select fn(?)").get(5);
    const runAll = () => db.prepare("select fn(?)").all(5);

    // Helper for iteration, which is async iterable
    const runIterate = async () => {
      const iter = db.prepare("select fn(?)").iterate(5);
      // We must pull values to trigger execution/logging/error
      for await (const row of iter) {
        // Do nothing
      }
    };

    // Execute tests
    await shouldThrow(runExec);
    await shouldThrow(runInsert);
    await shouldThrow(runBoundInsert);
    await shouldThrow(runGet);
    await shouldThrow(runAll);
    await shouldThrow(runIterate);
  });
});

async function expectAsyncError(fn, errorType) {
  try {
    await fn();
    throw new Error(
      `Expected to throw ${errorType ? errorType.name : "Error"}`,
    );
  } catch (err) {
    if (errorType) expect(err).to.be.instanceof(errorType);
  }
}
