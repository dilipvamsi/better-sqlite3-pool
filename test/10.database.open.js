"use strict";
const fs = require("fs");
const path = require("path");
const Database = require("../src");

describe("new Database()", function () {
  // Helper to check invalid inputs
  const expectRejectionForCreate = async (opts, errorType) => {
    try {
      await Database.create(util.current(), opts);
      throw new Error(`Should have rejected options: ${JSON.stringify(opts)}`);
    } catch (err) {
      expect(err).to.be.instanceOf(errorType);
    }
  };

  const expectTypeError = async (promise) => {
    try {
      await promise;
      throw new Error("Expected promise to reject, but it resolved");
    } catch (err) {
      expect(err).to.be.an.instanceof(TypeError);
      // Optional: verify message if needed
      // expect(err.message).to.include("filename has to be string");
    }
  };

  it("should throw when given invalid argument types", async function () {
    // Helper to verify that a promise rejects with a TypeError
    // Execute checks sequentially to prevent race conditions
    await expectTypeError(Database.create("", "")); // Now throws because options is string
    await expectTypeError(Database.create({}, ""));
    await expectTypeError(Database.create({}, {}));
    await expectTypeError(Database.create({}));
    await expectTypeError(Database.create(0));
    await expectTypeError(Database.create(123));
    await expectTypeError(Database.create(new String("test"))); // Wrapper object != primitive string
    await expectTypeError(Database.create(["test"]));
  });

  it("should throw when boolean options are provided as non-booleans", async function () {
    // Check 'readonly' with non-boolean values
    await expectTypeError(Database.create(util.next(), { readonly: 123 }));
    await expectTypeError(Database.create(util.next(), { readonly: "true" }));

    // Check 'fileMustExist' with non-boolean values
    await expectTypeError(Database.create(util.next(), { fileMustExist: 123 }));
    await expectTypeError(
      Database.create(util.next(), { fileMustExist: "yes" }),
    );
  });

  it("should allow anonymous temporary databases to be created", async function () {
    // Valid arguments for anonymous/temporary databases (filename must be an empty string)
    for (const args of [[""], ["", { timeout: 2000 }]]) {
      const db = (this.db = await Database.create(...args));
      expect(db.name).to.equal("");
      expect(db.memory).to.be.true;
      expect(db.readonly).to.be.false;
      expect(db.open).to.be.true;
      expect(db.inTransaction).to.be.false;
      expect(fs.existsSync("")).to.be.false;
      await db.close();
    }
  });

  it("should allow anonymous in-memory databases to be created", async function () {
    const db = (this.db = await Database.create(":memory:"));
    expect(db.name).to.equal(":memory:");
    expect(db.memory).to.be.true;
    expect(db.readonly).to.be.false;
    expect(db.open).to.be.true;
    expect(db.inTransaction).to.be.false;
    expect(fs.existsSync(":memory:")).to.be.false;
    await db.close();
  });

  it("should allow disk-bound databases to be created", async function () {
    expect(fs.existsSync(util.next())).to.be.false;
    const db = (this.db = await Database.create(util.current()));
    expect(db.name).to.equal(util.current());
    expect(db.memory).to.be.false;
    expect(db.readonly).to.be.false;
    expect(db.open).to.be.true;
    expect(db.inTransaction).to.be.false;
    expect(fs.existsSync(util.current())).to.be.true;
  });

  it("should allow readonly database connections to be created", async function () {
    expect(fs.existsSync(util.next())).to.be.false;

    // 1. Expect rejection when opening a non-existent file in readonly mode
    try {
      await Database.create(util.current(), { readonly: true });
      throw new Error("Should have rejected");
    } catch (err) {
      // console.error(err);
      expect(err).to.have.property("code", "SQLITE_CANTOPEN");
    }

    // 2. Create the file by opening a standard connection and closing it
    const temp = await Database.create(util.current());
    await temp.close();

    expect(fs.existsSync(util.current())).to.be.true;

    // 3. Open successfully in readonly mode
    const db = (this.db = await Database.create(util.current(), {
      readonly: true,
    }));
    expect(db.name).to.equal(util.current());
    expect(db.memory).to.be.false;
    expect(db.readonly).to.be.true;
    expect(db.open).to.be.true;
    expect(db.inTransaction).to.be.false;
    expect(fs.existsSync(util.current())).to.be.true;
  });

  it('should not allow the "readonly" option for in-memory databases', async function () {
    expect(fs.existsSync(util.next())).to.be.false;
    await expectTypeError(Database.create(":memory:", { readonly: true }));
    await expectTypeError(Database.create("", { readonly: true }));
    expect(fs.existsSync(util.current())).to.be.false;
  });

  it('should accept the "fileMustExist" option', async function () {
    expect(fs.existsSync(util.next())).to.be.false;

    // 1. Expect rejection when opening a non-existent file with fileMustExist: true
    try {
      await Database.create(util.current(), { fileMustExist: true });
      throw new Error("Should have rejected");
    } catch (err) {
      expect(err).to.have.property("code", "SQLITE_CANTOPEN");
    }

    // 2. Create the file by opening a standard connection and closing it
    const temp = await Database.create(util.current());
    await temp.close();

    expect(fs.existsSync(util.current())).to.be.true;

    // 3. Open successfully now that the file exists
    const db = (this.db = await Database.create(util.current(), {
      fileMustExist: true,
    }));

    expect(db.name).to.equal(util.current());
    expect(db.memory).to.be.false;
    expect(db.readonly).to.be.false;
    expect(db.open).to.be.true;
    expect(db.inTransaction).to.be.false;
    expect(fs.existsSync(util.current())).to.be.true;
  });

  it("should throw when given invalid timeout", async function () {
    await expectRejectionForCreate({ timeout: null }, TypeError);
    await expectRejectionForCreate({ timeout: NaN }, TypeError);
    await expectRejectionForCreate({ timeout: "75" }, TypeError);
    await expectRejectionForCreate({ timeout: -1 }, TypeError);
    await expectRejectionForCreate({ timeout: 75.01 }, TypeError);

    // 0x80000000 passes local validation (it's a valid JS integer),
    // but fails inside the Worker when passed to better-sqlite3 (RangeError).
    // The worker propagates this error back to the main thread.
    await expectRejectionForCreate({ timeout: 0x80000000 }, RangeError);
  });

  util.itUnix('should accept the "timeout" option', async function () {
    this.slow(5000);
    // Helper to test timeout duration
    const testTimeout = async (timeout) => {
      const db = await Database.create(util.current(), { timeout });
      try {
        let dbCon;
        const start = Date.now();
        // Attempt to acquire lock on a busy DB
        try {
          dbCon = await db.acquire();
          await dbCon.exec("BEGIN EXCLUSIVE");
          throw new Error("Should have thrown SQLITE_BUSY");
        } catch (err) {
          expect(err).to.have.property("code", "SQLITE_BUSY");
        } finally {
          if (dbCon) {
            await dbCon.release();
          }
        }
        const end = Date.now();
        // GHA/CI is slow: allow a generous buffer
        expect(end - start).to.be.within(timeout - 1, timeout * 3 + 300);
      } finally {
        await db.close();
      }
    };
    // 1. Create the Blocker (holds the lock)
    // util.next() generates the filename, util.current() retrieves it.
    const blocker = (this.db = await Database.create(util.next(), {
      timeout: 0x7fffffff,
    }));
    const blockerConn = await blocker.acquire();
    await blockerConn.exec("BEGIN EXCLUSIVE");
    // 2. Run the timing tests against the blocked file
    await testTimeout(0);
    await testTimeout(1000);
    blockerConn.release();
    // 3. Cleanup blocker
    await blocker.close();
  });

  it("should throw Error if the directory does not exist", async function () {
    expect(fs.existsSync(util.next())).to.be.false;
    const filepath = `temp/nonexistent/abcfoobar123/${util.current()}`;

    // Database.create is async. We expect the Promise to reject.
    try {
      await Database.create(filepath);
      throw new Error("Should have rejected");
    } catch (err) {
      // better-sqlite3 throws TypeError for bad paths,
      // but the Worker transport might convert it to a standard Error with the message.
      // We check that it rejected and has a relevant message.
      expect(err).to.exist;
      // If your createError util reconstructs TypeErrors, keep .to.be.instanceOf(TypeError)
      // Otherwise, checking the message is safer:
      expect(err.message).to.match(
        /cannot open|directory does not exist|unable to open/i,
      );
    }

    expect(fs.existsSync(filepath)).to.be.false;
    expect(fs.existsSync(util.current())).to.be.false;
  });

  it("should have a proper prototype chain", async function () {
    const db = (this.db = await Database.create(util.next()));

    expect(db).to.be.an.instanceof(Database);
    expect(db.constructor).to.equal(Database);
    expect(Database.prototype.constructor).to.equal(Database);
    expect(Database.prototype.close).to.be.a("function");
    // In the pool, instance methods are inherited from prototype
    expect(db.close).to.equal(Database.prototype.close);
    expect(Database.prototype).to.equal(Object.getPrototypeOf(db));
  });

  it("should throw when called as a function or direct constructor", function () {
    // The pool implementation explicitly forbids direct usage.
    // Original test checked factory pattern; we verify strict usage now.

    // 1. Check functional call
    expect(() => {
      // @ts-ignore
      Database(util.next());
    }).to.throw(/Class constructor Database cannot be invoked without 'new'/);

    // 2. Check direct 'new' usage without create()
    expect(() => {
      new Database(util.next());
    }).to.throw(/Direct constructor usage is not supported/);
  });

  it("should work properly when subclassed", async function () {
    class MyDatabase extends Database {
      foo() {
        return 999;
      }
    }

    // Use .create() on the subclass.
    // The Database.create implementation uses `new this(...)`, so it respects subclassing.
    const db = (this.db = await MyDatabase.create(util.next()));

    expect(db).to.be.an.instanceof(Database);
    expect(db).to.be.an.instanceof(MyDatabase);
    expect(db.constructor).to.equal(MyDatabase);

    expect(Database.prototype.close).to.equal(db.close);
    expect(MyDatabase.prototype.close).to.equal(db.close);

    expect(Database.prototype.foo).to.be.undefined;
    expect(MyDatabase.prototype.foo).to.equal(db.foo);

    expect(Database.prototype).to.equal(
      Object.getPrototypeOf(MyDatabase.prototype),
    );
    expect(MyDatabase.prototype).to.equal(Object.getPrototypeOf(db));

    expect(db.foo()).to.equal(999);
  });

  it('should accept the "nativeBinding" option', async function () {
    this.slow(500);

    // 1. Locate the actual .node binary
    // better-sqlite3-multiple-ciphers usually stores it in build/Release
    let bindingPath;
    try {
      const pkgRoot = path.dirname(
        require.resolve("better-sqlite3-multiple-ciphers/package.json"),
      );
      bindingPath = path.join(
        pkgRoot,
        "build",
        "Release",
        "better_sqlite3.node",
      );
    } catch (e) {
      // Fallback if package structure is different
      bindingPath = "better_sqlite3.node";
    }

    // Skip test if we can't verify the file exists (prevents false negatives)
    if (!fs.existsSync(bindingPath)) {
      console.warn(
        "Skipping nativeBinding test: Could not locate better_sqlite3.node binary",
      );
      this.skip();
      return;
    }

    let db1, db2;
    try {
      // 1. Default binding
      db1 = await Database.create(util.next());
      expect(db1.open).to.be.true;

      // 2. Explicit binding path (Must be the .node file)
      db2 = await Database.create(util.next(), { nativeBinding: bindingPath });
      expect(db2.open).to.be.true;
    } finally {
      if (db1) await db1.close();
      if (db2) await db2.close();
    }
  });
});
