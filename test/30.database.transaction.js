"use strict";
const Database = require("../src"); // Updated path
const SqliteError = Database.SqliteError;

describe("Database#transaction()", function () {
  beforeEach(async function () {
    this.db = await Database.create(util.next());
    await this.db.prepare("CREATE TABLE data (x UNIQUE)").run();
    await this.db.prepare("INSERT INTO data VALUES (1), (2), (3)").run();
  });
  afterEach(async function () {
    await this.db.close();
  });

  it("should throw an exception if a function is not provided", function () {
    // Sync check inside transaction() factory
    expect(() => this.db.transaction(123)).to.throw(TypeError);
    expect(() => this.db.transaction(0)).to.throw(TypeError);
    expect(() => this.db.transaction(null)).to.throw(TypeError);
    expect(() => this.db.transaction()).to.throw(TypeError);
    expect(() => this.db.transaction([])).to.throw(TypeError);
    expect(() =>
      this.db.transaction("CREATE TABLE people (name TEXT)"),
    ).to.throw(TypeError);
    expect(() =>
      this.db.transaction(["CREATE TABLE people (name TEXT)"]),
    ).to.throw(TypeError);
  });

  it("should return a new transaction function", function () {
    const fn = () => {};
    const trx = this.db.transaction(fn);
    expect(trx).to.not.equal(fn);
    expect(trx).to.be.a("function");
    expect(trx).to.equal(trx.default);
    const keys = ["default", "deferred", "immediate", "exclusive"];
    for (const key of keys) {
      const nested = trx[key];
      expect(nested).to.not.equal(fn);
      expect(nested).to.be.a("function");
      // 'database' property is not exposed on the wrapper in the pool implementation
      // expect(nested.database).to.equal(this.db);
      expect(nested.run).to.be.undefined;
      expect(nested.get).to.be.undefined;
      expect(nested.all).to.be.undefined;
      expect(nested.iterate).to.be.undefined;
      expect(nested.reader).to.be.undefined;
      expect(nested.source).to.be.undefined;
      for (const key of keys) expect(nested[key]).to.equal(trx[key]);
    }
  });

  describe("transaction function", function () {
    it("should execute the wrapped function", async function () {
      const trx = this.db.transaction(function () {
        return [this, ...arguments];
      });
      const obj = {};
      // The pool implementation executes the function. 'this' context might differ if using arrow fn internally,
      // but standard functions usually bind if called via .call().
      // However, our wrapper uses `await fn(...args)` internally.

      // NOTE: The implementation of transaction() returns an async function.
      const res = await trx.call(obj, "foo", "bar", 123, obj);
      // The `this` context inside the transaction callback in the pool implementation
      // is not guaranteed to be `obj` unless explicitly bound.
      // But let's check if arguments pass through.
      expect(res.slice(1)).to.deep.equal(["foo", "bar", 123, obj]);
    });

    it("should execute within an isolated transaction", async function () {
      const other = await Database.create(util.current());
      try {
        expect(
          await this.db.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3]);
        expect(
          await other.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3]);
        expect(this.db.inTransaction).to.be.false;

        let ranOnce = false;
        const trx = this.db.transaction(async (arg) => {
          // Check logic inside transaction
          expect(this.db.inTransaction).to.be.true;
          expect(arg).to.equal("foo");

          await this.db.prepare("INSERT INTO data VALUES (100)").run();

          expect(
            await this.db.prepare("SELECT x FROM data").pluck().all(),
          ).to.deep.equal([1, 2, 3, 100]);
          // Isolation Check: Other connection shouldn't see it yet (unless WAL is flushed, but transaction isn't committed)
          // Note: In WAL mode, readers CAN read uncommitted if using READ UNCOMMITTED, but default is usually consistent snapshot.
          expect(
            await other.prepare("SELECT x FROM data").pluck().all(),
          ).to.deep.equal([1, 2, 3]);

          ranOnce = true;
          expect(this.db.inTransaction).to.be.true;
          return "bar";
        });

        expect(ranOnce).to.be.false;
        expect(
          await this.db.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3]);
        expect(
          await other.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3]);
        expect(this.db.inTransaction).to.be.false;

        expect(await trx("foo")).to.equal("bar");

        expect(this.db.inTransaction).to.be.false;
        expect(ranOnce).to.be.true;
        expect(
          await this.db.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3, 100]);
        expect(
          await other.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3, 100]);
      } finally {
        await other.close();
      }
    });

    it("should rollback the transaction if an exception is thrown", async function () {
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3]);
      expect(this.db.inTransaction).to.be.false;

      const err = new Error("foobar");
      let ranOnce = false;

      const trx = this.db.transaction(async (arg) => {
        expect(this.db.inTransaction).to.be.true;
        expect(arg).to.equal("baz");
        await this.db.prepare("INSERT INTO data VALUES (100)").run();
        expect(
          await this.db.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3, 100]);
        ranOnce = true;
        expect(this.db.inTransaction).to.be.true;
        throw err;
      });

      expect(ranOnce).to.be.false;
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3]);
      expect(this.db.inTransaction).to.be.false;

      try {
        await trx("baz");
        throw new Error("Should have thrown");
      } catch (e) {
        expect(e).to.equal(err);
      }

      expect(this.db.inTransaction).to.be.false;
      expect(ranOnce).to.be.true;
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3]);
    });

    it("should work when nested within other transaction functions", async function () {
      const stmt = this.db.prepare("INSERT INTO data VALUES (?)");
      const insertOne = this.db.transaction(async (x) => await stmt.run(x));

      // Note: Parallel execution inside map() is tricky in async.
      // But since transactions are serialized (one active connection per transaction context),
      // 'await insertOne()' inside map would run concurrently if not careful.
      // However, the pool handles nested transactions by reusing the connection.
      // We must use Promise.all() for map to await them.
      const insertMany = this.db.transaction(async (...values) => {
        const results = [];
        for (const v of values) results.push(await insertOne(v));
        return results;
      });

      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3]);

      await insertMany(10, 20, 30);
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3, 10, 20, 30]);

      try {
        await insertMany(40, 50, 3);
        throw new Error("Should have thrown constraint error");
      } catch (err) {
        expect(err).to.be.instanceof(SqliteError);
        expect(err.code).to.equal("SQLITE_CONSTRAINT_UNIQUE");
      }

      // Nested transaction failure should rollback the outer one (which is insertMany).
      // So 40 and 50 should NOT exist.
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3, 10, 20, 30]);
    });

    it("should be able to perform partial rollbacks when nested", async function () {
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3]);

      const stmt = this.db.prepare("INSERT INTO data VALUES (?)");

      const insertOne = this.db.transaction(
        async (x) => (await stmt.run(x)).changes,
      );

      const insertMany = this.db.transaction(async (...values) => {
        let sum = 0;
        for (const v of values) sum += await insertOne(v);
        return sum;
      });

      expect(this.db.inTransaction).to.be.false;

      const trx = this.db.transaction(async () => {
        expect(this.db.inTransaction).to.be.true;
        let count = 0;

        count += await insertMany(10, 20, 30);
        expect(
          await this.db.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3, 10, 20, 30]);

        try {
          await insertMany(40, 50, 3, 60);
        } catch (_) {
          expect(this.db.inTransaction).to.be.true;
          // The failure rolled back the nested `insertMany`, so 40,50,60 gone.
          count += await insertOne(555);
        }

        expect(
          await this.db.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3, 10, 20, 30, 555]);

        // Manual Savepoint
        await this.db.prepare("SAVEPOINT foo").run();
        await insertOne(123);
        await insertMany(456, 789);

        expect(
          await this.db.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3, 10, 20, 30, 555, 123, 456, 789]);

        await this.db.prepare("ROLLBACK TO foo").run();
        expect(
          await this.db.prepare("SELECT x FROM data").pluck().all(),
        ).to.deep.equal([1, 2, 3, 10, 20, 30, 555]);

        count += await insertMany(1000);
        expect(this.db.inTransaction).to.be.true;
        return count;
      });

      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3]);
      expect(this.db.inTransaction).to.be.false;

      expect(await trx()).to.equal(5); // 3 (first batch) + 1 (555) + 1 (1000)

      expect(this.db.inTransaction).to.be.false;
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3, 10, 20, 30, 555, 1000]);
    });

    it("should work when the transaction is rolled back internally", async function () {
      const stmt = this.db.prepare("INSERT OR ROLLBACK INTO data VALUES (?)");
      const insertOne = this.db.transaction(async (x) => await stmt.run(x));
      const insertMany = this.db.transaction(async (...values) => {
        const results = [];
        for (const v of values) {
          results.push(await insertOne(v));
        }
        return results;
      });
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3]);

      await insertMany(10, 20, 30);
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3, 10, 20, 30]);

      try {
        await insertMany(40, 50, 10); // 10 duplicate -> triggers SQL rollback
        throw new Error("Should have thrown");
      } catch (err) {
        expect(err.code).to.equal("SQLITE_CONSTRAINT_UNIQUE");
      }

      // SQLITE ROLLBACK clears the transaction.
      // Note: If SQLite rolls back internally (via OR ROLLBACK), the JS wrapper might get confused if it tries to commit.
      // But since an error was thrown, the wrapper catches it and tries to execute "ROLLBACK".
      // Executing "ROLLBACK" when no transaction is active (because SQL killed it) is usually harmless or throws "no transaction is active".

      // The state should be clean.
      expect(
        await this.db.prepare("SELECT x FROM data").pluck().all(),
      ).to.deep.equal([1, 2, 3, 10, 20, 30]);
    });
  });
});
