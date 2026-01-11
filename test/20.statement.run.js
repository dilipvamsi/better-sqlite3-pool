"use strict";
const Database = require("../src");

describe("Statement#run()", function () {
  beforeEach(async function () {
    this.db = await Database.create(util.next());
    this.db.init = async (data = false) => {
      this.db.info = await this.db
        .prepare("CREATE TABLE entries (a TEXT, b INTEGER, c REAL, d BLOB)")
        .run();
      if (data) {
        await this.db
          .prepare("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)")
          .run();
        await this.db
          .prepare(
            "CREATE TABLE ages (age INTEGER, person INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE ON UPDATE CASCADE)",
          )
          .run();
        await this.db
          .prepare(
            "INSERT INTO entries VALUES ('foo', 25, 3.14, x'1133ddff'), ('foo', 25, 3.14, x'1133ddff'), ('foo', 25, 3.14, x'1133ddff')",
          )
          .run();
        await this.db
          .prepare("INSERT INTO people VALUES (1, 'bob'), (2, 'sarah')")
          .run();
      }
      return this.db;
    };
  });
  afterEach(async function () {
    await this.db.close();
  });

  it("should work with CREATE TABLE", async function () {
    const { info } = await this.db.init();
    expect(info.changes).to.equal(0);
    expect(info.lastInsertRowid).to.equal(0);
  });
  it("should work with CREATE TABLE IF NOT EXISTS", async function () {
    const stmt = (await this.db.init()).prepare(
      "CREATE TABLE IF NOT EXISTS entries (a TEXT, b INTEGER, c REAL, d BLOB)",
    );
    const info = await stmt.run();
    expect(info.changes).to.equal(0);
    expect(info.lastInsertRowid).to.equal(0);
  });
  it("should work with SELECT", async function () {
    const stmt = this.db.prepare("SELECT 555");
    const info = await stmt.run();
    expect(info.changes).to.equal(0);
    expect(info.lastInsertRowid).to.equal(0);
  });
  it("should work with INSERT INTO", async function () {
    let stmt = (await this.db.init()).prepare(
      "INSERT INTO entries VALUES ('foo', 25, 3.14, x'1133ddff')",
    );
    let info = await stmt.run();
    expect(info.changes).to.equal(1);
    expect(info.lastInsertRowid).to.equal(1);

    info = await stmt.run();
    expect(info.changes).to.equal(1);
    expect(info.lastInsertRowid).to.equal(2);

    stmt = this.db.prepare(
      "INSERT INTO entries VALUES ('foo', 25, 3.14, x'1133ddff'), ('foo', 25, 3.14, x'1133ddff')",
    );
    info = await stmt.run();
    expect(info.changes).to.equal(2);
    expect(info.lastInsertRowid).to.equal(4);
  });
  it("should work with UPDATE", async function () {
    const stmt = (await this.db.init(true)).prepare(
      "UPDATE entries SET a='bar' WHERE rowid=1",
    );
    expect((await stmt.run()).changes).to.equal(1);
  });
  it("should work with DELETE FROM", async function () {
    let stmt = (await this.db.init(true)).prepare(
      "DELETE FROM entries WHERE a='foo'",
    );
    expect((await stmt.run()).changes).to.equal(3);

    stmt = this.db.prepare(
      "INSERT INTO entries VALUES ('foo', 25, 3.14, x'1133ddff')",
    );
    await stmt.run();
    const info = await stmt.run();
    expect(info.changes).to.equal(1);
    expect(info.lastInsertRowid).to.equal(2);
  });
  it("should work with BEGIN and COMMIT", async function () {
    expect((await this.db.init(true)).inTransaction).to.equal(false);
    const conn = await this.db.acquire();
    const txStratStmt = conn.prepare("BEGIN TRANSACTION");
    expect((await txStratStmt.run()).changes).to.equal(0);
    expect(this.db.inTransaction).to.equal(true);
    const info = await conn
      .prepare("INSERT INTO entries VALUES ('foo', 25, 3.14, x'1133ddff')")
      .run();
    expect(info.changes).to.equal(1);
    expect(info.lastInsertRowid).to.equal(4);
    expect(this.db.inTransaction).to.equal(true);
    expect(
      await (
        await conn.prepare("COMMIT TRANSACTION").run()
      ).changes,
    ).to.equal(0);
    expect(this.db.inTransaction).to.equal(false);
    await conn.release();
  });
  it("should work with DROP TABLE", async function () {
    const stmt = (await this.db.init(true)).prepare("DROP TABLE entries");
    expect((await stmt.run()).changes).to.equal(0);
  });
  it("should throw an exception for failed constraints", async function () {
    // 1. Initialize (await the async init function)
    await this.db.init(true);

    // 2. Run valid insertions (await them)
    await this.db.prepare("INSERT INTO ages VALUES (25, 1)").run();
    await this.db.prepare("INSERT INTO ages VALUES (30, 2)").run();
    await this.db.prepare("INSERT INTO ages VALUES (35, 2)").run();

    // 3. Test Foreign Key Constraint failure
    let stmt = this.db.prepare("INSERT INTO ages VALUES (30, 3)");
    try {
      await stmt.run();
      throw new Error("Should have thrown exception for FOREIGNKEY constraint");
    } catch (err) {
      // Check properties on the caught error
      expect(err).to.have.property("code", "SQLITE_CONSTRAINT_FOREIGNKEY");
      // Optional: Check instance type if Database.SqliteError is exposed
      // expect(err).to.be.instanceof(Database.SqliteError);
    }

    // 4. Test Not Null Constraint failure
    stmt = this.db.prepare("INSERT INTO ages VALUES (30, NULL)");
    try {
      await stmt.run();
      throw new Error("Should have thrown exception for NOTNULL constraint");
    } catch (err) {
      expect(err).to.have.property("code", "SQLITE_CONSTRAINT_NOTNULL");
    }
  });
  it("should allow ad-hoc transactions", async function () {
    // 1. Initialize data
    await this.db.init(true);

    // 2. Acquire a dedicated connection (required for manual transactions)
    // The library forbids 'BEGIN' on the global 'db' object to prevent deadlocks.
    const conn = await this.db.acquire();

    try {
      // 3. Start Transaction
      expect((await conn.prepare("BEGIN TRANSACTION").run()).changes).to.equal(
        0,
      );

      // 4. Valid Insert
      expect(
        (await conn.prepare("INSERT INTO ages VALUES (45, 2)").run()).changes,
      ).to.equal(1);

      // 5. Invalid Insert (Foreign Key Error)
      const stmt = conn.prepare("INSERT INTO ages VALUES (30, 3)");
      try {
        await stmt.run();
        throw new Error(
          "Should have thrown exception for FOREIGNKEY constraint",
        );
      } catch (err) {
        expect(err).to.have.property("code", "SQLITE_CONSTRAINT_FOREIGNKEY");
      }

      // 6. Rollback
      expect(
        (await conn.prepare("ROLLBACK TRANSACTION").run()).changes,
      ).to.equal(0);
    } finally {
      // 7. Always release the connection
      conn.release();
    }
  });
  it("should not count changes from indirect mechanisms", async function () {
    await this.db.init(true);
    const stmt = this.db.prepare("UPDATE people SET id=55 WHERE id=2");
    expect((await stmt.run()).changes).to.equal(1);
  });

  it("should count accurate DELETE changes when a dropped table has side effects", async function () {
    await this.db.init(true);
    const stmt = this.db.prepare("DROP TABLE people");
    expect((await stmt.run()).changes).to.equal(2);
  });

  it("should obey the restrictions of readonly mode", async function () {
    await this.db.close();
    // Use factory method for async creation
    this.db = await Database.create(util.current(), { readonly: true });

    const stmt = this.db.prepare("CREATE TABLE people (name TEXT)");

    try {
      await stmt.run();
      throw new Error("Should have thrown SQLITE_READONLY");
    } catch (err) {
      expect(err).to.have.property("code", "SQLITE_READONLY");
    }
  });

  it("should accept bind parameters", async function () {
    await this.db
      .prepare(
        "CREATE TABLE entries (a TEXT CHECK(typeof(a)=='text'), b INTEGER CHECK(typeof(b)=='integer' OR typeof(b)=='real'), c REAL CHECK(typeof(c)=='real' OR typeof(c)=='integer'), d BLOB CHECK(typeof(d)=='blob'))",
      )
      .run();

    // 1. Valid executions
    await this.db
      .prepare("INSERT INTO entries VALUES (?, ?, ?, ?)")
      .run("foo", 25, 25, Buffer.alloc(8).fill(0xdd));

    await this.db
      .prepare("INSERT INTO entries VALUES (?, ?, ?, ?)")
      .run(["foo", 25, 25, Buffer.alloc(8).fill(0xdd)]);

    await this.db
      .prepare("INSERT INTO entries VALUES (?, ?, ?, ?)")
      .run(["foo", 25], [25], Buffer.alloc(8).fill(0xdd));

    await this.db
      .prepare("INSERT INTO entries VALUES (@a, @b, @c, @d)")
      .run({ a: "foo", b: 25, c: 25, d: Buffer.alloc(8).fill(0xdd) });

    await this.db
      .prepare("INSERT INTO entries VALUES ($a, $b, $c, $d)")
      .run({ a: "foo", b: 25, c: 25, d: Buffer.alloc(8).fill(0xdd) });

    await this.db
      .prepare("INSERT INTO entries VALUES (:a, :b, :c, :d)")
      .run({ a: "foo", b: 25, c: 25, d: Buffer.alloc(8).fill(0xdd) });

    await this.db
      .prepare("INSERT INTO entries VALUES (?, @a, @a, ?)")
      .run({ a: 25 }, ["foo"], Buffer.alloc(8).fill(0xdd));

    // 2. Invalid parameter counts (Async Rejections)
    try {
      await this.db
        .prepare("INSERT INTO entries VALUES (?, @a, @a, ?)")
        .run(
          { a: 25 },
          ["foo"],
          Buffer.alloc(8).fill(0xdd),
          Buffer.alloc(8).fill(0xdd),
        );
      throw new Error("Should have thrown RangeError (too many args)");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    try {
      await this.db
        .prepare("INSERT INTO entries VALUES (?, @a, @a, ?)")
        .run({ a: 25 }, ["foo"]);
      throw new Error("Should have thrown RangeError (too few args)");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    // 3. Mixed valid execution
    await this.db
      .prepare("INSERT INTO entries VALUES (?, @a, @a, ?)")
      .run({ a: 25, c: 25 }, ["foo"], Buffer.alloc(8).fill(0xdd));

    // 4. More Invalid cases
    try {
      await this.db
        .prepare("INSERT INTO entries VALUES (?, @a, @a, ?)")
        .run({}, ["foo"], Buffer.alloc(8).fill(0xdd));
      throw new Error("Should have thrown RangeError (missing named param)");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    // 5. Constraint Check Failure
    try {
      await this.db
        .prepare("INSERT INTO entries VALUES (?, ?, ?, ?)")
        .run(25, "foo", 25, Buffer.alloc(8).fill(0xdd));
      throw new Error("Should have thrown SQLITE_CONSTRAINT_CHECK");
    } catch (err) {
      expect(err).to.have.property("code", "SQLITE_CONSTRAINT_CHECK");
    }

    // 6. Valid Extras
    await this.db
      .prepare("INSERT INTO entries VALUES (?, ?, ?, ?)")
      .run("foo", 25, 25, Buffer.alloc(8).fill(0xdd), {});

    await this.db
      .prepare("INSERT INTO entries VALUES (?, ?, ?, ?)")
      .run("foo", 25, 25, Buffer.alloc(8).fill(0xdd), { foo: "foo" });

    // 7. Invalid object keys as params
    try {
      await this.db
        .prepare("INSERT INTO entries VALUES (?, ?, ?, ?)")
        .run("foo", 25, 25, { 4: Buffer.alloc(8).fill(0xdd) });
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    try {
      await this.db.prepare("INSERT INTO entries VALUES (?, ?, ?, ?)").run();
      throw new Error("Should have thrown RangeError (no args)");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    try {
      await this.db.prepare("INSERT INTO entries VALUES (?, ?, ?, ?)").run({
        length: 4,
        0: "foo",
        1: 25,
        2: 25,
        3: Buffer.alloc(8).fill(0xdd),
      });
      throw new Error("Should have thrown RangeError (array-like obj)");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    // 8. Type Errors
    try {
      await this.db
        .prepare("INSERT INTO entries VALUES (?, ?, ?, ?)")
        .run("foo", 25, new Number(25), Buffer.alloc(8).fill(0xdd));
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    try {
      await this.db
        .prepare("INSERT INTO entries VALUES (?, ?, ?, ?)")
        .run("foo", { low: 25, high: 25 }, 25, Buffer.alloc(8).fill(0xdd));
      throw new Error("Should have thrown RangeError (int64 object)");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    function Foo() {
      this.a = "foo";
      this.b = 25;
      this.c = 25;
      this.d = Buffer.alloc(8).fill(0xdd);
    }

    try {
      await this.db
        .prepare("INSERT INTO entries VALUES (@a, @b, @c, @d)")
        .run(new Foo());
      throw new Error("Should have thrown TypeError (custom class)");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    // 9. Validation via Select
    let i = 0;
    let row;
    // Note: get() is async now
    while (
      (row = await this.db
        .prepare(`SELECT * FROM entries WHERE rowid=${++i}`)
        .get())
    ) {
      expect(row).to.deep.equal({
        a: "foo",
        b: 25,
        c: 25,
        d: Buffer.alloc(8).fill(0xdd),
      });
    }
    expect(i).to.equal(11);
  });
});
