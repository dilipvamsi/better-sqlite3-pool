"use strict";
const Database = require("../src"); // Updated path

describe("Statement#iterate()", function () {
  beforeEach(async function () {
    this.db = await Database.create(util.next());
    await this.db
      .prepare(
        "CREATE TABLE entries (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)",
      )
      .run();
    await this.db
      .prepare(
        "INSERT INTO entries WITH RECURSIVE temp(a, b, c, d, e) AS (SELECT 'foo', 1, 3.14, x'dddddddd', NULL UNION ALL SELECT a, b + 1, c, d, e FROM temp LIMIT 10) SELECT * FROM temp",
      )
      .run();
  });
  afterEach(async function () {
    await this.db.close();
  });

  it("should throw an exception when used on a statement that returns no data", async function () {
    let stmt = this.db.prepare(
      "INSERT INTO entries VALUES ('foo', 1, 3.14, x'dddddddd', NULL)",
    );
    expect(stmt.reader).to.be.false;

    // iterate() returns an AsyncGenerator immediately. It usually throws on the first .next() call.
    const iter1 = stmt.iterate();
    try {
      await iter1.next();
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    stmt = this.db.prepare(
      "CREATE TABLE IF NOT EXISTS entries (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)",
    );
    expect(stmt.reader).to.be.false;
    const iter2 = stmt.iterate();
    try {
      await iter2.next();
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    stmt = this.db.prepare("BEGIN TRANSACTION");
    expect(stmt.reader).to.be.false;
    const iter3 = stmt.iterate();
    try {
      await iter3.next();
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    await this.db
      .prepare(
        "INSERT INTO entries WITH RECURSIVE temp(a, b, c, d, e) AS (SELECT 'foo', 1, 3.14, x'dddddddd', NULL UNION ALL SELECT a, b + 1, c, d, e FROM temp LIMIT 10) SELECT * FROM temp",
      )
      .run();
  });

  it("should return an iterator over each matching row", async function () {
    const row = {
      a: "foo",
      b: 1,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xdd),
      e: null,
    };

    let count = 0;
    let stmt = this.db.prepare("SELECT * FROM entries ORDER BY rowid");
    expect(stmt.reader).to.be.true;
    expect(stmt.busy).to.be.false;

    const iterator = stmt.iterate();
    expect(iterator).to.not.be.null;
    expect(typeof iterator).to.equal("object");
    expect(iterator.next).to.be.a("function");
    expect(iterator.return).to.be.a("function");
    expect(iterator.throw).to.be.a("function"); // AsyncGenerators have throw()
    expect(iterator[Symbol.asyncIterator]).to.be.a("function");
    expect(iterator[Symbol.asyncIterator]()).to.equal(iterator);

    // Note: stmt.busy becomes true after first fetch, which happens asynchronously.
    // We can't synchronously check stmt.busy immediately after calling iterate() unless we await next().

    for await (const data of iterator) {
      row.b = ++count;
      expect(data).to.deep.equal(row);
      expect(stmt.busy).to.be.true;
    }
    expect(count).to.equal(10);
    expect(stmt.busy).to.be.false;

    count = 0;
    stmt = this.db.prepare("SELECT * FROM entries WHERE b > 5 ORDER BY rowid");
    expect(stmt.busy).to.be.false;
    const iterator2 = stmt.iterate();
    expect(iterator).to.not.equal(iterator2);

    // Wait for first item to trigger busy state check inside loop
    for await (const data of iterator2) {
      row.b = ++count + 5;
      expect(data).to.deep.equal(row);
      expect(stmt.busy).to.be.true;
    }
    expect(count).to.equal(5);
    expect(stmt.busy).to.be.false;
  });

  it("should work with RETURNING clause", async function () {
    let stmt = this.db.prepare(
      "INSERT INTO entries (a, b) VALUES ('bar', 888), ('baz', 999) RETURNING *",
    );
    expect(stmt.reader).to.be.true;

    const results1 = [];
    for await (const row of stmt.iterate()) results1.push(row);

    expect(results1).to.deep.equal([
      { a: "bar", b: 888, c: null, d: null, e: null },
      { a: "baz", b: 999, c: null, d: null, e: null },
    ]);

    stmt = this.db.prepare(
      "SELECT * FROM entries WHERE b > 800 ORDER BY rowid",
    );

    const results2 = [];
    for await (const row of stmt.iterate()) results2.push(row);

    expect(results2).to.deep.equal([
      { a: "bar", b: 888, c: null, d: null, e: null },
      { a: "baz", b: 999, c: null, d: null, e: null },
    ]);
  });

  it("should obey the current pluck and expand settings", async function () {
    const shouldHave = async (desiredData) => {
      let i = 0;
      for await (const data of stmt.iterate()) {
        i += 1;
        if (typeof desiredData === "object" && desiredData !== null) {
          if (Array.isArray(desiredData)) {
            desiredData[1] = i;
          } else if (
            typeof desiredData.entries === "object" &&
            desiredData.entries !== null
          ) {
            desiredData.entries.b = i;
          } else {
            desiredData.b = i;
          }
        }
        expect(data).to.deep.equal(desiredData);
      }
      expect(i).to.equal(10);
    };
    const expanded = {
      entries: {
        a: "foo",
        b: 1,
        c: 3.14,
        d: Buffer.alloc(4).fill(0xdd),
        e: null,
      },
      $: { c: 5.5 },
    };
    const row = Object.assign({}, expanded.entries, expanded.$);
    const plucked = expanded.entries.a;
    const raw = Object.values(expanded.entries).concat(expanded.$.c);
    const stmt = this.db.prepare(
      "SELECT *, 2 + 3.5 AS c FROM entries ORDER BY rowid",
    );

    await shouldHave(row);
    stmt.pluck(true);
    await shouldHave(plucked);
    await shouldHave(plucked);
    stmt.pluck(false);
    await shouldHave(row);
    await shouldHave(row);
    stmt.pluck();
    await shouldHave(plucked);
    await shouldHave(plucked);
    stmt.expand();
    await shouldHave(expanded);
    await shouldHave(expanded);
    stmt.expand(false);
    await shouldHave(row);
    await shouldHave(row);
    stmt.expand(true);
    await shouldHave(expanded);
    await shouldHave(expanded);
    stmt.pluck(true);
    await shouldHave(plucked);
    await shouldHave(plucked);
    stmt.raw();
    await shouldHave(raw);
    await shouldHave(raw);
    stmt.raw(false);
    await shouldHave(row);
    await shouldHave(row);
    stmt.raw(true);
    await shouldHave(raw);
    await shouldHave(raw);
    stmt.expand(true);
    await shouldHave(expanded);
    await shouldHave(expanded);
  });

  it("should close the iterator when throwing in a for-of loop", async function () {
    const err = new Error("foobar");
    const stmt = this.db.prepare("SELECT * FROM entries ORDER BY rowid");
    const iterator = stmt.iterate();
    let count = 0;
    try {
      for await (const row of iterator) {
        ++count;
        throw err;
      }
    } catch (e) {
      expect(e).to.equal(err);
    }
    expect(count).to.equal(1);

    // Check if closed
    expect(await iterator.next()).to.deep.equal({
      value: undefined,
      done: true,
    });

    // Re-iterate
    for await (const row of iterator) ++count;
    expect(count).to.equal(1);

    // New iterator
    for await (const row of stmt.iterate()) ++count;
    expect(count).to.equal(11);
  });

  it("should close the iterator when using break in a for-of loop", async function () {
    const stmt = this.db.prepare("SELECT * FROM entries ORDER BY rowid");
    const iterator = stmt.iterate();
    let count = 0;
    for await (const row of iterator) {
      ++count;
      break;
    }
    expect(count).to.equal(1);

    // Check if closed
    expect(await iterator.next()).to.deep.equal({
      value: undefined,
      done: true,
    });

    // Re-iterate (should be done)
    for await (const row of iterator) ++count;
    expect(count).to.equal(1);

    // New iterator
    for await (const row of stmt.iterate()) ++count;
    expect(count).to.equal(11);
  });

  it("should return an empty iterator when no rows were found", async function () {
    const stmt = this.db.prepare("SELECT * FROM entries WHERE b == 999");
    expect(await stmt.iterate().next()).to.deep.equal({
      value: undefined,
      done: true,
    });
    for await (const data of stmt.pluck().iterate()) {
      throw new Error("This callback should not have been invoked");
    }
  });

  it("should accept bind parameters", async function () {
    const shouldHave = async (SQL, desiredData, args) => {
      let i = 0;
      const stmt = this.db.prepare(SQL);
      for await (const data of stmt.iterate(...args)) {
        desiredData.b = ++i;
        expect(data).to.deep.equal(desiredData);
      }
      expect(i).to.equal(1);
    };

    const row = {
      a: "foo",
      b: 1,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xdd),
      e: null,
    };
    const SQL1 =
      "SELECT * FROM entries WHERE a=? AND b=? AND c=? AND d=? AND e IS ?";
    const SQL2 =
      "SELECT * FROM entries WHERE a=@a AND b=@b AND c=@c AND d=@d AND e IS @e";

    await shouldHave(SQL1, row, [
      "foo",
      1,
      3.14,
      Buffer.alloc(4).fill(0xdd),
      null,
    ]);
    await shouldHave(SQL1, row, [
      ["foo", 1, 3.14, Buffer.alloc(4).fill(0xdd), null],
    ]);
    await shouldHave(SQL1, row, [
      ["foo", 1],
      [3.14],
      Buffer.alloc(4).fill(0xdd),
      [,],
    ]);
    await shouldHave(SQL2, row, [
      { a: "foo", b: 1, c: 3.14, d: Buffer.alloc(4).fill(0xdd), e: undefined },
    ]);

    for await (const data of this.db.prepare(SQL2).iterate({
      a: "foo",
      b: 1,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xaa),
      e: undefined,
    })) {
      throw new Error("This callback should not have been invoked");
    }

    // Error cases - validation happens synchronously in .iterate() if arguments are bad
    // But for RangeError it might come from worker rejection.

    expect(() => this.db.prepare(SQL2).iterate(row, () => {})).to.throw(
      TypeError,
    ); // Sync validation for args type

    try {
      const it = this.db
        .prepare(SQL2)
        .iterate({ a: "foo", b: 1, c: 3.14, d: Buffer.alloc(4).fill(0xdd) });
      await it.next();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    try {
      const it = this.db.prepare(SQL1).iterate();
      await it.next();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    expect(
      () => this.db.prepare(SQL2).iterate(), // Sync check for missing args? Or async worker check?
      // In Statement.js, we don't sync check for missing args for named params usually.
      // But let's assume async rejection.
    ).to.not.throw(); // Changed expectation: .iterate() itself shouldn't throw unless sync validation fails.

    // Let's re-verify:
    try {
      const it = this.db.prepare(SQL2).iterate();
      await it.next();
      throw new Error("Should have thrown TypeError/RangeError");
    } catch (err) {
      // Better-sqlite3 throws RangeError for missing params
      expect(err).to.be.instanceof(TypeError);
    }

    try {
      const it = this.db.prepare(SQL2).iterate(row, {});
      await it.next();
      throw new Error("Should have thrown TypeError/RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    try {
      const it = this.db.prepare(SQL2).iterate({});
      await it.next();
      throw new Error("Should have thrown TypeError/RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    // Clean up last iterator test
    const iter = this.db
      .prepare(SQL1)
      .iterate("foo", 1, 3.14, Buffer.alloc(4).fill(0xdd), null);
    await iter.return();

    expect(() =>
      this.db
        .prepare(SQL1)
        .iterate(
          "foo",
          1,
          new (function () {})(),
          Buffer.alloc(4).fill(0xdd),
          null,
        ),
    ).to.throw(TypeError); // Custom class validation (Sync)
  });
});
