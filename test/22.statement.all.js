"use strict";
const Database = require("../src"); // Updated path

describe("Statement#all()", function () {
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
    try {
      await stmt.all();
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    stmt = this.db.prepare(
      "CREATE TABLE IF NOT EXISTS entries (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)",
    );
    expect(stmt.reader).to.be.false;
    try {
      await stmt.all();
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    stmt = this.db.prepare("BEGIN TRANSACTION");
    expect(stmt.reader).to.be.false;
    try {
      await stmt.all();
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }
  });

  it("should return an array of every matching row", async function () {
    const row = {
      a: "foo",
      b: 1,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xdd),
      e: null,
    };

    let stmt = this.db.prepare("SELECT * FROM entries ORDER BY rowid");
    expect(stmt.reader).to.be.true;
    await matchesFrom(await stmt.all(), 1);

    stmt = this.db.prepare("SELECT * FROM entries WHERE b > 5 ORDER BY rowid");
    await matchesFrom(await stmt.all(), 6);

    async function matchesFrom(rows, i) {
      let index = 0;
      for (; i <= 10; ++i, ++index) {
        row.b = i;
        expect(rows[index]).to.deep.equal(row);
      }
      expect(index).to.equal(rows.length);
    }
  });

  it("should work with RETURNING clause", async function () {
    let stmt = this.db.prepare(
      "INSERT INTO entries (a, b) VALUES ('bar', 888), ('baz', 999) RETURNING *",
    );
    expect(stmt.reader).to.be.true;
    expect(await stmt.all()).to.deep.equal([
      { a: "bar", b: 888, c: null, d: null, e: null },
      { a: "baz", b: 999, c: null, d: null, e: null },
    ]);

    stmt = this.db.prepare(
      "SELECT * FROM entries WHERE b > 800 ORDER BY rowid",
    );
    expect(await stmt.all()).to.deep.equal([
      { a: "bar", b: 888, c: null, d: null, e: null },
      { a: "baz", b: 999, c: null, d: null, e: null },
    ]);
  });

  it("should obey the current pluck and expand settings", async function () {
    const stmt = this.db.prepare(
      "SELECT *, 2 + 3.5 AS c FROM entries ORDER BY rowid",
    );
    const expanded = new Array(10).fill().map((_, i) => ({
      entries: {
        a: "foo",
        b: i + 1,
        c: 3.14,
        d: Buffer.alloc(4).fill(0xdd),
        e: null,
      },
      $: { c: 5.5 },
    }));
    const rows = expanded.map((x) => Object.assign({}, x.entries, x.$));
    const plucked = expanded.map((x) => x.entries.a);
    const raw = expanded.map((x) => Object.values(x.entries).concat(x.$.c));

    expect(await stmt.all()).to.deep.equal(rows);
    expect(await stmt.pluck(true).all()).to.deep.equal(plucked);
    expect(await stmt.all()).to.deep.equal(plucked);
    expect(await stmt.pluck(false).all()).to.deep.equal(rows);
    expect(await stmt.all()).to.deep.equal(rows);
    expect(await stmt.pluck().all()).to.deep.equal(plucked);
    expect(await stmt.all()).to.deep.equal(plucked);
    expect(await stmt.expand().all()).to.deep.equal(expanded);
    expect(await stmt.all()).to.deep.equal(expanded);
    expect(await stmt.expand(false).all()).to.deep.equal(rows);
    expect(await stmt.all()).to.deep.equal(rows);
    expect(await stmt.expand(true).all()).to.deep.equal(expanded);
    expect(await stmt.all()).to.deep.equal(expanded);
    expect(await stmt.pluck(true).all()).to.deep.equal(plucked);
    expect(await stmt.all()).to.deep.equal(plucked);
    expect(await stmt.raw().all()).to.deep.equal(raw);
    expect(await stmt.all()).to.deep.equal(raw);
    expect(await stmt.raw(false).all()).to.deep.equal(rows);
    expect(await stmt.all()).to.deep.equal(rows);
    expect(await stmt.raw(true).all()).to.deep.equal(raw);
    expect(await stmt.all()).to.deep.equal(raw);
    expect(await stmt.expand(true).all()).to.deep.equal(expanded);
    expect(await stmt.all()).to.deep.equal(expanded);
  });

  it("should return an empty array when no rows were found", async function () {
    const stmt = this.db.prepare("SELECT * FROM entries WHERE b == 999");
    expect(await stmt.all()).to.deep.equal([]);
    expect(await stmt.pluck().all()).to.deep.equal([]);
  });

  it("should accept bind parameters", async function () {
    const rows = [
      { a: "foo", b: 1, c: 3.14, d: Buffer.alloc(4).fill(0xdd), e: null },
    ];
    const SQL1 =
      "SELECT * FROM entries WHERE a=? AND b=? AND c=? AND d=? AND e IS ?";
    const SQL2 =
      "SELECT * FROM entries WHERE a=@a AND b=@b AND c=@c AND d=@d AND e IS @e";

    let result = await this.db
      .prepare(SQL1)
      .all("foo", 1, 3.14, Buffer.alloc(4).fill(0xdd), null);
    expect(result).to.deep.equal(rows);

    result = await this.db
      .prepare(SQL1)
      .all(["foo", 1, 3.14, Buffer.alloc(4).fill(0xdd), null]);
    expect(result).to.deep.equal(rows);

    result = await this.db
      .prepare(SQL1)
      .all(["foo", 1], [3.14], Buffer.alloc(4).fill(0xdd), [,]);
    expect(result).to.deep.equal(rows);

    result = await this.db.prepare(SQL2).all({
      a: "foo",
      b: 1,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xdd),
      e: undefined,
    });
    expect(result).to.deep.equal(rows);

    result = await this.db.prepare(SQL2).all({
      a: "foo",
      b: 1,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xaa),
      e: undefined,
    });
    expect(result).to.deep.equal([]);

    try {
      await this.db
        .prepare(SQL2)
        .all({ a: "foo", b: 1, c: 3.14, d: Buffer.alloc(4).fill(0xdd) });
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    try {
      await this.db.prepare(SQL1).all();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    try {
      await this.db.prepare(SQL2).all({});
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }
  });
});
