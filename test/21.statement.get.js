"use strict";
const Database = require("../src"); // Assuming correct path

describe("Statement#get()", function () {
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
      await stmt.get();
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    stmt = this.db.prepare(
      "CREATE TABLE IF NOT EXISTS entries (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)",
    );
    expect(stmt.reader).to.be.false;
    try {
      await stmt.get();
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }

    stmt = this.db.prepare("BEGIN TRANSACTION");
    expect(stmt.reader).to.be.false;
    try {
      await stmt.get();
      throw new Error("Should have thrown TypeError");
    } catch (err) {
      expect(err).to.be.instanceof(TypeError);
    }
  });

  it("should return the first matching row", async function () {
    let stmt = this.db.prepare("SELECT * FROM entries ORDER BY rowid");
    expect(stmt.reader).to.be.true;
    expect(await stmt.get()).to.deep.equal({
      a: "foo",
      b: 1,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xdd),
      e: null,
    });

    stmt = this.db.prepare("SELECT * FROM entries WHERE b > 5 ORDER BY rowid");
    expect(await stmt.get()).to.deep.equal({
      a: "foo",
      b: 6,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xdd),
      e: null,
    });
  });

  it("should work with RETURNING clause", async function () {
    let stmt = this.db.prepare(
      "INSERT INTO entries (a, b) VALUES ('bar', 888), ('baz', 999) RETURNING *",
    );
    expect(stmt.reader).to.be.true;
    expect(await stmt.get()).to.deep.equal({
      a: "bar",
      b: 888,
      c: null,
      d: null,
      e: null,
    });

    stmt = this.db.prepare(
      "SELECT * FROM entries WHERE b > 900 ORDER BY rowid",
    );
    expect(await stmt.get()).to.deep.equal({
      a: "baz",
      b: 999,
      c: null,
      d: null,
      e: null,
    });
  });

  it("should obey the current pluck and expand settings", async function () {
    const stmt = this.db.prepare(
      "SELECT *, 2 + 3.5 AS c FROM entries ORDER BY rowid",
    );
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

    expect(await stmt.get()).to.deep.equal(row);
    expect(await stmt.pluck(true).get()).to.deep.equal(plucked);
    expect(await stmt.get()).to.deep.equal(plucked);
    expect(await stmt.pluck(false).get()).to.deep.equal(row);
    expect(await stmt.get()).to.deep.equal(row);
    expect(await stmt.pluck().get()).to.deep.equal(plucked);
    expect(await stmt.get()).to.deep.equal(plucked);
    expect(await stmt.expand().get()).to.deep.equal(expanded);
    expect(await stmt.get()).to.deep.equal(expanded);
    expect(await stmt.expand(false).get()).to.deep.equal(row);
    expect(await stmt.get()).to.deep.equal(row);
    expect(await stmt.expand(true).get()).to.deep.equal(expanded);
    expect(await stmt.get()).to.deep.equal(expanded);
    expect(await stmt.pluck(true).get()).to.deep.equal(plucked);
    expect(await stmt.get()).to.deep.equal(plucked);
    expect(await stmt.raw().get()).to.deep.equal(raw);
    expect(await stmt.get()).to.deep.equal(raw);
    expect(await stmt.raw(false).get()).to.deep.equal(row);
    expect(await stmt.get()).to.deep.equal(row);
    expect(await stmt.raw(true).get()).to.deep.equal(raw);
    expect(await stmt.get()).to.deep.equal(raw);
    expect(await stmt.expand(true).get()).to.deep.equal(expanded);
    expect(await stmt.get()).to.deep.equal(expanded);
  });

  it("should return undefined when no rows were found", async function () {
    const stmt = this.db.prepare("SELECT * FROM entries WHERE b == 999");
    expect(await stmt.get()).to.be.undefined;
    expect(await stmt.pluck().get()).to.be.undefined;
  });

  it("should accept bind parameters", async function () {
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

    let result = await this.db
      .prepare(SQL1)
      .get("foo", 1, 3.14, Buffer.alloc(4).fill(0xdd), null);
    expect(result).to.deep.equal(row);

    result = await this.db
      .prepare(SQL1)
      .get(["foo", 1, 3.14, Buffer.alloc(4).fill(0xdd), null]);
    expect(result).to.deep.equal(row);

    result = await this.db
      .prepare(SQL1)
      .get(["foo", 1], [3.14], Buffer.alloc(4).fill(0xdd), [,]);
    expect(result).to.deep.equal(row);

    result = await this.db.prepare(SQL2).get({
      a: "foo",
      b: 1,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xdd),
      e: undefined,
    });
    expect(result).to.deep.equal(row);

    result = await this.db.prepare(SQL2).get({
      a: "foo",
      b: 1,
      c: 3.14,
      d: Buffer.alloc(4).fill(0xaa),
      e: undefined,
    });
    expect(result).to.be.undefined;

    try {
      await this.db
        .prepare(SQL2)
        .get({ a: "foo", b: 1, c: 3.14, d: Buffer.alloc(4).fill(0xdd) });
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    try {
      await this.db.prepare(SQL1).get();
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }

    try {
      await this.db.prepare(SQL2).get({});
      throw new Error("Should have thrown RangeError");
    } catch (err) {
      expect(err).to.be.instanceof(RangeError);
    }
  });
});
