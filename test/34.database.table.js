"use strict";
const Database = require("../src");
const { SqliteError } = Database;

describe("Database#table()", function () {
  beforeEach(async function () {
    this.db = await Database.create(util.next());
  });
  afterEach(async function () {
    await this.db.close();
  });

  it("should throw an exception if the correct arguments are not provided", async function () {
    await expectAsyncError(() => this.db.table(), TypeError);
    await expectAsyncError(() => this.db.table(null), TypeError);
    await expectAsyncError(() => this.db.table("a"), TypeError);
    await expectAsyncError(() => this.db.table({}), TypeError);
    // These fail because arg1 must be string
    await expectAsyncError(
      () => this.db.table({ rows: function* () {}, columns: ["x"] }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table({ name: "b", rows: function* () {}, columns: ["x"] }),
      TypeError,
    );
    await expectAsyncError(() => this.db.table(() => {}), TypeError);
    await expectAsyncError(() => this.db.table(function* c() {}), TypeError);
    await expectAsyncError(() => this.db.table({}, function d() {}), TypeError);
    await expectAsyncError(
      () =>
        this.db.table(
          { name: "e", rows: function* e() {}, columns: ["x"] },
          function e() {},
        ),
      TypeError,
    );
    await expectAsyncError(() => this.db.table("f"), TypeError);
    await expectAsyncError(() => this.db.table("g", null), TypeError);
    await expectAsyncError(() => this.db.table("h", {}), TypeError);
    await expectAsyncError(
      () => this.db.table("i", Object.create(Function.prototype)),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("j", { columns: ["x"] }, function j() {}),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("k", { name: "k", columns: ["x"] }, function* k() {}),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("l", { name: "l", rows: function* l() {} }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table(new String("m"), {
          columns: ["x"],
          rows: function* m() {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table(new String("n"), () => {}),
      TypeError,
    );
  });

  it('should throw an exception if the "columns" option is invalid', async function () {
    await expectAsyncError(
      () => this.db.table("a", { rows: function* () {} }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("b", { columns: undefined, rows: function* () {} }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("c", { columns: "x", rows: function* () {} }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("d", {
          columns: {
            length: 1,
            0: "x",
            [Symbol.iterator]: () => ["x"].values(),
          },
          rows: function* () {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("e", { columns: ["x", , "y"], rows: function* () {} }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("f", {
          columns: ["x", new String("y")],
          rows: function* () {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("g", { columns: ["x", "x"], rows: function* () {} }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("h", { columns: [], rows: function* () {} }),
      RangeError,
    );
  });

  it('should throw an exception if the "parameters" option is invalid', async function () {
    await expectAsyncError(
      () =>
        this.db.table("a", {
          parameters: undefined,
          columns: ["foo"],
          rows: function* () {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("b", {
          parameters: "x",
          columns: ["foo"],
          rows: function* () {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("c", {
          parameters: {
            length: 1,
            0: "x",
            [Symbol.iterator]: () => ["x"].values(),
          },
          columns: ["foo"],
          rows: function* () {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("d", {
          parameters: ["x", , "y"],
          columns: ["foo"],
          rows: function* () {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("e", {
          parameters: ["x", new String("y")],
          columns: ["foo"],
          rows: function* () {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("f", {
          parameters: ["x", "x"],
          columns: ["foo"],
          rows: function* () {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("g", {
          parameters: ["x"],
          columns: ["x"],
          rows: function* () {},
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("h", {
          parameters: [...Array(33)].map((_, i) => `p${i}`),
          columns: ["foo"],
          rows: function* () {},
        }),
      RangeError,
    );
  });

  it('should throw an exception if the "rows" option is invalid', async function () {
    await expectAsyncError(
      () => this.db.table("a", { columns: ["x"] }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("b", { columns: ["x"], rows: undefined }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("c", { columns: ["x"], rows: {} }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("d", { columns: ["x"], rows: () => {} }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("e", { columns: ["x"], rows: function () {} }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("f", {
          columns: ["x"],
          rows: Object.create(Function.prototype),
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("g", {
          columns: ["x"],
          rows: Object.create(Object.getPrototypeOf(function* () {})),
        }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("h", {
          columns: ["x"],
          rows: Object.setPrototypeOf(
            () => {},
            Object.create(Object.getPrototypeOf(function* () {})),
          ),
        }),
      TypeError,
    );
  });

  it("should throw an exception if the provided name is empty", async function () {
    await expectAsyncError(
      () => this.db.table("", { columns: ["x"], rows: function* () {} }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("", { name: "a", columns: ["x"], rows: function* () {} }),
      TypeError,
    );
    await expectAsyncError(
      () =>
        this.db.table("", {
          name: "b",
          columns: ["x"],
          rows: function* b() {},
        }),
      TypeError,
    );
    await expectAsyncError(() => this.db.table("", function c() {}), TypeError);
  });

  it("should throw an exception if generator.length is invalid", async function () {
    const length = (x) =>
      Object.defineProperty(function* () {}, "length", { value: x });
    await expectAsyncError(
      () => this.db.table("a", { columns: ["x"], rows: length(undefined) }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("b", { columns: ["x"], rows: length(null) }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("c", { columns: ["x"], rows: length("1") }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("d", { columns: ["x"], rows: length(NaN) }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("e", { columns: ["x"], rows: length(Infinity) }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("f", { columns: ["x"], rows: length(1.000000001) }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("g", { columns: ["x"], rows: length(-0.000000001) }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("h", { columns: ["x"], rows: length(-1) }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("i", { columns: ["x"], rows: length(32.000000001) }),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.table("j", { columns: ["x"], rows: length(33) }),
      RangeError,
    );
  });

  it("should register a virtual table and return the database object", async function () {
    // Eponymous table (Object)
    expect(
      await this.db.table("a", { columns: ["x"], rows: function* () {} }),
    ).to.equal(this.db);

    const length = (x) =>
      Object.defineProperty(function* () {}, "length", { value: x });
    expect(
      await this.db.table("b", { columns: ["x"], rows: length(1) }),
    ).to.equal(this.db);
    expect(
      await this.db.table("c", { columns: ["x"], rows: length(32) }),
    ).to.equal(this.db);
  });

  it("should enable the registered virtual table to be queried from SQL", async function () {
    // We define the data INSIDE the generator so it can be serialized to the worker.
    await this.db.table("vtab", {
      columns: ["a", "b", "c", "d", "e"],
      *rows() {
        const rows = [
          { a: null, b: 123, c: 456.789, d: "foo", e: Buffer.from("bar") },
          { a: null, b: 987, c: 654.321, d: "oof", e: Buffer.from("rab") },
        ];
        for (const obj of rows) {
          yield Object.values(obj);
        }
      },
    });

    const expected = [
      { a: null, b: 123, c: 456.789, d: "foo", e: Buffer.from("bar") },
      { a: null, b: 987, c: 654.321, d: "oof", e: Buffer.from("rab") },
    ];

    expect(await this.db.prepare("SELECT * FROM vtab").all()).to.deep.equal(
      expected,
    );
    expect(
      await this.db.prepare("SELECT * FROM vtab WHERE b < 500").all(),
    ).to.deep.equal(expected.slice(0, 1));
    expect(
      await this.db.prepare("SELECT * FROM vtab ORDER BY d DESC").all(),
    ).to.deep.equal(expected.slice().reverse());
  });

  it("should infer parameters for the virtual table", async function () {
    await this.db.table("vtab", {
      columns: ["a", "b"],
      *rows(x, y) {
        yield [x, y];
        yield [x * 2, y * 3];
      },
    });
    expect(
      await this.db.prepare("SELECT * FROM vtab(?, ?)").all(2, 3),
    ).to.deep.equal([
      { a: 2, b: 3 },
      { a: 4, b: 9 },
    ]);
    expect(
      await this.db
        .prepare('SELECT * FROM vtab WHERE "$1" = ? AND "$2" = ?')
        .all(2, 3),
    ).to.deep.equal([
      { a: 2, b: 3 },
      { a: 4, b: 9 },
    ]);
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?, ?, ?)").all(),
      SqliteError,
    );
    await expectAsyncError(
      () =>
        this.db
          .prepare(
            'SELECT * FROM vtab WHERE "$1" = ? AND "$2" = ? AND "$3" = ?',
          )
          .all(),
      SqliteError,
    );
  });

  it("should accept explicit parameters for the virtual table", async function () {
    await this.db.table("vtab", {
      columns: ["a", "b"],
      parameters: ["x", "y", "z"],
      *rows(p1, p2, p3, p4) {
        yield [arguments[0], arguments[1] + arguments[2]];
        yield [arguments[0] * 2, (arguments[1] + arguments[2]) * 3];
      },
    });
    expect(
      await this.db.prepare("SELECT * FROM vtab(?, ?, ?)").all(2, 3, 4),
    ).to.deep.equal([
      { a: 2, b: 7 },
      { a: 4, b: 21 },
    ]);
    expect(
      await this.db
        .prepare("SELECT * FROM vtab WHERE x = ? AND y = ? AND z = ?")
        .all(2, 3, 4),
    ).to.deep.equal([
      { a: 2, b: 7 },
      { a: 4, b: 21 },
    ]);
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?, ?, ?, ?)").all(),
      SqliteError,
    );
    await expectAsyncError(
      () =>
        this.db
          .prepare(
            'SELECT * FROM vtab WHERE "$1" = ? AND "$2" = ? AND "$3" = ?',
          )
          .all(),
      SqliteError,
    );
  });

  it("should accept a large number of parameters for the virtual table", async function () {
    const args = [
      "foo",
      "bar",
      1,
      -2,
      Buffer.from("hello"),
      5,
      -10,
      "baz",
      99.9,
      -0.5,
    ];
    await this.db.table("vtab", {
      columns: ["x"],
      *rows(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10) {
        yield [p10];
        yield [p9];
        yield [p8];
        yield [p7];
        yield [p6];
        yield [p5];
        yield [p4];
        yield [p3];
        yield [p2];
        yield [p1];
      },
    });
    expect(
      await this.db
        .prepare("SELECT * FROM vtab(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .pluck()
        .all(args),
    ).to.deep.equal(args.slice().reverse());
    expect(
      await this.db
        .prepare("SELECT * FROM vtab(?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .pluck()
        .all(args.slice(0, -1)),
    ).to.deep.equal([null].concat(args.slice(0, -1).reverse()));
    await expectAsyncError(
      () =>
        this.db
          .prepare("SELECT * FROM vtab(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
          .all(),
      SqliteError,
    );
  });

  it("should correctly handle arguments even when used out of order", async function () {
    // Note: We removed side-effect checks (calls.push) as they are not thread-safe.
    // We rely on result correctness.
    await this.db.table("vtab", {
      columns: ["x", "y"],
      *rows(x, y) {
        yield { x, y };
      },
    });
    expect(
      await this.db
        .prepare('SELECT * FROM vtab WHERE "$1" = ? AND "$2" = ?')
        .get(10, 5),
    ).to.deep.equal({ x: 10, y: 5 });
    expect(
      await this.db
        .prepare('SELECT * FROM vtab WHERE "$2" = ? AND "$1" = ?')
        .get(5, 10),
    ).to.deep.equal({ x: 10, y: 5 });
    expect(
      await this.db
        .prepare('SELECT * FROM vtab WHERE "$2" = ? AND "$2" = ? AND "$1" = ?')
        .get(5, 5, 10),
    ).to.deep.equal({ x: 10, y: 5 });
    expect(
      await this.db
        .prepare('SELECT * FROM vtab WHERE "$2" = ? AND "$2" = ? AND "$1" = ?')
        .get(5, 9, 10),
    ).to.be.undefined;
    expect(
      await this.db
        .prepare('SELECT * FROM vtab WHERE "$2" = ? AND "$2" = ? AND "$1" = ?')
        .get(9, 5, 10),
    ).to.be.undefined;
  });

  it("should correctly handle arguments that are constrained to other arguments", async function () {
    await this.db.table("vtab", {
      columns: ["x", "y"],
      *rows(x, y) {
        yield { x, y };
      },
    });
    expect(
      await this.db
        .prepare('SELECT * FROM vtab WHERE "$1" = ? AND "$2" = "$1"')
        .get(10),
    ).to.deep.equal({ x: 10, y: 10 });
    expect(
      await this.db
        .prepare('SELECT * FROM vtab WHERE "$2" = "$1" AND "$1" = ?')
        .get(10),
    ).to.deep.equal({ x: 10, y: 10 });
    expect(
      await this.db
        .prepare(
          'SELECT * FROM vtab WHERE "$2" = ? AND "$2" = "$1" AND "$1" = ?',
        )
        .get(10, 10),
    ).to.deep.equal({ x: 10, y: 10 });
    expect(
      await this.db
        .prepare(
          'SELECT * FROM vtab WHERE "$2" = ? AND "$2" = "$1" AND "$1" = ?',
        )
        .get(5, 10),
    ).to.be.undefined;
    expect(
      await this.db
        .prepare(
          'SELECT * FROM vtab WHERE "$2" = "$1" AND "$2" = ? AND "$1" = ?',
        )
        .get(5, 10),
    ).to.be.undefined;
  });

  it("should cause the virtual table to throw when yielding an invalid value", async function () {
    await this.db.table("a", {
      columns: ["x"],
      *rows() {
        yield [42];
      },
    });
    await this.db.table("b", {
      columns: ["x"],
      *rows() {
        yield 42;
      },
    });
    await this.db.table("c", {
      columns: ["x"],
      *rows() {
        yield;
      },
    });
    await this.db.table("d", {
      columns: ["x"],
      *rows() {
        yield null;
      },
    });

    expect(await this.db.prepare("SELECT * FROM a").get()).to.deep.equal({
      x: 42,
    });
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM b").get(),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM c").get(),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM d").get(),
      TypeError,
    );
  });

  it("should allow arrays to be yielded as rows", async function () {
    await this.db.table("vtab", {
      columns: ["a", "b", "c", "d", "e"],
      *rows() {
        const rows = [
          { a: null, b: 123, c: 456.789, d: "foo", e: Buffer.from("bar") },
          { a: null, b: 987, c: 654.321, d: "oof", e: Buffer.from("rab") },
        ];
        for (const obj of rows) {
          yield Object.values(obj);
        }
      },
    });
    const expected = [
      { a: null, b: 123, c: 456.789, d: "foo", e: Buffer.from("bar") },
      { a: null, b: 987, c: 654.321, d: "oof", e: Buffer.from("rab") },
    ];
    expect(await this.db.prepare("SELECT * FROM vtab").all()).to.deep.equal(
      expected,
    );
  });

  it("should allow objects to be yielded as rows", async function () {
    await this.db.table("vtab", {
      columns: ["a", "b", "c", "d", "e"],
      *rows() {
        const rows = [
          { a: null, b: 123, c: 456.789, d: "foo", e: Buffer.from("bar") },
          { a: null, b: 987, c: 654.321, d: "oof", e: Buffer.from("rab") },
          { e: Buffer.from("hello"), d: "world", c: 0.1, b: 10, a: null },
          {
            d: "old friend",
            c: -0.1,
            e: Buffer.from("goodbye"),
            a: null,
            b: -10,
          },
        ];
        for (const obj of rows) {
          yield obj;
        }
      },
    });
    const expected = [
      { a: null, b: 123, c: 456.789, d: "foo", e: Buffer.from("bar") },
      { a: null, b: 987, c: 654.321, d: "oof", e: Buffer.from("rab") },
      { e: Buffer.from("hello"), d: "world", c: 0.1, b: 10, a: null },
      { d: "old friend", c: -0.1, e: Buffer.from("goodbye"), a: null, b: -10 },
    ];
    expect(await this.db.prepare("SELECT * FROM vtab").all()).to.deep.equal(
      expected,
    );
  });

  it("should throw an exception if an invalid array is yielded", async function () {
    await this.db.table("vtab", {
      columns: ["a", "b", "c", "d", "e"],
      *rows(n) {
        const tests = [
          [1, 2, 3, 4, 5],
          [1, 2, 3, 4, 5, 6],
          [1, 2, 3, 4],
          [],
          [1, 2, 3, 4, new Number(5)],
          [1, 2, 3, 4, [5]],
          [1, 2, 3, 4, new Date()],
        ];
        yield tests[n];
      },
    });
    // Reconstruct expected data locally
    expect(
      await this.db.prepare("SELECT * FROM vtab(?)").raw().all(0),
    ).to.deep.equal([[1, 2, 3, 4, 5]]);
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(1),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(2),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(3),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(4),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(5),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(6),
      TypeError,
    );
  });

  it("should throw an exception if an invalid object is yielded", async function () {
    await this.db.table("vtab", {
      columns: ["a", "b", "c", "d", "e"],
      *rows(n) {
        const tests = [
          { a: 1, b: 2, c: 3, d: 4, e: 5 },
          { a: 1, b: 2, c: 3, d: 4, e: 5, f: 6 },
          { a: 1, b: 2, c: 3, d: 4 },
          {},
          { a: 1, b: 2, c: 3, d: 4, e: new Number(5) },
          { a: 1, b: 2, c: 3, d: 4, e: [5] },
          { a: 1, b: 2, c: 3, d: 4, e: new Date() },
          { a: 1, b: 2, c: 3, d: 4, f: 5 },
        ];
        yield tests[n];
      },
    });
    expect(await this.db.prepare("SELECT * FROM vtab(?)").all(0)).to.deep.equal(
      [{ a: 1, b: 2, c: 3, d: 4, e: 5 }],
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(1),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(2),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(3),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(4),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(5),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(6),
      TypeError,
    );
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM vtab(?)").all(7),
      TypeError,
    );
  });

  it("should automatically assign rowids without affecting yielded objects", async function () {
    await this.db.table("a", {
      columns: ["x"],
      *rows() {
        yield* [{ x: 5 }, { x: 10 }];
      },
    });
    expect(await this.db.prepare("SELECT rowid, * FROM a").all()).to.deep.equal(
      [
        { rowid: 1, x: 5 },
        { rowid: 2, x: 10 },
      ],
    );

    await this.db.table("b", {
      columns: ["rowid"],
      *rows() {
        yield* [{ rowid: 5 }, { rowid: 10 }];
      },
    });
    expect(
      await this.db.prepare("SELECT oid AS oid, * FROM b").all(),
    ).to.deep.equal([
      { oid: 1, rowid: 5 },
      { oid: 2, rowid: 10 },
    ]);
  });

  it("should be driven by stmt.iterate() one row at a time", async function () {
    await this.db.table("vtab", {
      columns: ["x"],
      *rows() {
        yield ["foo"];
        yield ["bar"];
        yield ["baz"];
        yield ["qux"];
      },
    });
    const values = [];
    for await (const value of this.db
      .prepare("SELECT * FROM vtab")
      .pluck()
      .iterate()) {
      values.push(value);
      if (value === "baz") break;
    }
    expect(values).to.deep.equal(["foo", "bar", "baz"]);
  });

  it("should throw an exception if preparing a statement that uses an unsupported operator on a parameter", async function () {
    await this.db.table("vtab", {
      columns: ["a", "b"],
      parameters: ["x", "y", "z"],
      *rows(x, y, z) {
        yield [x, y + z];
        yield [x * 2, (y + z) * 3];
      },
    });
    expect(
      await this.db.prepare("SELECT * FROM vtab(?, ?, ?)").all(2, 3, 4),
    ).to.deep.equal([
      { a: 2, b: 7 },
      { a: 4, b: 21 },
    ]);
    expect(
      await this.db
        .prepare("SELECT * FROM vtab WHERE x = ? AND y = ? AND z = ?")
        .all(2, 3, 4),
    ).to.deep.equal([
      { a: 2, b: 7 },
      { a: 4, b: 21 },
    ]);
    await expectAsyncError(
      () =>
        this.db
          .prepare("SELECT * FROM vtab WHERE x = ? AND y = ? AND z > ?")
          .all(),
      SqliteError,
    );
    await expectAsyncError(
      () =>
        this.db
          .prepare("SELECT * FROM vtab WHERE x = ? AND y < ? AND z = ?")
          .all(),
      SqliteError,
    );
    await expectAsyncError(
      () =>
        this.db
          .prepare("SELECT * FROM vtab WHERE x IS ? AND y = ? AND z = ?")
          .all(),
      SqliteError,
    );
  });

  it("should properly escape column and parameter names", async function () {
    await this.db.table("vtab", {
      columns: ["foo);"],
      parameters: ['x"); SELECT "y', "y"],
      *rows(x, y) {
        yield [x];
        yield [y];
        yield [x + y];
      },
    });
    expect(
      await this.db
        .prepare(
          'SELECT "foo);" FROM vtab WHERE "x""); SELECT ""y" = ? AND y = ?',
        )
        .all(5, 10),
    ).to.deep.equal([{ "foo);": 5 }, { "foo);": 10 }, { "foo);": 15 }]);
  });

  it("should not allow CREATE VIRTUAL TABLE statements by default", async function () {
    // Standard Object -> Eponymous Table (no CREATE needed, but also not a module for CREATE)
    await this.db.table("mod", {
      columns: ["x"],
      *rows() {},
    });
    // These fail because 'mod' is not registered as a Module, it's an eponymous table 'mod'
    await expectAsyncError(
      () => this.db.exec("CREATE VIRTUAL TABLE a USING mod"),
      SqliteError,
    );
    await expectAsyncError(
      () => this.db.exec("CREATE VIRTUAL TABLE b USING mod()"),
      SqliteError,
    );
    await expectAsyncError(
      () => this.db.exec("CREATE VIRTUAL TABLE c USING mod(foo)"),
      SqliteError,
    );
  });

  it("should support CREATE VIRTUAL TABLE statements by accepting a factory function", async function () {
    // Factory Function -> Module
    await this.db.table("mod", function (...args) {
      return {
        columns: ["x"],
        *rows() {
          yield* args.map((x) => [x]);
        },
      };
    });
    // Modules are NOT eponymous
    await expectAsyncError(
      () => this.db.prepare("SELECT * FROM mod").all(),
      SqliteError,
    );

    await this.db.exec(
      `CREATE VIRTUAL TABLE foo USING mod(hello world, how are you?)`,
    );
    await this.db.exec(`CREATE VIRTUAL TABLE bar USING mod(1, 2, 3)`);

    expect(
      await this.db.prepare("SELECT x FROM foo").pluck().all(),
    ).to.deep.equal(["hello world", "how are you?"]);
    expect(
      await this.db.prepare("SELECT x FROM bar").pluck().all(),
    ).to.deep.equal(["1", "2", "3"]);
  });

  it("should correctly handle omitted arguments in any order", async function () {
    await this.db.table("vtab", {
      columns: ["value"],
      parameters: ["x", "y", "z"],
      *rows(x = 100, y = 10, z = 1) {
        yield [x + y + z];
      },
    });
    expect(
      await this.db
        .prepare("SELECT * FROM vtab(?, ?, ?)")
        .pluck()
        .get(2.2, 3.3, 4.4),
    ).to.equal(9.9);
    expect(
      await this.db.prepare("SELECT * FROM vtab(?, ?)").pluck().get(2.2, 3.3),
    ).to.equal(6.5);
    expect(
      await this.db.prepare("SELECT * FROM vtab(?)").pluck().get(2.2),
    ).to.equal(13.2);
    expect(await this.db.prepare("SELECT * FROM vtab").pluck().get()).to.equal(
      111,
    );
    expect(
      await this.db
        .prepare("SELECT * FROM vtab WHERE x = ? AND y = ? AND z = ?")
        .pluck()
        .get(2.2, 3.3, 4.4),
    ).to.equal(9.9);
    expect(
      await this.db
        .prepare("SELECT * FROM vtab WHERE x = ? AND y = ?")
        .pluck()
        .get(2.2, 3.3),
    ).to.equal(6.5);
    expect(
      await this.db
        .prepare("SELECT * FROM vtab WHERE x = ? AND z = ?")
        .pluck()
        .get(2.2, 3.3),
    ).to.equal(15.5);
    expect(
      await this.db
        .prepare("SELECT * FROM vtab WHERE y = ? AND z = ?")
        .pluck()
        .get(2.2, 3.3),
    ).to.equal(105.5);
    expect(
      await this.db.prepare("SELECT * FROM vtab WHERE x = ?").pluck().get(2.2),
    ).to.equal(13.2);
    expect(
      await this.db.prepare("SELECT * FROM vtab WHERE y = ?").pluck().get(2.2),
    ).to.equal(103.2);
    expect(
      await this.db.prepare("SELECT * FROM vtab WHERE z = ?").pluck().get(2.2),
    ).to.equal(112.2);
  });

  it("should not call the generator function if any arguments are NULL", async function () {
    let calls = 0;
    // We cannot track 'calls' side-effect.
    // But we know if generator is called with defaults, it returns > 0.
    // If it's not called, SQL returns nothing?
    // Actually, better-sqlite3 behavior is that the row generator is NOT invoked, resulting in 0 rows.
    await this.db.table("vtab", {
      columns: ["val"],
      parameters: ["x", "y", "z"],
      *rows(x = 0, y = 0, z = 0) {
        yield [x + y + z];
      },
    });
    expect(
      await this.db
        .prepare("SELECT val FROM vtab(?, ?, ?)")
        .pluck()
        .all(1, 10, 100),
    ).to.deep.equal([111]);
    expect(
      await this.db.prepare("SELECT val FROM vtab(?, ?)").pluck().all(1, 10),
    ).to.deep.equal([11]);
    expect(
      await this.db
        .prepare("SELECT val FROM vtab(?, ?, ?)")
        .pluck()
        .all(1, 10, null),
    ).to.deep.equal([]);
    expect(
      await this.db
        .prepare("SELECT val FROM vtab(?, ?, ?)")
        .pluck()
        .all(1, null, 100),
    ).to.deep.equal([]);
    expect(
      await this.db
        .prepare("SELECT val FROM vtab(?, ?, ?)")
        .pluck()
        .all(null, 10, 100),
    ).to.deep.equal([]);
    expect(
      await this.db.prepare("SELECT val FROM vtab(?, ?)").pluck().all(1, null),
    ).to.deep.equal([]);
  });

  it("should close a statement iterator that caused a virtual table to throw", async function () {
    await this.db.prepare("CREATE TABLE iterable (x INTEGER)").run();
    await this.db
      .prepare(
        "INSERT INTO iterable WITH RECURSIVE temp(x) AS (SELECT 1 UNION ALL SELECT x * 2 FROM temp LIMIT 10) SELECT * FROM temp",
      )
      .run();

    await this.db.table("vtab", {
      columns: ["value"],
      parameters: ["x"],
      *rows(x) {
        if (x >= 16) throw new Error("foo");
        yield [x];
      },
    });
    const iterator = this.db
      .prepare("SELECT value FROM vtab JOIN iterable USING (x)")
      .pluck()
      .iterate();

    let total = 0;
    try {
      for await (const value of iterator) {
        total += value;
      }
      throw new Error("Should have thrown");
    } catch (e) {
      expect(e.message).to.equal("foo");
    }

    expect(total).to.equal(1 + 2 + 4 + 8);
    expect(await iterator.next()).to.deep.equal({
      value: undefined,
      done: true,
    });
  });

  it("should not be able to affect bound buffers mid-query", async function () {
    const input = Buffer.alloc(1024 * 8).fill(0xbb);
    await this.db.table("vtab", {
      columns: ["x"],
      *rows(arg) {
        arg[0] = 2;
        yield [123];
      },
    });
    const row = await this.db
      .prepare('SELECT :input, "$1", x FROM vtab(:input)')
      .raw()
      .get({ input });
    expect(row[0].equals(Buffer.alloc(1024 * 8).fill(0xbb))).to.be.true;
    expect(row[1].equals(Buffer.alloc(1024 * 8).fill(0xbb))).to.be.true;
    expect(row[2]).to.equal(123);
  });

  describe("should propagate exceptions", function () {
    const exceptions = [
      new TypeError("foobar"),
      new Error("baz"),
      { yup: "ok" },
      "foobarbazqux",
      "",
      null,
      123.4,
    ];

    it("thrown in the factory function", async function () {
      await expectAsyncError(
        () =>
          this.db.table(`mod_fail`, () => {
            throw new Error("FactoryFail");
          }),
        Error,
      );
    });

    it("thrown in the rows() function", async function () {
      await this.db.table(`mod_rows_err`, {
        columns: ["x"],
        *rows() {
          throw new Error("baz");
        },
      });
      await expectAsyncError(
        () => this.db.prepare(`SELECT * FROM mod_rows_err`).pluck().all(),
        Error,
      );
    });

    it("thrown due to yielding an invalid value", async function () {
      await this.db.table("mod_yield_err", {
        columns: ["x"],
        *rows() {
          yield [new Number(42)];
        },
      });
      await expectAsyncError(
        () => this.db.prepare("SELECT * FROM mod_yield_err").all(),
        TypeError,
      );
    });
  });

  describe("should not affect external environment", function () {
    it("was_js_error state", async function () {
      await this.db.prepare("CREATE TABLE data (value INTEGER)").run();
      const stmt = this.db.prepare("SELECT value FROM data");
      await this.db.prepare("DROP TABLE data").run();

      await this.db.table("vtab", {
        columns: ["x"],
        *rows() {
          throw new Error("foo");
        },
      });

      await expectAsyncError(
        () => this.db.prepare("SELECT * FROM vtab").get(),
        Error,
      );
      await expectAsyncError(() => stmt.get(), SqliteError);
    });
  });

  it("should correctly handle limit and offset clause", async function () {
    await this.db.table("vtab", {
      columns: ["x"],
      *rows() {
        yield { x: 1 };
        yield { x: 2 };
        yield { x: 3 };
      },
    });
    expect(
      await this.db.prepare("SELECT * FROM vtab LIMIT 1").all(),
    ).to.deep.equal([{ x: 1 }]);
    expect(
      await this.db.prepare("SELECT * FROM vtab LIMIT 1 OFFSET 2").all(),
    ).to.deep.equal([{ x: 3 }]);
    expect(
      await this.db.prepare("SELECT * FROM vtab LIMIT 100 OFFSET 1").all(),
    ).to.deep.equal([{ x: 2 }, { x: 3 }]);
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
