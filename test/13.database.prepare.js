"use strict";
const Database = require("../.");

describe("Database#prepare()", function () {
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

  function assertStmt(stmt, source, db, reader, readonly) {
    expect(stmt.source).to.equal(source);
    expect(stmt.constructor.name).to.equal("Statement");
    expect(stmt.database).to.equal(db);
    expect(stmt.reader).to.equal(reader);
    expect(stmt.readonly).to.equal(readonly);
    expect(() => new stmt.constructor(source)).to.throw(TypeError);
  }

  it("should throw an exception if a string is not provided", function () {
    expect(() => this.db.prepare(123)).to.throw(TypeError);
    expect(() => this.db.prepare(0)).to.throw(TypeError);
    expect(() => this.db.prepare(null)).to.throw(TypeError);
    expect(() => this.db.prepare()).to.throw(TypeError);
    expect(() =>
      this.db.prepare(new String("CREATE TABLE people (name TEXT)")),
    ).to.throw(TypeError);
  });
  it("should throw an exception if invalid SQL is provided", async function () {
    // 1. Syntax Error (Missing closing parenthesis)
    const stmt1 = this.db.prepare("CREATE TABLE people (name TEXT");
    // The error happens here, inside the worker:
    await expectRejection(stmt1.run(), Database.SqliteError);

    // 2. Logic Error (Table does not exist yet)
    const stmt2 = this.db.prepare("INSERT INTO people VALUES (?)");
    await expectRejection(stmt2.run("foo"), Database.SqliteError);
  });
  it("should throw an exception if no statements are provided", function () {
    expect(() => this.db.prepare("")).to.throw(RangeError);
    expect(() => this.db.prepare(";")).to.throw(RangeError);
  });
  it("should throw an exception if more than one statement is provided", async function () {
    await expectRejection(
      this.db
        .prepare(
          "CREATE TABLE people (name TEXT);CREATE TABLE animals (name TEXT)",
        )
        .run(),
      RangeError,
    );
    await expectRejection(
      this.db.prepare("CREATE TABLE people (name TEXT);/").run(),
      RangeError,
    );
    await expectRejection(
      this.db.prepare("CREATE TABLE people (name TEXT);-").run(),
      RangeError,
    );
    await expectRejection(
      this.db.prepare("CREATE TABLE people (name TEXT);--\n/").run(),
      RangeError,
    );
    await expectRejection(
      this.db.prepare("CREATE TABLE people (name TEXT);--\nSELECT 123").run(),
      RangeError,
    );
    await expectRejection(
      this.db
        .prepare("CREATE TABLE people (name TEXT);-- comment\nSELECT 123")
        .run(),
      RangeError,
    );

    await expectRejection(
      this.db.prepare("CREATE TABLE people (name TEXT);/**/-").run(),
      RangeError,
    );
    await expectRejection(
      this.db.prepare("CREATE TABLE people (name TEXT);/**/SELECT 123").run(),
      RangeError,
    );
    await expectRejection(
      this.db
        .prepare("CREATE TABLE people (name TEXT);/* comment */SELECT 123")
        .run(),
      RangeError,
    );
  });
  it("should create a prepared Statement object", function () {
    const stmt1 = this.db.prepare("CREATE TABLE people (name TEXT) ");
    const stmt2 = this.db.prepare("CREATE TABLE people (name TEXT); ");
    assertStmt(
      stmt1,
      "CREATE TABLE people (name TEXT) ",
      this.db,
      false,
      false,
    );
    assertStmt(
      stmt2,
      "CREATE TABLE people (name TEXT); ",
      this.db,
      false,
      false,
    );
    expect(stmt1).to.not.equal(stmt2);
    expect(stmt1).to.not.equal(
      this.db.prepare("CREATE TABLE people (name TEXT) "),
    );
  });
  it("should create a prepared Statement object with just an expression", function () {
    const stmt = this.db.prepare("SELECT 555");
    assertStmt(stmt, "SELECT 555", this.db, true, true);
  });
  it('should set the correct values for "reader" and "readonly"', function () {
    this.db.exec("CREATE TABLE data (value)");
    assertStmt(
      this.db.prepare("SELECT 555"),
      "SELECT 555",
      this.db,
      true,
      true,
    );
    assertStmt(this.db.prepare("BEGIN"), "BEGIN", this.db, false, true);
    assertStmt(
      this.db.prepare("BEGIN EXCLUSIVE"),
      "BEGIN EXCLUSIVE",
      this.db,
      false,
      false,
    );
    assertStmt(
      this.db.prepare("DELETE FROM data RETURNING *"),
      "DELETE FROM data RETURNING *",
      this.db,
      true,
      false,
    );
  });
  it("should create a prepared Statement object ignoring trailing comments and whitespace", function () {
    assertStmt(
      this.db.prepare("SELECT 555;     "),
      "SELECT 555;     ",
      this.db,
      true,
      true,
    );
    assertStmt(
      this.db.prepare("SELECT 555;-- comment"),
      "SELECT 555;-- comment",
      this.db,
      true,
      true,
    );
    assertStmt(
      this.db.prepare("SELECT 555;--abc\n--de\n--f"),
      "SELECT 555;--abc\n--de\n--f",
      this.db,
      true,
      true,
    );
    assertStmt(
      this.db.prepare("SELECT 555;/* comment */"),
      "SELECT 555;/* comment */",
      this.db,
      true,
      true,
    );
    assertStmt(
      this.db.prepare("SELECT 555;/* comment */-- comment"),
      "SELECT 555;/* comment */-- comment",
      this.db,
      true,
      true,
    );
    assertStmt(
      this.db.prepare("SELECT 555;-- comment\n/* comment */"),
      "SELECT 555;-- comment\n/* comment */",
      this.db,
      true,
      true,
    );
  });
});
