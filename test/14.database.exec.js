"use strict";
const Database = require("../src");

describe("Database#exec()", function () {
  beforeEach(async function () {
    this.db = await Database.create(util.next());
  });
  afterEach(async function () {
    await this.db.close();
  });

  const expectRejection = async (
    promise,
    ExpectedError,
    propertyName,
    propertyValue,
  ) => {
    try {
      await promise;
      throw new Error("Expected promise to reject, but it resolved");
    } catch (err) {
      // console.log(err);
      expect(err).to.be.instanceof(ExpectedError);
      if (propertyName) {
        expect(err).to.have.property(propertyName);
      }
      if (propertyValue) {
        expect(err[propertyName]).to.equal(propertyValue);
      }
    }
  };

  it("should throw an exception if a string is not provided", async function () {
    await expectRejection(this.db.exec(123), TypeError);
    await expectRejection(this.db.exec(0), TypeError);
    await expectRejection(this.db.exec(null), TypeError);
    await expectRejection(this.db.exec(), TypeError);
    await expectRejection(
      this.db.exec(new String("CREATE TABLE entries (a TEXT, b INTEGER)")),
      TypeError,
    );
  });
  it("should throw an exception if invalid SQL is provided", async function () {
    await expectRejection(
      this.db.exec("CREATE TABLE entries (a TEXT, b INTEGER"),
      Database.SqliteError,
      "code",
      "SQLITE_ERROR",
    );
  });
  it("should obey the restrictions of readonly mode", async function () {
    await this.db.close();
    this.db = await Database.create(util.current(), { readonly: true });
    await expectRejection(
      this.db.exec("CREATE TABLE people (name TEXT)"),
      Database.SqliteError,
      "code",
      "SQLITE_READONLY",
    );
    await this.db.exec("SELECT 555");
  });
  it("should execute the SQL, returning the database object itself", async function () {
    const returnValues = [];

    const r1 = await this.db.exec("CREATE TABLE entries (a TEXT, b INTEGER)");
    const r2 = await this.db.exec(
      "INSERT INTO entries VALUES ('foobar', 44); INSERT INTO entries VALUES ('baz', NULL);",
    );
    const r3 = await this.db.exec("SELECT * FROM entries");

    expect(r1).to.equal(this.db);
    expect(r2).to.equal(this.db);
    expect(r3).to.equal(this.db);

    const rows = await this.db
      .prepare("SELECT * FROM entries ORDER BY rowid")
      .all();
    expect(rows.length).to.equal(2);
    expect(rows[0].a).to.equal("foobar");
    expect(rows[0].b).to.equal(44);
    expect(rows[1].a).to.equal("baz");
    expect(rows[1].b).to.equal(null);
  });
});
