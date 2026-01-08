"use strict";
const { existsSync } = require("fs");
const Database = require("../src");

describe("Database#close()", function () {
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

  it("should cause db.open to return false", async function () {
    expect(this.db.open).to.be.true;
    await this.db.close();
    expect(this.db.open).to.be.false;
  });
  it("should return the database object", async function () {
    expect(this.db.open).to.be.true;
    expect(await this.db.close()).to.equal(this.db);
    expect(this.db.open).to.be.false;
  });

  it("should prevent any further database operations", async function () {
    await this.db.close();
    await expectRejection(
      this.db.exec("CREATE TABLE people (name TEXT)"),
      TypeError,
    );
    expect(() => this.db.prepare("CREATE TABLE cats (name TEXT)")).to.throw(
      TypeError,
    );
    expect(() => this.db.transaction(() => {})).to.throw(TypeError);
    await expectRejection(this.db.pragma("cache_size"), TypeError);
    await expectRejection(
      this.db.function("foo", () => {}),
      TypeError,
    );
    await expectRejection(
      this.db.table("foo", () => {}),
      TypeError,
    );
    await expectRejection(
      this.db.aggregate("foo", { step: () => {} }),
      TypeError,
    );
  });

  it("should prevent any existing statements from running", async function () {
    await this.db.prepare("CREATE TABLE people (name TEXT)").run();
    const stmt1 = this.db.prepare("SELECT * FROM people");
    const stmt2 = this.db.prepare("INSERT INTO people VALUES ('foobar')");

    this.db.prepare("SELECT * FROM people").bind();
    this.db.prepare("INSERT INTO people VALUES ('foobar')").bind();
    await this.db.prepare("SELECT * FROM people").get();
    await this.db.prepare("SELECT * FROM people").all();
    await this.db.prepare("SELECT * FROM people").iterate().return();
    await this.db.prepare("INSERT INTO people VALUES ('foobar')").run();

    await this.db.close();

    expect(() => stmt1.bind()).to.throw(TypeError);
    expect(() => stmt2.bind()).to.throw(TypeError);
    await expectRejection(stmt1.get(), TypeError);
    await expectRejection(stmt1.all(), TypeError);
    expect(() => stmt1.iterate()).to.throw(TypeError);
    await expectRejection(stmt2.run(), TypeError);
  });

  it("should delete the database's associated temporary files", async function () {
    expect(existsSync(util.current())).to.be.true;
    await this.db.pragma("journal_mode = WAL");
    await this.db.prepare("CREATE TABLE people (name TEXT)").run();
    await this.db.prepare("INSERT INTO people VALUES (?)").run("foobar");
    expect(existsSync(`${util.current()}-wal`)).to.be.true;

    await this.db.close();

    expect(existsSync(util.current())).to.be.true;
    expect(existsSync(`${util.current()}-wal`)).to.be.false;
  });
});
