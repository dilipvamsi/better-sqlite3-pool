"use strict";
const fs = require("fs");
const path = require("path");
const Database = require("../src"); // Adjust path to your main file

// Simple temp file helper
const tempDir = path.join(__dirname, "temp");
if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir);
const nextFile = () =>
  path.join(tempDir, `test-${Date.now()}-${Math.random()}.db`);

describe("new Database()", function () {
  this.timeout(5000); // Increase timeout for worker spawning

  afterEach(async function () {
    if (this.db) await this.db.close();
  });

  it("should allow disk-bound databases to be created", async function () {
    const filename = nextFile();
    this.db = await Database.create(filename);

    // Check instance type
    expect(this.db).to.be.an.instanceof(Database);

    // Verify it works by running a simple query
    const result = await this.db.prepare("SELECT 1 as val").get();
    expect(result.val).to.equal(1);
  });

  it("should allow in-memory databases to be created", async function () {
    this.db = await Database.create(":memory:");
    const result = await this.db.prepare("SELECT 1 as val").get();
    expect(result.val).to.equal(1);
  });

  it("should accept pool options (min/max)", async function () {
    const filename = nextFile();
    this.db = await Database.create(filename, { min: 1, max: 4 });

    // Verify internal state (specific to your implementation)
    expect(this.db.minReaders).to.equal(1);
    expect(this.db.maxReaders).to.equal(4);
    expect(this.db.readers).to.be.an("array");
  });

  it("should have a proper prototype chain", async function () {
    const filename = nextFile();
    this.db = await Database.create(filename);

    expect(this.db).to.be.an.instanceof(Database);
    expect(this.db.constructor).to.equal(Database);
    expect(Database.prototype.close).to.be.a("function");
    expect(Database.prototype.prepare).to.be.a("function");
  });

  it("should work properly when subclassed", async function () {
    class MyDatabase extends Database {
      foo() {
        return 999;
      }
    }

    const filename = nextFile();
    this.db = await MyDatabase.create(filename);

    expect(this.db).to.be.an.instanceof(Database);
    expect(this.db).to.be.an.instanceof(MyDatabase);
    expect(this.db.foo()).to.equal(999);

    // Verify functionality still works
    const result = await this.db.prepare("SELECT 1 as val").get();
    expect(result.val).to.equal(1);
  });

  it("should not throw synchronously on invalid paths (errors happen in workers)", async function () {
    // Unlike better-sqlite3, your constructor only sets up threads.
    // File errors happen async inside the worker.
    const filepath = `temp/nonexistent/dir/${Date.now()}.db`;

    // This should NOT throw synchronously
    Database.create(filepath).catch((err) =>
      expect(err).to.be.instanceof(TypeError),
    );
  });

  it("should propagate worker errors when executing queries", async function () {
    const filepath = `temp/nonexistent/dir/${Date.now()}.db`;
    // The error should appear when we try to use the DB
    try {
      this.db = await Database.create(filepath);
      await this.db.prepare("SELECT 1").get();
      throw new Error("Should have failed");
    } catch (err) {
      expect(err.message).to.include(
        "Cannot open database because the directory does not exist",
      );
      // Or whatever error message better-sqlite3 returns inside the worker
    }
  });

  it("should behave correctly as a single-threaded instance for :memory: databases", async function () {
    this.db = await Database.create(":memory:");

    // 1. Verify readers are 0
    expect(this.db.readers.length).to.equal(0);
    expect(this.db.memory).to.be.true;

    // 2. Write data (Goes to Writer)
    await this.db.exec("CREATE TABLE memo (id INTEGER PRIMARY KEY, txt TEXT)");
    await this.db.prepare("INSERT INTO memo (txt) VALUES (?)").run("visible");

    // 3. Read data (MUST go to Writer to see the table)
    const row = await this.db.prepare("SELECT * FROM memo").get();

    // If it spawned a reader, this would fail (table not found) or return undefined
    expect(row).to.exist;
    expect(row.txt).to.equal("visible");
  });
});
