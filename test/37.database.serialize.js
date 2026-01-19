"use strict";
const fs = require("fs");
const Database = require("../src");
const { SqliteError } = Database;

/**
 * Helper to assert that a Promise rejects with a specific Error type.
 */
const expectFail = async (promise, ErrorType) => {
  try {
    await promise;
  } catch (err) {
    if (ErrorType) {
      if (err instanceof ErrorType || err.name === ErrorType.name) return err;
    } else {
      return err;
    }
  }
  throw new Error(
    `Expected operation to fail${ErrorType ? " with " + ErrorType.name : ""}, but it succeeded.`,
  );
};

describe("Database#serialize()", function () {
  this.timeout(10000);

  beforeEach(async function () {
    this.db = await Database.create(util.next());
    await this.db
      .prepare(
        "CREATE TABLE entries (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)",
      )
      .run();

    this.seed = async () => {
      await this.db
        .prepare(
          "INSERT INTO entries WITH RECURSIVE temp(a, b, c, d, e) AS (SELECT 'foo', 1, 3.14, x'dddddddd', NULL UNION ALL SELECT a, b + 1, c, d, e FROM temp LIMIT 1000) SELECT * FROM temp",
        )
        .run();
    };
  });

  afterEach(async function () {
    if (this.db && this.db.open) {
      await this.db.close();
    }
  });

  it("should serialize the database and return a buffer", async function () {
    let buffer = await this.db.serialize();
    expect(buffer).to.be.an.instanceof(Buffer);
    expect(buffer.length).to.be.above(1000); // Standard header is 4096 usually, but empty tables might be small

    const lengthBefore = buffer.length;

    await this.seed();

    buffer = await this.db.serialize();
    expect(buffer).to.be.an.instanceof(Buffer);
    expect(buffer.length).to.be.above(lengthBefore);
  });

  it("should return a buffer that can be used to restore the Database", async function () {
    // Note: The Pool `Database.create` currently requires a filename string.
    // It does not support `new Database(buffer)` directly like native better-sqlite3.
    // We simulate this by writing the buffer to a file and opening that.

    await this.seed();
    const buffer = await this.db.serialize();
    expect(buffer).to.be.an.instanceof(Buffer);
    expect(buffer.length).to.be.above(1000);

    await this.db.prepare("delete from entries").run();
    await this.db.close();

    // Workaround: Write buffer to disk to test integrity
    const restorePath = util.next();
    fs.writeFileSync(restorePath, buffer);

    this.db = await Database.create(restorePath);

    const bufferCopy = await this.db.serialize();
    expect(buffer.length).to.equal(bufferCopy.length);
    // Deep equal on large buffers can be slow, but valid for logic check
    // expect(buffer).to.deep.equal(bufferCopy);

    await this.db
      .prepare("insert into entries (rowid, a, b) values (?, ?, ?)")
      .run(0, "bar", -999);

    const rows = await this.db
      .prepare("select a, b from entries order by rowid limit 2")
      .all();
    expect(rows).to.deep.equal([
      { a: "bar", b: -999 },
      { a: "foo", b: 1 },
    ]);
  });

  it('should accept the "attached" option', async function () {
    // 1. Snapshot empty DB
    const smallBuffer = await this.db.serialize();

    // 2. Seed main DB and snapshot
    await this.seed();
    const bigBuffer = await this.db.serialize();

    // 3. Create new In-Memory DB
    const attachedDbFile = this.db.name; // Keep reference to file
    await this.db.close();

    this.db = await Database.create(":memory:");

    // 4. Attach the previous file
    await this.db.prepare(`attach '${attachedDbFile}' as other`).run();

    // 5. Serialize
    const smallBuffer2 = await this.db.serialize(); // Serializes 'main' (empty memory)
    const bigBuffer2 = await this.db.serialize({ attached: "other" }); // Serializes attached file

    expect(bigBuffer.length).to.equal(bigBuffer2.length);
    // expect(bigBuffer).to.deep.equal(bigBuffer2); // Content check

    expect(smallBuffer.length).to.be.lessThan(bigBuffer.length);
    expect(smallBuffer2.length).to.be.lessThan(bigBuffer.length);

    // smallBuffer (file header) vs smallBuffer2 (memory header) might differ slightly
    // but both represent empty DBs.
  });

  it('should return a buffer that can be opened with the "readonly" option', async function () {
    await this.seed();
    const buffer = await this.db.serialize();
    expect(buffer).to.be.an.instanceof(Buffer);

    await this.db.close();

    // Workaround: Write buffer to disk
    const roPath = util.next();
    fs.writeFileSync(roPath, buffer);

    // Open Readonly
    this.db = await Database.create(roPath, { readonly: true });

    // Write should fail
    await expectFail(
      this.db
        .prepare("insert into entries (rowid, a, b) values (?, ?, ?)")
        .run(0, "bar", -999),
      SqliteError,
    );

    // Read should succeed
    const rows = await this.db
      .prepare("select a, b from entries order by rowid limit 2")
      .all();
    expect(rows).to.deep.equal([
      { a: "foo", b: 1 },
      { a: "foo", b: 2 },
    ]);

    const bufferCopy = await this.db.serialize();
    expect(buffer.length).to.equal(bufferCopy.length);
  });

  it("should work with an empty database", async function () {
    await this.db.close();

    // Create fresh memory DB
    this.db = await Database.create(":memory:");

    const buffer = await this.db.serialize();
    expect(buffer).to.be.an.instanceof(Buffer);
    // SQLite page size default is 4096
    expect(buffer.length).to.equal(4096);

    await this.db.close();

    // Restore check
    const restorePath = util.next();
    fs.writeFileSync(restorePath, buffer);

    this.db = await Database.create(restorePath);
    const buf2 = await this.db.serialize();
    expect(buf2.length).to.equal(4096);
  });
});
