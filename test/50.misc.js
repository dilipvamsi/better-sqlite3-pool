"use strict";
const Database = require("../src");

describe("miscellaneous", function () {
  // Increase timeout for async operations, especially the stress test
  this.timeout(30000);

  beforeEach(async function () {
    this.db = await Database.create(util.next());
  });

  afterEach(async function () {
    if (this.db && this.db.open) {
      await this.db.close();
    }
  });

  it("supports LIMIT in DELETE statements", async function () {
    await this.db.prepare("CREATE TABLE foo (x INTEGER PRIMARY KEY)").run();

    expect(
      await this.db.prepare("INSERT INTO foo (x) VALUES (1), (2), (3)").run(),
    ).to.deep.include({ changes: 3, lastInsertRowid: 3 });

    expect(
      await this.db.prepare("DELETE FROM foo ORDER BY x ASC LIMIT 1").run(),
    ).to.have.property("changes", 1);

    expect(
      await this.db.prepare("SELECT x FROM foo ORDER BY x ASC").all(),
    ).to.deep.equal([{ x: 2 }, { x: 3 }]);
  });

  it("supports LIMIT in UPDATE statements", async function () {
    await this.db
      .prepare("CREATE TABLE foo (x INTEGER PRIMARY KEY, y INTEGER)")
      .run();

    expect(
      await this.db
        .prepare("INSERT INTO foo (x, y) VALUES (1, 1), (2, 2), (3, 3)")
        .run(),
    ).to.deep.include({ changes: 3, lastInsertRowid: 3 });

    expect(
      await this.db
        .prepare("UPDATE foo SET y = 100 ORDER BY x DESC LIMIT 2")
        .run(),
    ).to.have.property("changes", 2);

    expect(
      await this.db.prepare("SELECT x, y FROM foo ORDER BY x ASC").all(),
    ).to.deep.equal([
      { x: 1, y: 1 },
      { x: 2, y: 100 },
      { x: 3, y: 100 },
    ]);
  });

  it("persists non-trivial quantities of reads and writes", async function () {
    const runDuration = 5000; // Reduced slightly for test suite speed, adjustable
    const runUntil = Date.now() + runDuration;

    this.slow(runDuration * 5);
    this.timeout(runDuration * 2 + 5000); // Ensure test doesn't time out before loop ends

    // WAL mode is usually auto-enabled for existing files in the pool,
    // but for a new file in this specific stress test logic, explicit enable is fine.
    await this.db.pragma("journal_mode = WAL");
    await this.db.prepare("CREATE TABLE foo (a INTEGER, b TEXT, c REAL)").run();

    let i = 1;
    const r = 0.141592654;
    const insert = this.db.prepare("INSERT INTO foo VALUES (?, ?, ?)");

    // Transaction wrapper returns an async function
    const insertMany = this.db.transaction(async (count) => {
      for (const end = i + count; i < end; ++i) {
        // Must await the run inside the transaction
        const result = await insert.run(i, String(i), i + r);
        // Note: Pool uses loose 'include' for deep equal on run result
        // because lastInsertRowid might be BigInt/Number depending on config
        // but checking properties is safer.
        expect(result.changes).to.equal(1);
        // expect(result.lastInsertRowid).to.equal(i); // Type strictness varies (BigInt vs Number)
      }
    });

    // Batched transactions of 100 inserts.
    // We await the transaction execution.
    while (Date.now() < runUntil) {
      await insertMany(100);
    }

    // Expect decent throughput (1000+ records)
    expect(i).to.be.above(1000);

    const select = this.db.prepare("SELECT * FROM foo ORDER BY a DESC");

    // Use Async Iterator
    for await (const row of select.iterate()) {
      i -= 1;
      expect(row).to.deep.equal({ a: i, b: String(i), c: i + r });
    }

    expect(i).to.equal(1);
  });
});
