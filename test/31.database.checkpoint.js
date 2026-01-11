"use strict";
const fs = require("fs");
const Database = require("../src");

describe("Database#pragma('wal_checkpoint(RESTART)')", function () {
  let db1, db2;

  before(async function () {
    db1 = await Database.create(util.next());
    db2 = await Database.create(util.next());

    // Note: The pool automatically sets WAL for the writer, but we set it here explicitly
    // to match the original test logic and ensure state.
    await db1.pragma("journal_mode = WAL");
    await db1.prepare("CREATE TABLE entries (a TEXT, b INTEGER)").run();

    await db2.pragma("journal_mode = WAL");
    await db2.prepare("CREATE TABLE entries (a TEXT, b INTEGER)").run();
  });

  after(async function () {
    if (db1 && db1.open) await db1.close();
    if (db2 && db2.open) await db2.close();
  });

  async function fillWall(count, expectation) {
    // Iterate sequentially to handle async operations correctly
    for (const db of [db1, db2]) {
      let size1, size2;
      for (let i = 0; i < count; ++i) {
        size1 = fs.statSync(`${db.name}-wal`).size;
        await db.prepare("INSERT INTO entries VALUES (?, ?)").run("bar", 999);
        size2 = fs.statSync(`${db.name}-wal`).size;
        expectation(size2, size1, db);
      }
    }
  }

  describe("when used without a specified database", function () {
    it("every insert should increase the size of the WAL file", async function () {
      await fillWall(10, (b, a) => expect(b).to.be.above(a));
    });

    it("inserts after a checkpoint should NOT increase the size of the WAL file", async function () {
      // Attach db2 to db1
      await db1.prepare(`ATTACH '${db2.name}' AS foobar`).run();

      // Checkpoint all attached databases (db1 and foobar)
      await db1.pragma("wal_checkpoint(RESTART)");

      // Expectation: File size remains same (reuse) because checkpoint reset the WAL index
      await fillWall(10, (b, a) => expect(b).to.equal(a));
    });
  });

  describe("when used on a specific database", function () {
    it("every insert should increase the size of the WAL file", async function () {
      // Clean up previous state
      await db1.prepare("DETACH foobar").run();
      await db1.close();
      await db2.close();

      // Re-open
      db1 = await Database.create(db1.name);
      db2 = await Database.create(db2.name);

      // Note: Re-opening usually resets WAL mode in standard SQLite if not persistent,
      // but better-sqlite3-pool enforces WAL on writer startup.

      await db1.prepare("CREATE TABLE _unused (a TEXT, b INTEGER)").run();
      await db2.prepare("CREATE TABLE _unused (a TEXT, b INTEGER)").run();

      await fillWall(10, (b, a) => expect(b).to.be.above(a));
    });

    it("inserts after a checkpoint should NOT increase the size of the WAL file", async function () {
      await db1.prepare(`ATTACH '${db2.name}' AS bazqux`).run();

      // Checkpoint ONLY the attached 'bazqux' (db2)
      await db1.pragma("bazqux.wal_checkpoint(RESTART)");

      await fillWall(10, (b, a, db) => {
        if (db === db1) {
          // db1 was NOT checkpointed, so it should grow
          expect(b).to.be.above(a);
        } else {
          // db2 (bazqux) WAS checkpointed, so it should reuse space (equal size)
          expect(b).to.be.equal(a);
        }
      });
    });
  });
});
