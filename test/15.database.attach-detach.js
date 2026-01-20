"use strict";
const fs = require("fs");
const path = require("path");
const Database = require("../src");

describe("Database#attach() and #detach()", function () {
  it("should attach a secondary database, perform cross-database joins, and detach", async function () {
    // 1. Setup the "Secondary" database (the one to be attached)
    const attachedFilename = util.next();
    const setupDb = await Database.create(attachedFilename);

    await setupDb.exec(
      "CREATE TABLE attached_users (id INTEGER PRIMARY KEY, name TEXT)",
    );
    await setupDb.exec(
      "INSERT INTO attached_users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
    );
    await setupDb.close();

    // 2. Setup the "Main" database
    const mainDb = (this.db = await Database.create(util.next()));
    await mainDb.exec(
      "CREATE TABLE main_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)",
    );
    await mainDb.exec(
      "INSERT INTO main_orders (id, user_id, amount) VALUES (100, 1, 50)",
    );

    // 3. Attach the secondary database
    await mainDb.attach(attachedFilename, "secondary");

    // 4. Verify basic read from attached DB
    const user = await mainDb
      .prepare("SELECT * FROM secondary.attached_users WHERE id = ?")
      .get(2);
    expect(user).to.deep.equal({ id: 2, name: "Bob" });

    // 5. Verify Cross-Database JOIN
    const result = await mainDb
      .prepare(
        `
      SELECT o.id as order_id, u.name as user_name
      FROM main_orders o
      JOIN secondary.attached_users u ON o.user_id = u.id
      WHERE o.id = 100
    `,
      )
      .get();

    expect(result).to.deep.equal({ order_id: 100, user_name: "Alice" });

    // 6. Detach
    await mainDb.detach("secondary");

    // 7. Verify Detach (Querying should fail now)
    try {
      await mainDb.prepare("SELECT * FROM secondary.attached_users").get();
      throw new Error("Query should have failed after detach");
    } catch (err) {
      // Expecting SQLITE_ERROR: no such table: secondary.attached_users
      expect(err.message).to.include("no such table");
    }
  });

  it("should apply journal_mode to the attached database", async function () {
    // 1. Create a DB file for attaching
    const attachedFilename = util.next();
    const tempDb = await Database.create(attachedFilename);
    // Explicitly set to DELETE first to ensure we test the transition
    await tempDb.pragma("journal_mode = DELETE");
    await tempDb.close();

    // 2. Open Main DB
    const db = (this.db = await Database.create(util.next()));

    // 3. Attach with journalMode option
    await db.attach(attachedFilename, "wal_db", { journalMode: "WAL" });

    // 4. Check the pragma on the attached schema
    const mode = await db.pragma("wal_db.journal_mode", { simple: true });
    expect(mode.toUpperCase()).to.equal("WAL");

    // 5. Force a write to ensure WAL file creation (Fix for lazy creation)
    // Changing journal mode header *should* be enough, but explicit write guarantees flush.
    await db.exec("CREATE TABLE wal_db.force_wal_creation (id INTEGER)");

    // 6. Verify the -wal file exists on disk
    expect(fs.existsSync(`${attachedFilename}-wal`)).to.be.true;

    await db.detach("wal_db");
  });

  it("should throw TypeError for invalid arguments", async function () {
    const db = (this.db = await Database.create(util.next()));
    const filename = util.next();

    // Helper to verify rejection
    const expectReject = async (promise) => {
      try {
        await promise;
        throw new Error("Should have rejected");
      } catch (err) {
        expect(err).to.be.an.instanceof(TypeError);
      }
    };

    // Invalid filename
    await expectReject(db.attach(123, "alias"));
    // Invalid alias
    await expectReject(db.attach(filename, {}));
    // Invalid options
    await expectReject(db.attach(filename, "alias", { journalMode: 123 }));
    // Invalid detach alias
    await expectReject(db.detach(null));
  });

  it("should support attach/detach within an exclusive transaction connection", async function () {
    // 1. Setup secondary file
    const attachedFilename = util.next();
    const temp = await Database.create(attachedFilename);
    await temp.exec("CREATE TABLE t1 (a INTEGER)");
    await temp.close();

    // 2. Open Main DB
    const db = (this.db = await Database.create(util.next()));

    // 3. Acquire exclusive connection
    const conn = await db.acquire();
    try {
      // 4. Attach via connection
      await conn.attach(attachedFilename, "conn_alias");

      // 5. Verify access
      const count = await conn
        .prepare("SELECT count(*) as c FROM conn_alias.t1")
        .get();
      expect(count.c).to.equal(0);

      // 6. Detach via connection
      await conn.detach("conn_alias");

      // 7. Verify failure
      try {
        await conn.prepare("SELECT * FROM conn_alias.t1").run();
        throw new Error("Should have failed");
      } catch (err) {
        expect(err.message).to.include("no such table");
      }
    } finally {
      await conn.release();
    }
  });
});
