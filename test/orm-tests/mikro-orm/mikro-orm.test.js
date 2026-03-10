const { MikroORM, EntitySchema } = require("@mikro-orm/core");
const { SqliteDriver } = require("@mikro-orm/sqlite");
const { expect } = require("chai");
const path = require("path");
const fs = require("fs");
const adapter = require("../../../src/adapter");

// --- Entities using EntitySchema ---
const User = new EntitySchema({
  name: "User",
  properties: {
    id: { type: "number", primary: true },
    name: { type: "string" },
  },
});

describe("MikroORM Integration via SQLite3Adapter", function () {
  this.timeout(30000);
  const dbPath = path.join(__dirname, "mikro-orm-test.db");
  let orm;

  before(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);

    orm = await MikroORM.init({
      entities: [User],
      dbName: dbPath,
      driver: SqliteDriver,
      driverOptions: {
        // MikroORM uses the 'sqlite3' package by default.
        // We inject our adapter as the driver module.
        driver: adapter,
      },
      debug: false,
    });

    const generator = orm.schema;
    await generator.createSchema();
  });

  after(async () => {
    if (orm) {
      await orm.close();
    }
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  });

  it("should initialize and create schema", async () => {
    const generator = orm.schema;
    const sql = await generator.getCreateSchemaSQL();
    // console.log("MikroORM SQL:", sql);
    expect(sql.toLowerCase()).to.contain("create table");
    expect(sql.toLowerCase()).to.contain("user");
    expect(sql.toLowerCase()).to.contain("id");
  });

  it("should perform basic CRUD operations using EntityManager", async () => {
    const em = orm.em.fork();

    // Create
    const user = em.create("User", { name: "John Doe" });
    await em.persistAndFlush(user);
    expect(user.id).to.exist;

    // Read
    const found = await em.findOne("User", { id: user.id });
    expect(found.name).to.equal("John Doe");

    // Update
    found.name = "Jane Doe";
    await em.flush();
    const updated = await em.findOne(
      "User",
      { id: user.id },
      { refresh: true },
    );
    expect(updated.name).to.equal("Jane Doe");

    // Delete
    await em.removeAndFlush(updated);
    const deleted = await em.findOne("User", { id: user.id });
    expect(deleted).to.be.null;
  });

  it("should handle transactions correctly", async () => {
    const em = orm.em.fork();

    await em
      .transactional(async (txEm) => {
        const user = txEm.create("User", { name: "TxUser" });
        await txEm.persistAndFlush(user);

        // Visibility inside TX
        const inside = await txEm.findOne("User", { name: "TxUser" });
        expect(inside).to.not.be.null;

        // Rollback is implicit if we throw
        throw new Error("Rollback");
      })
      .catch((err) => {
        if (err.message !== "Rollback") throw err;
      });

    const outside = await em.findOne("User", { name: "TxUser" });
    expect(outside).to.be.null;
  });

  it("should handle parallel requests (concurrency)", async () => {
    const em = orm.em.fork();
    const count = 30;
    const items = Array.from({ length: count }, (_, i) =>
      em.create("User", { name: `User ${i}` }),
    );

    await em.persistAndFlush(items);

    // Parallel reads via different forks (simulating different requests)
    const results = await Promise.all(
      Array.from({ length: 10 }, () => orm.em.fork().find("User", {})),
    );

    expect(results[0].length).to.be.at.least(count);
  });
});
