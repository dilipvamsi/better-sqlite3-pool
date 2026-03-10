const expect = require("chai").expect;
const path = require("path");
const fs = require("fs");
const knex = require("knex");
const adapter = require("../../../src/adapter");

describe("Knex.js Integration via SQLite3Adapter", function () {
  this.timeout(30000);
  const dbPath = path.join(__dirname, "knex-test.db");
  let db;

  before(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);

    db = knex({
      client: "sqlite3",
      connection: {
        filename: path.resolve(dbPath),
      },
      useNullAsDefault: true,
      pool: { min: 2, max: 10 },
    });

    // We need to inject our adapter into Knex.
    // Knex's SQLite3 client uses 'sqlite3' package.
    // We can override the client's driver.
    db.client.driver = adapter;

    // Create tables
    await db.schema.createTable("users", (table) => {
      table.increments("id");
      table.string("name");
    });
  });

  after(async () => {
    if (db) {
      await db.destroy();
    }
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  });

  it("should perform basic CRUD operations", async () => {
    // Create
    const [id] = await db("users").insert({ name: "John Doe" });
    expect(id).to.be.at.least(1);

    // Read
    const user = await db("users").where({ id }).first();
    expect(user.name).to.equal("John Doe");

    // Update
    await db("users").where({ id }).update({ name: "Jane Doe" });
    const updatedUser = await db("users").where({ id }).first();
    expect(updatedUser.name).to.equal("Jane Doe");

    // Delete
    await db("users").where({ id }).del();
    const deletedUser = await db("users").where({ id }).first();
    expect(deletedUser).to.be.undefined;
  });

  it("should handle transactions correctly", async () => {
    try {
      await db.transaction(async (tx) => {
        await tx("users").insert({ name: "TxUser" });

        const inside = await tx("users").where({ name: "TxUser" }).first();
        expect(inside).to.exist;

        throw new Error("Rollback");
      });
    } catch (err) {
      if (err.message !== "Rollback") throw err;
    }

    const outside = await db("users").where({ name: "TxUser" }).first();
    expect(outside).to.be.undefined;
  });

  it("should allow parallel reads while a transaction is happening (Non-blocking)", async () => {
    // Start a transaction and hold it
    const tx = await db.transaction();
    await tx("users").insert({ name: "PendingUser" });

    // Check from outside
    const outside = await db("users").where({ name: "PendingUser" }).first();
    expect(outside).to.be.undefined; // Should be invisible due to isolation

    // Can we still read existing data?
    await db("users").insert({ name: "ExistingUser" });
    const existing = await db("users").where({ name: "ExistingUser" }).first();
    expect(existing).to.exist;

    await tx.rollback();
  });

  it("should handle heavy concurrency with many parallel requests", async () => {
    const count = 50;
    const inserts = Array.from({ length: count }, (_, i) =>
      db("users").insert({ name: `User ${i}` }),
    );

    await Promise.all(inserts);

    const users = await db("users").select();
    expect(users.length).to.be.at.least(count);
  });
});
