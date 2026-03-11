const { Sequelize, DataTypes } = require("sequelize");
const { expect } = require("chai");
const path = require("path");
const fs = require("fs");
const { adapter } = require("better-sqlite3-pool");

describe("Sequelize Integration via SQLite3Adapter", function () {
  this.timeout(30000);
  const dbPath = path.join(__dirname, "sequelize-test.db");
  let sequelize;

  before(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);

    sequelize = new Sequelize({
      dialect: "sqlite",
      storage: dbPath,
      dialectModule: adapter, // Use our adapter as the dialect module
      logging: false,
    });

    await sequelize.authenticate();
  });

  after(async () => {
    if (sequelize) {
      await sequelize.close();
    }
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  });

  it("should define models and sync schema", async () => {
    const User = sequelize.define("User", {
      name: DataTypes.STRING,
    });
    const Post = sequelize.define("Post", {
      title: DataTypes.STRING,
    });
    User.hasMany(Post);
    Post.belongsTo(User);

    await sequelize.sync({ force: true });

    // Check if tables exist by querying sqlite_master
    const [results] = await sequelize.query(
      "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('Users', 'Posts')",
    );
    expect(results).to.have.lengthOf(2);
  });

  it("should perform basic CRUD operations", async () => {
    const User = sequelize.models.User;

    // Create
    const user = await User.create({ name: "John Doe" });
    expect(user.id).to.exist;
    expect(user.name).to.equal("John Doe");

    // Read
    const foundUser = await User.findByPk(user.id);
    expect(foundUser.name).to.equal("John Doe");

    // Update
    foundUser.name = "Jane Doe";
    await foundUser.save();
    const updatedUser = await User.findByPk(user.id);
    expect(updatedUser.name).to.equal("Jane Doe");

    // Delete
    await foundUser.destroy();
    const deletedUser = await User.findByPk(user.id);
    expect(deletedUser).to.be.null;
  });

  it("should handle associations (One-to-Many)", async () => {
    const User = sequelize.models.User;
    const Post = sequelize.models.Post;

    const user = await User.create({ name: "Alice" });
    await Post.create({ title: "Post 1", UserId: user.id });
    await Post.create({ title: "Post 2", UserId: user.id });

    const userWithPosts = await User.findByPk(user.id, {
      include: [Post],
    });

    expect(userWithPosts.Posts).to.have.lengthOf(2);
    expect(userWithPosts.Posts.map((p) => p.title)).to.include("Post 1");
  });

  it("should handle transactions correctly", async () => {
    const User = sequelize.models.User;

    const t = await sequelize.transaction();
    try {
      await User.create({ name: "TxUser" }, { transaction: t });

      // Visibility inside tx
      const userInside = await User.findOne({
        where: { name: "TxUser" },
        transaction: t,
      });
      expect(userInside).to.not.be.null;

      // Isolation outside tx (using fresh connection)
      const sequelize2 = new Sequelize({
        dialect: "sqlite",
        storage: dbPath,
        dialectModule: adapter,
        logging: false,
      });
      await sequelize2.authenticate();
      sequelize2.define("User", { name: DataTypes.STRING });
      const userOutside = await sequelize2.models.User.findOne({
        where: { name: "TxUser" },
      });
      expect(userOutside).to.be.null;
      await sequelize2.close();

      await t.rollback();
    } catch (err) {
      await t.rollback();
      throw err;
    }

    const foundAfterRollback = await User.findOne({
      where: { name: "TxUser" },
    });
    expect(foundAfterRollback).to.be.null;
  });

  it("should allow parallel reads while a transaction is happening (Non-blocking)", async () => {
    const User = sequelize.models.User;
    await User.create({ name: "ExistingUser" });

    const t = await sequelize.transaction();
    try {
      await User.create({ name: "PendingUser" }, { transaction: t });

      // Parallel reader
      const sequelize2 = new Sequelize({
        dialect: "sqlite",
        storage: dbPath,
        dialectModule: adapter,
        logging: false,
      });
      await sequelize2.authenticate();
      sequelize2.define("User", { name: DataTypes.STRING });

      // Should see existing data
      const existing = await sequelize2.models.User.findOne({
        where: { name: "ExistingUser" },
      });
      expect(existing).to.not.be.null;

      // Should NOT see pending data
      const pending = await sequelize2.models.User.findOne({
        where: { name: "PendingUser" },
      });
      expect(pending).to.be.null;

      await sequelize2.close();
      await t.commit();
    } catch (err) {
      await t.rollback();
      throw err;
    }

    const finallyFound = await User.findOne({ where: { name: "PendingUser" } });
    expect(finallyFound).to.not.be.null;
  });
});
