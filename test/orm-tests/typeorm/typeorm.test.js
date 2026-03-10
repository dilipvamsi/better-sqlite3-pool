require("reflect-metadata");
const { DataSource, EntitySchema } = require("typeorm");
const { expect } = require("chai");
const path = require("path");
const fs = require("fs");
const adapter = require("../../../src/adapter");

// --- Entities using EntitySchema (for ES compatibility without decorators) ---

const UserEntity = new EntitySchema({
  name: "User",
  tableName: "users",
  columns: {
    id: {
      primary: true,
      type: "int",
      generated: true,
    },
    name: {
      type: "varchar",
    },
  },
  relations: {
    posts: {
      target: "Post",
      type: "one-to-many",
      inverseSide: "user",
      cascade: true,
    },
  },
});

const PostEntity = new EntitySchema({
  name: "Post",
  tableName: "posts",
  columns: {
    id: {
      primary: true,
      type: "int",
      generated: true,
    },
    title: {
      type: "varchar",
    },
  },
  relations: {
    user: {
      target: "User",
      type: "many-to-one",
      inverseSide: "posts",
      onDelete: "CASCADE",
    },
  },
});

describe("TypeORM Integration via SQLite3Adapter", function () {
  this.timeout(30000);
  const dbPath = path.join(__dirname, "typeorm-test.db");
  let dataSource;

  before(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);

    dataSource = new DataSource({
      type: "sqlite",
      database: dbPath,
      entities: [UserEntity, PostEntity],
      synchronize: true,
      logging: false,
      driver: adapter, // Pass our adapter as the driver
    });

    await dataSource.initialize();
  });

  after(async () => {
    if (dataSource && dataSource.isInitialized) {
      await dataSource.destroy();
    }
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  });

  it("should initialize and synchronize schema", async () => {
    expect(dataSource.isInitialized).to.be.true;
  });

  it("should perform basic CRUD operations", async () => {
    const userRepository = dataSource.getRepository("User");

    // Create
    const user = { name: "John Doe" };
    const savedUser = await userRepository.save(user);
    expect(savedUser.id).to.be.a("number");

    // Read
    const foundUser = await userRepository.findOneBy({ id: savedUser.id });
    expect(foundUser.name).to.equal("John Doe");

    // Update
    foundUser.name = "Jane Doe";
    await userRepository.save(foundUser);
    const updatedUser = await userRepository.findOneBy({ id: savedUser.id });
    expect(updatedUser.name).to.equal("Jane Doe");

    // Delete
    await userRepository.remove(updatedUser);
    const deletedUser = await userRepository.findOneBy({ id: savedUser.id });
    expect(deletedUser).to.be.null;
  });

  it("should handle relations (One-to-Many)", async () => {
    const userRepository = dataSource.getRepository("User");
    const postRepository = dataSource.getRepository("Post");

    const user = await userRepository.save({ name: "Alice" });

    await postRepository.save([
      { title: "Post 1", user: user },
      { title: "Post 2", user: user },
    ]);

    const userWithPosts = await userRepository.findOne({
      where: { id: user.id },
      relations: ["posts"],
    });

    expect(userWithPosts.posts).to.have.lengthOf(2);
    expect(userWithPosts.posts.map((p) => p.title)).to.include("Post 1");
  });

  it("should handle transactions and visibility within transaction", async () => {
    const queryRunner = dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    // Create a separate reader DataSource to ensure total connection isolation
    const readerDS = new DataSource(dataSource.options);
    await readerDS.initialize();

    try {
      // Create user within transaction
      await queryRunner.manager.save("User", { name: "TxUser" });

      // 1. Visibility inside transaction: should be able to find it
      const foundInside = await queryRunner.manager.findOneBy("User", {
        name: "TxUser",
      });
      expect(foundInside).to.not.be.null;
      expect(foundInside.name).to.equal("TxUser");

      // 2. Isolation: should NOT be able to find it from a separate reader (outside TX)
      const foundOutside = await readerDS
        .getRepository("User")
        .findOneBy({ name: "TxUser" });
      expect(foundOutside).to.be.null;

      await queryRunner.rollbackTransaction();
    } finally {
      await queryRunner.release();
      await readerDS.destroy();
    }
  });

  it("should handle rollback correctly", async () => {
    const queryRunner = dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      await queryRunner.manager.save("User", { name: "RollbackUser" });
      await queryRunner.rollbackTransaction();
    } finally {
      await queryRunner.release();
    }

    const found = await dataSource
      .getRepository("User")
      .findOneBy({ name: "RollbackUser" });
    expect(found).to.be.null;
  });

  it("should allow parallel reads while a transaction is happening (Non-blocking)", async () => {
    // Create an existing user first to check if we can read it
    await dataSource.getRepository("User").save({ name: "ExistingUser" });

    const queryRunner = dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    // Separate reader
    const readerDS = new DataSource(dataSource.options);
    await readerDS.initialize();

    try {
      // Write something in transaction
      await queryRunner.manager
        .getRepository("User")
        .save({ name: "PendingUser" });

      // 1. Check from another reader if we can see the ALREADY committed data
      const existingUser = await readerDS
        .getRepository("User")
        .findOneBy({ name: "ExistingUser" });
      expect(existingUser).to.not.be.null;
      expect(existingUser.name).to.equal("ExistingUser");

      // 2. Check if we can see the pending write (should be null)
      const pendingUser = await readerDS
        .getRepository("User")
        .findOneBy({ name: "PendingUser" });
      expect(pendingUser).to.be.null;

      await queryRunner.commitTransaction();
    } finally {
      await queryRunner.release();
      await readerDS.destroy();
    }

    // Now it should be visible
    const userAfterCommit = await dataSource
      .getRepository("User")
      .findOneBy({ name: "PendingUser" });
    expect(userAfterCommit).to.not.be.null;
  });

  it("should handle heavy concurrency with many parallel requests", async () => {
    const userRepository = dataSource.getRepository("User");
    const count = 50;
    const names = Array.from({ length: count }, (_, i) => `User ${i}`);

    // Parallel Saves
    await Promise.all(names.map((name) => userRepository.save({ name })));

    // Parallel Reads
    const results = await Promise.all(
      names.map((name) => userRepository.findOneBy({ name })),
    );

    expect(results).to.have.lengthOf(count);
  });
});
