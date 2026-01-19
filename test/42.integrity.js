"use strict";
const Database = require("../src");
const { SqliteError } = require("better-sqlite3-multiple-ciphers");

describe("integrity checks", function () {
  // Increase timeout for async worker operations
  this.timeout(10000);

  beforeEach(async function () {
    this.db = await Database.create(util.next());

    // Setup schema
    await this.db
      .prepare(
        "CREATE TABLE entries (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)",
      )
      .run();

    // Insert data
    await this.db
      .prepare(
        "INSERT INTO entries WITH RECURSIVE temp(a, b, c, d, e) AS (SELECT 'foo', 1, 3.14, x'dddddddd', NULL UNION ALL SELECT a, b + 1, c, d, e FROM temp LIMIT 5) SELECT * FROM temp",
      )
      .run();

    // Register simple passthrough function (serialized to worker)
    await this.db.function("func", (x) => x);

    this.iterator = this.db.prepare(
      "SELECT func(b) from entries ORDER BY rowid",
    );
    this.reader = this.db.prepare("SELECT func(b) from entries ORDER BY rowid");
    this.writer = this.db.prepare("UPDATE entries SET c = c + 2.718");
  });

  afterEach(async function () {
    if (this.db.open) {
      await this.db.close();
    }
  });

  /**
   * Helper to verify an operation succeeds.
   * If fn() throws or returns a rejected promise, the test will fail automatically.
   */
  const allowed = (fn) => async () => {
    const result = fn();
    if (result instanceof Promise) {
      await result;
    }
  };

  /**
   * Robust helper to verify success/failure for both Sync and Async operations.
   * Replaces chai-as-promised logic.
   */
  const blocked =
    (fn, errorType = Error) =>
    async () => {
      try {
        const result = fn();
        // If it's asynchronous, await it to catch the rejection
        if (result instanceof Promise) {
          await result;
        }
      } catch (err) {
        // Expected failure path
        expect(err).to.be.instanceof(errorType);
        return;
      }
      // If we reach here, no error was thrown
      throw new Error(
        `Expected function to throw ${errorType.name || errorType}, but it succeeded.`,
      );
    };

  const normally = async (fn) => await fn();

  // Iterate fully through a statement, running the test function in the middle
  const whileIterating = async (self, fn) => {
    let count = 0;
    const iter = self.iterator.iterate();

    // Start iteration
    const first = await iter.next();
    if (!first.done) count++;

    // Run the test function concurrently while iterator is open
    await fn();

    // Finish iteration
    for await (const _ of iter) {
      count += 1;
    }
    expect(count).to.equal(5);
  };

  const whileClosed = async (self, fn) => {
    await self.db.close();
    await fn();
  };

  describe("Database#prepare()", function () {
    // Prepare is synchronous and local, should work unless closed
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.db.prepare("SELECT 555")),
      );
      await whileIterating(
        this,
        allowed(() => this.db.prepare("DELETE FROM entries")),
      );
      await normally(allowed(() => this.db.prepare("SELECT 555")));
    });

    specify("while closed (blocked)", async function () {
      await whileClosed(
        this,
        blocked(() => this.db.prepare("SELECT 555"), TypeError),
      );
    });
  });

  describe("Database#exec()", function () {
    // Pool allows concurrent exec
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.db.exec("SELECT 555")),
      );
      await normally(allowed(() => this.db.exec("SELECT 555")));
    });

    specify("while closed (blocked)", async function () {
      await whileClosed(
        this,
        blocked(() => this.db.exec("SELECT 555"), TypeError),
      );
    });
  });

  describe("Database#pragma()", function () {
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.db.pragma("cache_size")),
      );
      await normally(allowed(() => this.db.pragma("cache_size")));
    });

    specify("while closed (blocked)", async function () {
      await whileClosed(
        this,
        blocked(() => this.db.pragma("cache_size"), TypeError),
      );
    });
  });

  describe("Database#backup()", function () {
    specify("while iterating (allowed)", async function () {
      const promises = [];
      await whileIterating(
        this,
        allowed(() => promises.push(this.db.backup(util.next()))),
      );
      await Promise.all(promises);
    });

    specify("while closed (blocked)", async function () {
      await whileClosed(
        this,
        blocked(() => this.db.backup(util.next()), TypeError),
      );
    });
  });

  describe("Database#function()", function () {
    specify("while iterating (allowed)", async function () {
      let i = 0;
      await whileIterating(
        this,
        allowed(() => this.db.function(`fn_${++i}`, () => {})),
      );
      await normally(allowed(() => this.db.function(`fn_${++i}`, () => {})));
    });

    specify("while closed (blocked)", async function () {
      let i = 0;
      await whileClosed(
        this,
        blocked(() => this.db.function(`fn_${++i}`, () => {}), TypeError),
      );
    });
  });

  describe("Database#aggregate()", function () {
    specify("while iterating (allowed)", async function () {
      let i = 0;
      await whileIterating(
        this,
        allowed(() => this.db.aggregate(`agg_${++i}`, { step: () => {} })),
      );
      await normally(
        allowed(() => this.db.aggregate(`agg_${++i}`, { step: () => {} })),
      );
    });

    specify("while closed (blocked)", async function () {
      let i = 0;
      await whileClosed(
        this,
        blocked(
          () => this.db.aggregate(`agg_${++i}`, { step: () => {} }),
          TypeError,
        ),
      );
    });
  });

  describe("Database#table()", function () {
    // specify("while iterating (allowed)", async function () {
    //   let i = 0;
    //   await whileIterating(
    //     this,
    //     allowed(() =>
    //       this.db.table(`tbl_${++i}`, { columns: ["x"], *rows() {} }),
    //     ),
    //   );
    //   await normally(
    //     allowed(() =>
    //       this.db.table(`tbl_${++i}`, { columns: ["x"], *rows() {} }),
    //     ),
    //   );
    // });

    specify("while closed (blocked)", async function () {
      let i = 0;
      await whileClosed(
        this,
        blocked(
          () => this.db.table(`tbl_${++i}`, { columns: ["x"], *rows() {} }),
          TypeError,
        ),
      );
    });
  });

  describe("Database#close()", function () {
    // Closing while iterating should wait for gracefully close workers
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.db.close()),
      );
      await normally(allowed(() => this.db.close()));
    });

    specify("while closed (allowed)", async function () {
      await whileClosed(
        this,
        allowed(() => this.db.close()),
      );
    });
  });

  describe("Database#defaultSafeIntegers()", function () {
    specify("while iterating (allowed)", async function () {
      let bool = true;
      await whileIterating(
        this,
        allowed(() => this.db.defaultSafeIntegers((bool = !bool))),
      );
    });

    specify("while closed (blocked)", async function () {
      let bool = true;
      await whileClosed(
        this,
        blocked(() => this.db.defaultSafeIntegers((bool = !bool)), TypeError),
      );
    });
  });

  describe("Database#open", function () {
    specify("while iterating (allowed)", async function () {
      await whileIterating(this, () => expect(this.db.open).to.be.true);
    });

    specify("while closed (allowed)", async function () {
      await whileClosed(this, () => expect(this.db.open).to.be.false);
    });
  });

  describe("Database#inTransaction", function () {
    // Note: Global inTransaction check depends on writer state.
    // We test with manual acquire in pool normally, but exec('BEGIN') works too (auto-routed to writer).
    specify("while iterating (allowed)", async function () {
      // Acquire a connection to effectively be "In Transaction" on the writer
      const conn = await this.db.acquire();
      await conn.exec("BEGIN");

      try {
        await whileIterating(
          this,
          () => expect(this.db.inTransaction).to.be.true,
        );
      } finally {
        await conn.exec("ROLLBACK");
        conn.release();
      }
    });

    specify("while closed (allowed)", async function () {
      // When closed, inTransaction defaults to false
      await whileClosed(this, () => expect(this.db.inTransaction).to.be.false);
    });
  });

  describe("Statement#run()", function () {
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.writer.run()),
      );
      await normally(allowed(() => this.writer.run()));
    });

    specify("while closed (blocked)", async function () {
      await whileClosed(
        this,
        blocked(() => this.writer.run(), TypeError),
      );
    });
  });

  describe("Statement#get()", function () {
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.reader.get()),
      );
      await normally(allowed(() => this.reader.get()));
    });

    specify("while closed (blocked)", async function () {
      await whileClosed(
        this,
        blocked(() => this.reader.get(), TypeError),
      );
    });
  });

  describe("Statement#all()", function () {
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.reader.all()),
      );
      await normally(allowed(() => this.reader.all()));
    });

    specify("while closed (blocked)", async function () {
      await whileClosed(
        this,
        blocked(() => this.reader.all(), TypeError),
      );
    });
  });

  describe("Statement#iterate()", function () {
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.reader.iterate()),
      );
      await normally(allowed(() => this.reader.iterate()));
    });

    specify("while closed (blocked)", async function () {
      await whileClosed(
        this,
        blocked(() => this.reader.iterate(), TypeError),
      );
    });
  });

  describe("Statement#bind()", function () {
    const bind = (stmt) => {
      // Only bind if not already bound (Statement throws if rebound)
      if (!stmt._bound) {
        stmt.bind();
      }
    };

    // Bind is local configuration, mostly allowed unless already executed/bound
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => bind(this.reader)),
      );
    });

    specify("while closed (blocked)", async function () {
      // Bind checks _ensureOpen
      await whileClosed(
        this,
        blocked(() => bind(this.reader), TypeError),
      );
    });
  });

  describe("Statement#pluck()", function () {
    // Pluck is synchronous configuration
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.reader.pluck()),
      );
    });

    specify("while closed (allowed)", async function () {
      // pluck() does NOT call ensureOpen
      await whileClosed(
        this,
        allowed(() => this.reader.pluck()),
      );
    });
  });

  describe("Statement#expand()", function () {
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.reader.expand()),
      );
    });

    specify("while closed (allowed)", async function () {
      await whileClosed(
        this,
        allowed(() => this.reader.expand()),
      );
    });
  });

  describe("Statement#raw()", function () {
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.reader.raw()),
      );
    });

    specify("while closed (allowed)", async function () {
      await whileClosed(
        this,
        allowed(() => this.reader.raw()),
      );
    });
  });

  describe("Statement#safeIntegers()", function () {
    specify("while iterating (allowed)", async function () {
      await whileIterating(
        this,
        allowed(() => this.reader.safeIntegers()),
      );
    });

    specify("while closed (allowed)", async function () {
      await whileClosed(
        this,
        allowed(() => this.reader.safeIntegers()),
      );
    });
  });

  describe("Statement#columns()", function () {
    specify("while iterating (allowed after exec)", async function () {
      await this.reader.all(); // Ensure execution
      await whileIterating(
        this,
        allowed(() => this.reader.columns()),
      );
    });

    specify("before execution (blocked)", async function () {
      // New pool specific behavior: columns() requires prior execution
      try {
        this.writer.columns();
        throw new Error("Should have thrown");
      } catch (err) {
        expect(err).to.be.instanceof(SqliteError);
      }
    });
  });
});
