# better-sqlite3-pool

A **non-blocking**, **multi-threaded**, **auto-scaling** SQLite connection pool built on top of `better-sqlite3-multiple-ciphers`.

Designed for high-concurrency Node.js applications (like REST APIs or GraphQL servers) that need the speed of SQLite and the security of **SQLCipher encryption** without blocking the main event loop.

> [!IMPORTANT]
> **Production Ready:** Nearly 100% of the original `better-sqlite3` test suite has been ported and is fully covered to ensure maximum reliability and behavioral parity.

## 🚀 Why this exists?

The standard `better-sqlite3` is the fastest driver available, but it is **synchronous**.

1. **The Blocking Problem:** If a query takes 50ms, your Node.js server cannot handle *any* other requests during that time.
2. **The Encryption Penalty:** Using SQLCipher adds heavy CPU overhead for decryption. Running this on the main thread kills throughput.

**better-sqlite3-pool solves this** by moving all database operations to Worker Threads.

## ✨ Features

- **🚫 Non-Blocking:** All queries return `Promises`. Your event loop stays free.
- **🔒 Encrypted Support:** Native support for **SQLCipher**, **wxSQLite3**, and **AES-256**.
- **⚡ True Concurrency:** Read queries run in parallel across multiple workers/cores.
- **📈 Auto-Scaling:** Spawns more Reader Workers automatically as load increases.
- **💾 WAL-Safe Encryption:** Smart handling of Journal Modes to prevent header corruption on encrypted files.
- **❤️ Transaction Heartbeats:** Auto-rollbacks stalled transactions to prevent "database locked" deadlocks.
- **🔌 SQLite3 Adapter:** Drop-in compatibility mode for legacy libraries via `better-sqlite3-pool/adapter`.
- **🏷️ Better TypeScript Support:** Improved type definitions for both the main pool and the adapter.

---

## 📦 Installation

```bash
npm install better-sqlite3-pool
```

*Note: You do not need to install `better-sqlite3` separately.*

---

## 🆚 Comparison: Original vs. Pool

| Feature | `better-sqlite3` (Original) | `better-sqlite3-pool` (This Library) |
| :--- | :--- | :--- |
| **API Style** | **Synchronous** (Blocking) | **Asynchronous** (Promises/Await) |
| **Concurrency** | 1 Query at a time (Serialized) | **Parallel Reads** (Writer + N Readers) |
| **Main Thread** | Blocked during queries | **Free** to handle HTTP requests |
| **Encryption** | Blocks event loop (High CPU) | **Offloaded** to Worker Threads |
| **Transactions** | Blocks everything | Blocks Writer only (Readers continue) |
| **UDF Functions** | Can use closures | Pure functions only (Serialized) |
| **Best For** | Desktop Apps, CLI, Scripts | **Web Servers, APIs, Electron Main** |

---

## 🛠 Basic Usage

Initialization is asynchronous to ensure workers are ready.

```javascript
const { Database } = require("better-sqlite3-pool");

async function main() {
  // 1. Initialize the pool (Factory Pattern)
  const db = await Database.create("my-database.db", {
    minWorkers: 2, // Always keep 2 readers alive
    maxWorkers: 4, // Scale up to 4 readers under load
  });

  // 2. Writes (Sent to the single Writer thread)
  const res = await db
    .prepare("INSERT INTO users (name) VALUES (?)")
    .run("Alice");

  console.log(`Inserted ID: ${res.lastInsertRowid}`);

  // 3. Reads (Load balanced across Reader threads)
  const user = await db
    .prepare("SELECT * FROM users WHERE id = ?")
    .get(res.lastInsertRowid);

  console.log(user);

  // 4. Shutdown
  await db.close();
}

main();
```

---

## 🔐 Encryption Usage

When using encryption, you must tell the pool the file is encrypted so it handles the WAL (Write-Ahead Log) header correctly.

### Creating a new Encrypted Database file

```javascript
const db = await Database.create("secure.db");

// Should rekey the database to initialize the database with key
// As key can not be set on new database opened on WAL mode
await db.pragma("rekey = 'secret-password'");

// Now you can safely enable WAL mode for performance
await db.pragma("journal_mode = WAL");

await db.exec("CREATE TABLE IF NOT EXISTS confidential (data TEXT)");
```

### Opening an existing Encrypted Database

```javascript
const db = await Database.create("secure.db");

// Broadcast the key to ALL workers (Writer + Readers)
await db.pragma("key = 'secret-password'");

// Now you can safely enable WAL mode for performance
await db.pragma("journal_mode = WAL");

await db.exec("CREATE TABLE IF NOT EXISTS confidential (data TEXT)");
```

### Rekeying (Changing Password)

```javascript
// Change password from 'old' to 'new'
await db.rekey("new-password");
```

---

## ⚡ Transactions

### Managed Transactions (Recommended)

This wrapper automatically acquires a connection, begins the transaction, runs your logic, and commits (or rolls back on error).

```javascript
const insertMany = db.transaction(async (users) => {
  const stmt = db.prepare("INSERT INTO users (name) VALUES (?)");
  for (const user of users) {
    await stmt.run(user);
  }
});

// Takes an exclusive lock on the Writer
await insertMany(["Bob", "Charlie", "Dave"]);
```

### Manual Acquisition (Advanced)

If you need granular control, you can acquire an exclusive session on the Writer.

```javascript
// Locks the writer worker. No other writes can happen until release().
const conn = await db.acquire();

try {
  await conn.exec("BEGIN");
  await conn.prepare("INSERT INTO log VALUES (?)").run("Log 1");

  // Do some heavy calculation...

  await conn.prepare("INSERT INTO log VALUES (?)").run("Log 2");
  await conn.exec("COMMIT");
} catch (err) {
  await conn.exec("ROLLBACK");
} finally {
  // CRITICAL: Must release to unlock the pool
  conn.release();
}
```

**Safety:** If your code crashes or hangs while holding a connection, the `transactionTimeout` (default 30s) will automatically rollback and release the lock.

---

## 🌊 Streaming (Iterators)

For large datasets, use `.iterate()`. This uses a backpressure mechanism to stream rows from the worker without loading them all into RAM.

```javascript
const stmt = db.prepare("SELECT * FROM huge_table");

for await (const row of stmt.iterate()) {
  console.log(row.name);
  // The worker pauses fetching until you ask for the next row
}
```

---

## 🔌 ORM Support (via SQLite3Adapter)

This library includes a robust compatibility layer for ORMs that expect the legacy `node-sqlite3` callback API. This allows you to use `better-sqlite3-pool` as a drop-in replacement in existing projects.

### Supported ORMs

- **TypeORM**: Natively supported via the `"sqlite"` driver.
- **Sequelize**: Supported as a `dialectModule`.
- **MikroORM**: Supported via the `@mikro-orm/sqlite` package.
-   **TypeORM**: Natively supported via the `"sqlite"` driver.
-   **Sequelize**: Supported as a `dialectModule`.
-   **MikroORM**: Supported via the `@mikro-orm/sqlite` package.
-   **Knex.js**: Supported via the `"sqlite3"` client.

### Integration Examples

#### TypeORM

```javascript
const { adapter } = require("better-sqlite3-pool");

const dataSource = new DataSource({
  type: "sqlite",
  database: "test.db",
  driver: adapter, // Pass the adapter object
  // ...
});
```

#### Sequelize

```javascript
const { adapter } = require("better-sqlite3-pool");

const sequelize = new Sequelize({
  dialect: "sqlite",
  storage: "test.db",
  dialectModule: adapter, // Use as dialectModule
});
```

#### MikroORM

```javascript
const { adapter } = require("better-sqlite3-pool");

const db = await MikroORM.init({
  type: 'sqlite',
  dbName: 'test.db',
  driver: adapter.Database, // Pass the Adapter class
  // ...
});
```

#### Knex.js

```javascript
const { adapter } = require("better-sqlite3-pool");

const db = knex({
  client: "sqlite3",
  connection: { filename: "test.db" },
  pool: { min: 2, max: 10 }
});

// Inject our adapter as the driver
db.client.driver = adapter;
```

---

## 🧪 Testing

The library includes extensive integration tests for all supported ORMs. These tests are isolated into independent modules to ensure native compatibility.

```bash
npm run test:orm
```

See the [ORM Tests README](test/orm-tests/README.md) for more technical details on the testing infrastructure.

---

## ⚙️ Configuration Options

```javascript
const db = await Database.create("file.db", {
  // Worker Pool Settings
  minWorkers: 1,        // Minimum readers (Default: 1)
  maxWorkers: 4,        // Maximum readers (Default: 2)

  // Database Settings
  readonly: false,      // Open in read-only mode
  fileMustExist: false, // Throw if file missing
  nativeBinding: null,  // Path to custom .node addon

  // Timeouts
  timeout: 5000,              // SQLite busy timeout
  transactionTimeout: 30000,  // Max time a transaction can stay idle
  connectionMaxLife: 60000,   // Max duration of an acquired connection

  // Logging
  verbose: console.log, // Log executed SQL
});
```

## ⚠️ Limitations & Gotchas

1. **User Defined Functions (UDFs):**
   - Since functions run in a separate Worker thread, they **cannot close over variables** from your main thread. They must be "pure" or self-contained.
   - *Bad:* `let count = 0; db.function('fn', () => count++)` (Count stays 0 in main thread).
   - *Good:* `db.function('add', (a, b) => a + b)`
2. **In-Memory Databases:**
   - `:memory:` databases cannot be shared across threads. If you use one, the pool effectively becomes a single-threaded wrapper around the Writer.
3. **Host Parameters:**
   - Standard `better-sqlite3` allows binding standard JS objects. Since we pass data via `postMessage`, arguments must be **serializable** (no Functions, Promises, or Symbols as parameters).

## 📜 License

MIT
