# better-sqlite3-pool

A non-blocking, multi-threaded, auto-scaling SQLite connection pool built on top of `better-sqlite3-multiple-ciphers`.

Designed for high-concurrency Node.js applications that need the speed of SQLite and the security of **SQLCipher encryption** without blocking the main event loop.

## Features

- **Non-Blocking:** Moves CPU-heavy queries (and decryption) to Worker Threads.
- **Encrypted Support:** Full support for SQLCipher, wxSQLite3, and multiple-ciphers.
- **Auto-Scaling:** Spawns more Reader Workers as load increases.
- **True Concurrency:** Reads run in parallel across multiple CPU cores.
- **TypeORM Support:** Drop-in adapter included.
- **BigInt Safety:** Smart casting for large integers (Schema-Aware).
- **Streaming:** Iterators with backpressure for massive datasets.

---

## Installation

Replace the standard driver with the cipher-enabled version:

```bash
npm install better-sqlite3-pool
```

---

## Basic Usage

The API mirrors `better-sqlite3` but is **Promise-based**.

To use an encrypted database, simply broadcast the `key` pragma immediately after initialization.

```javascript
const Database = require("better-sqlite3-pool");

// Initialize the pool
const db = new Database("encrypted-data.db", { min: 2, max: 4 });

async function main() {
  // 0. Unlock the database (Broadcasts to all workers)
  await db.pragma('key = "my-secret-password"');

  // 1. Prepare & Run (Write)
  const result = await db
    .prepare("INSERT INTO users (name) VALUES (?)")
    .run("Alice");
  console.log(result.lastInsertRowid); // BigInt (e.g. 1n)

  // 2. Read (Auto-scaled to available reader)
  const user = await db
    .prepare("SELECT * FROM users WHERE id = ?")
    .get(result.lastInsertRowid);
  console.log(user);

  // 3. Streaming (Low Memory)
  for await (const row of db.prepare("SELECT * FROM large_table").iterate()) {
    console.log(row.name);
  }
}

main();
```

---

## TypeORM Configuration

To use with TypeORM, point the driver to the included adapter.

```typescript
import { DataSource } from "typeorm";
import * as Driver from "better-sqlite3-pool/adapter";

export const AppDataSource = new DataSource({
  type: "sqlite",
  driver: Driver, // Inject our adapter
  database: "secure.sqlite",
  synchronize: true,
  entities: ["src/entity/**/*.ts"],
  // Pass connection hooks to unlock the DB
  pool: {
    afterCreate: async (conn) => {
      await conn.pragma('key = "secret-key"');
    },
  },
});
```

### BigInt Handling in TypeORM

This driver automatically converts "safe" integers (up to `2^53`) to JavaScript Numbers. Huge integers remain as `BigInt`.

```typescript
@Entity()
export class User {
  @PrimaryGeneratedColumn({ type: "integer" })
  id: bigint; // TypeORM maps this correctly

  @Column({ type: "integer" })
  status: number; // Small numbers are automatically cast to Number
}
```

---

## Architecture & Performance

| Feature             | Standard better-sqlite3 | better-sqlite3-pool               |
| :------------------ | :---------------------- | :-------------------------------- |
| **Backend**         | Standard SQLite         | **Multiple Ciphers (Encryption)** |
| **Blocking**        | Yes (Blocks Loop)       | **No (Async)**                    |
| **Decryption Cost** | Blocks Main Thread      | **Offloaded to Workers**          |
| **Concurrency**     | Serialized              | **Parallel Reads**                |
| **BigInt**          | ✅ Native                | **✅ Native (Smart Cast)**         |
| **Best For**        | CLI / Desktop           | **Secure Web Servers / API**      |

### How it works

1.  **Writer:** A single worker thread handles all writes (`INSERT`, `UPDATE`, transactions) sequentially.
2.  **Readers:** A pool of worker threads handles `SELECT` queries.
3.  **Encryption:** Decryption overhead is distributed across the worker threads, keeping the Node.js event loop responsive even with heavy encryption settings (e.g., high iteration counts).

---

## Advanced API

### Pragma Broadcasting

Changes apply to **ALL** current and future workers. This is essential for setting encryption keys or WAL mode.

```javascript
// Enable WAL mode (recommended for performance)
await db.pragma("journal_mode = WAL");

// Rekey the database
await db.pragma('rekey = "new-password"');
```

### User Defined Functions (UDF)

Functions are stringified and broadcast to all workers.

```javascript
await db.function("is_expensive", (price) => (price > 100 ? 1 : 0));
const row = await db
  .prepare("SELECT is_expensive(price) as exp FROM products")
  .get();
```

### Row Modes

```javascript
// Return array: [1, 'Alice']
const rows = await db.prepare("SELECT id, name FROM users").raw().all();

// Return value: 1
const id = await db.prepare("SELECT id FROM users").pluck().get();
```
