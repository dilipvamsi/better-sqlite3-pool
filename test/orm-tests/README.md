# ORM Integration Tests

This directory contains integration tests for various Node.js ORMs using the `better-sqlite3-pool` adapter. These tests ensure the adapter correctly implements the expected driver APIs for each ORM.

## Background

The `SQLite3Adapter` in `src/adapter.js` mimics the `sqlite3` driver API. This allows it to be used as a drop-in `dialectModule` or custom driver for popular ORMs, enabling them to benefit from the performance and connection pooling of `better-sqlite3` in a worker pool.

## Project Structure

The ORM tests are organized into independent modules to isolate dependencies and keep the root project lean:

```text
test/orm-tests/
├── knex/            # Knex.js integration
├── mikro-orm/       # MikroORM integration
├── sequelize/       # Sequelize integration
└── typeorm/         # TypeORM integration
```

Each subdirectory contains its own `package.json` and local `node_modules`.

## Why Independent Modules?

1. **Dependency Isolation**: ORMs often have large dependency trees. Keeping them separate avoids version conflicts and a bloated root `package.json`.
2. **Native Verification**: Tests confirm that each ORM works with our `SQLite3Adapter` using its standard `sqlite3` driver/dialect settings.
3. **Adapter vs. Direct Pool**:
   - **Adapter (`adapter.Database`)**: Mimics the **callback-based** `sqlite3` API. Required for legacy-style ORMs like Sequelize and TypeORM.
   - **Pool (`Database`)**: Provides a **Promise-based** API. Modern synchronous drivers (like some better-sqlite3 drivers) are NOT compatible with the async pool directly.

**Conclusion**: Use the **Adapter** for ORMs that support the standard `sqlite3` driver interface.

## Configuration Examples

### TypeORM

```javascript
const adapter = require('better-sqlite3-pool/adapter');
const dataSource = new DataSource({
  type: "sqlite",
  database: "path/to/db",
  driver: adapter, // Pass adapter as the driver
  entities: [...],
  synchronize: true,
});
```

### Sequelize

```javascript
const adapter = require('better-sqlite3-pool/adapter');
const sequelize = new Sequelize({
  dialect: "sqlite",
  storage: "path/to/db",
  dialectModule: adapter, // Pass adapter as dialectModule
});
```

### MikroORM

```javascript
const adapter = require('better-sqlite3-pool/adapter');
const db = await MikroORM.init({
  type: 'sqlite',
  dbName: 'path/to/db',
  driver: adapter.Database, // Pass the Adapter class
  entities: [...],
});
```

## How to Run

### Run All ORM Tests

From the project root:

```bash
npm run test:orm
```

### Run Individual ORM Suites

```bash
npm test --prefix test/orm-tests/typeorm
npm test --prefix test/orm-tests/sequelize
npm test --prefix test/orm-tests/mikro-orm
npm test --prefix test/orm-tests/knex
```

## Current Status

All ORM tests are **PASSING**.

The adapter has been specifically patched to handle:

- **Sequelize Positional Parameters**: Correctly mapping `$1`, `$2` etc. to the format expected by `better-sqlite3`.
- **TypeORM Verbose Mode**: Supporting the `.verbose()` call on the driver module.
- **Concurrency**: Ensuring non-blocking read-heavy workloads during active write transactions.
