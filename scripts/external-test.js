/**
 * @file external-test.js
 * @description Automated script to verify better-sqlite3-pool types and behavior in an external consumer environment.
 */

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');
const os = require('os');

const ROOT = path.resolve(__dirname, '..');
const TEMP_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'better-sqlite3-pool-ext-test-'));

console.log(`🚀 Starting external verification in: ${TEMP_DIR}`);

// 1. Setup minimal package.json
const pkg = {
    name: "external-repro-auto",
    version: "1.0.0",
    description: "Automated test for better-sqlite3-pool",
    private: true,
    dependencies: {
        "better-sqlite3-pool": `file:${ROOT}`,
        "@types/node": "^20.0.0",
        "typescript": "^5.0.0"
    }
};

fs.writeFileSync(path.join(TEMP_DIR, 'package.json'), JSON.stringify(pkg, null, 2));

// 2. Setup tsconfig.json
const tsconfig = {
    compilerOptions: {
        target: "ESNext",
        module: "NodeNext",
        moduleResolution: "NodeNext",
        esModuleInterop: true,
        skipLibCheck: true,
        strict: true,
        declaration: true,
        outDir: "./dist"
    }
};

fs.writeFileSync(path.join(TEMP_DIR, 'tsconfig.json'), JSON.stringify(tsconfig, null, 2));

// 3. Install dependencies
console.log('📦 Installing dependencies...');
const install = spawnSync('npm', ['install'], { cwd: TEMP_DIR, stdio: 'inherit', shell: true });
if (install.status !== 0) {
    console.error('❌ Failed to install dependencies');
    process.exit(1);
}

// 4. Test Scenarios
const tests = [
    {
        name: "Scenario 1: Star Import as Type (The 'import * as Database' issue)",
        file: "test-star.ts",
        content: `
import * as Database from "better-sqlite3-pool";

async function prepareDatabase(db: Database) {
    console.log("✅ Star import as type works. DB Name:", db.name);
}

(async () => {
    const db = await Database.create(":memory:");
    await prepareDatabase(db);
    await db.close();
})();
        `
    },
    {
        name: "Scenario 2: Named Imports",
        file: "test-named.ts",
        content: `
import { Database, adapter, SqliteError } from "better-sqlite3-pool";

async function test() {
    console.log("✅ Named import Database:", !!Database);
    console.log("✅ Named import adapter:", !!adapter);
    console.log("✅ Named import SqliteError:", !!SqliteError);

    const db = await Database.create(":memory:");
    const stmt = db.prepare("SELECT 1 as val");
    const result = await stmt.get();
    console.log("✅ Query executed via named import:", result);
    await db.close();
}

test();
        `
    },
    {
        name: "Scenario 3: CommonJS require",
        file: "test-require.js",
        content: `
const Database = require("better-sqlite3-pool");

async function test() {
    console.log("✅ require('better-sqlite3-pool') works:", !!Database);
    console.log("✅ Static member Statement:", !!Database.Statement);
    console.log("✅ Static member adapter:", !!Database.adapter);

    const db = await Database.create(":memory:");
    console.log("✅ DB created via require:", !!db);
    await db.close();
}

test().catch(err => {
    console.error(err);
    process.exit(1);
});
        `
    }
];

let failed = false;

for (const t of tests) {
    console.log(`\n🔍 Testing: ${t.name}`);
    const filePath = path.join(TEMP_DIR, t.file);
    fs.writeFileSync(filePath, t.content.trim());

    if (t.file.endsWith('.ts')) {
        console.log(`   Compiling ${t.file}...`);
        const compile = spawnSync('npx', ['tsc', t.file, '--noEmit'], { cwd: TEMP_DIR, stdio: 'inherit', shell: true });
        if (compile.status !== 0) {
            console.error(`❌ Compilation failed for ${t.file}`);
            failed = true;
            continue;
        }
    }

    console.log(`   Executing ${t.file}...`);
    // For TS files we use ts-node or just run with node if it's compatible or we compiled it
    // But since we are testing both runtime and types, let's use node on the TS file via ts-node or similar
    // Actually, easier to just run node on .js files or use npx tsx/node --loader if needed.
    // Let's use 'npx tsx' for a smooth experience without manual compilation management
    const run = spawnSync('npx', ['tsx', t.file], { cwd: TEMP_DIR, stdio: 'inherit', shell: true });
    if (run.status !== 0) {
        console.error(`❌ Execution failed for ${t.file}`);
        failed = true;
    }
}

// 5. Cleanup
console.log(`\n🧹 Cleaning up: ${TEMP_DIR}`);
fs.rmSync(TEMP_DIR, { recursive: true, force: true });

if (failed) {
    console.error('\n🔴 EXTERNAL VERIFICATION FAILED');
    process.exit(1);
} else {
    console.log('\n🟢 EXTERNAL VERIFICATION PASSED');
    process.exit(0);
}
