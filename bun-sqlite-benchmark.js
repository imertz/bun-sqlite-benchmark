#!/usr/bin/env bun
import { Database } from 'bun:sqlite';
import { cpus } from 'os';
import { performance } from 'perf_hooks';

const DB_FILE = 'benchmark.db';
const NUM_RECORDS = 1000000;
const NUM_WORKERS = cpus().length;

async function runBenchmark() {
  console.log(`Starting Bun SQLite benchmark with ${NUM_WORKERS} workers...`);

  // Initialize database
  const db = new Database(DB_FILE);
  db.exec('DROP TABLE IF EXISTS users');
  db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

  // Benchmark insert
  console.log(`Inserting ${NUM_RECORDS} records...`);
  const insertStart = performance.now();
  await runInsertWorkers(db);
  const insertEnd = performance.now();
  const insertTime = insertEnd - insertStart;
  console.log(`Insert time: ${insertTime.toFixed(2)} ms`);
  console.log(`Inserts per second: ${(NUM_RECORDS / (insertTime / 1000)).toFixed(2)}`);

  // Verify number of inserted records
  const count = db.query('SELECT COUNT(*) as count FROM users').get().count;
  console.log(`Actual number of records inserted: ${count}`);

  // Benchmark select
  console.log(`Selecting ${NUM_RECORDS} records...`);
  const selectStart = performance.now();
  await runSelectWorkers(db);
  const selectEnd = performance.now();
  const selectTime = selectEnd - selectStart;
  console.log(`Select time: ${selectTime.toFixed(2)} ms`);
  console.log(`Selects per second: ${(NUM_RECORDS / (selectTime / 1000)).toFixed(2)}`);

  db.close();
  console.log('Benchmark completed.');
}

async function runInsertWorkers(db) {
  const workers = [];
  const recordsPerWorker = Math.ceil(NUM_RECORDS / NUM_WORKERS);

  const insertStmt = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');

  for (let i = 0; i < NUM_WORKERS; i++) {
    const start = i * recordsPerWorker;
    const end = Math.min((i + 1) * recordsPerWorker, NUM_RECORDS);

    const worker = new Worker(new URL('./worker.js', import.meta.url));
    workers.push(worker);

    worker.postMessage({ operation: 'generate', start, end });
  }

  await Promise.all(workers.map(worker =>
    new Promise((resolve) => {
      worker.onmessage = (event) => {
        const records = event.data;
        db.transaction(() => {
          for (const record of records) {
            insertStmt.run(record.name, record.email);
          }
        })();
        worker.terminate();
        resolve();
      };
    })
  ));
}

async function runSelectWorkers(db) {
  const workers = [];
  const recordsPerWorker = Math.ceil(NUM_RECORDS / NUM_WORKERS);

  for (let i = 0; i < NUM_WORKERS; i++) {
    const start = i * recordsPerWorker;
    const end = Math.min((i + 1) * recordsPerWorker, NUM_RECORDS);

    const worker = new Worker(new URL('./worker.js', import.meta.url));
    workers.push(worker);

    worker.postMessage({ operation: 'select', start, end });
  }

  await Promise.all(workers.map(worker =>
    new Promise((resolve) => {
      worker.onmessage = () => {
        worker.terminate();
        resolve();
      };
    })
  ));
}

runBenchmark();

// Export the runBenchmark function for potential reuse
export { runBenchmark };