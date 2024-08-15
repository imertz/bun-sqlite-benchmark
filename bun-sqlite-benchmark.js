import { Database } from 'bun:sqlite';
import { cpus } from 'os';
import { performance } from 'perf_hooks';
import { unlink } from 'fs/promises';

const DB_FILE = 'benchmark.db';
const NUM_RECORDS = 1000000;
const NUM_WORKERS = cpus().length;
const BATCH_SIZE = 100;
const LOG_INTERVAL = 100000;
const MAX_SELECT_TIME = 10000; // 10 seconds in milliseconds

async function runBenchmark(withIndex = false) {
  console.log(`Starting time-limited Bun SQLite benchmark...`);
  console.log(`Running ${withIndex ? 'with' : 'without'} index on email`);

  const db = new Database(DB_FILE);
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec('DROP TABLE IF EXISTS users');
  db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

  if (withIndex) {
    console.log("Creating index on 'email' column...");
    db.exec('CREATE INDEX idx_users_email ON users(email)');
  }

  console.log(`Inserting ${NUM_RECORDS.toLocaleString()} records...`);
  const insertStart = performance.now();
  await runInsertWorkers(db);
  const insertEnd = performance.now();
  const insertTime = insertEnd - insertStart;
  console.log(`Insert time: ${insertTime.toFixed(2)} ms`);
  console.log(`Inserts per second: ${(NUM_RECORDS / (insertTime / 1000).toLocaleString()).toFixed(2)}`);

  const count = db.query('SELECT COUNT(*) as count FROM users').get().count;
  console.log(`Actual number of records inserted: ${count.toLocaleString()}`);

  console.log(`Selecting records by email (max ${MAX_SELECT_TIME / 1000} seconds)...`);
  const { selectTime, processedCount } = await runTimeLimitedSelect(db);
  console.log(`Select time: ${selectTime.toFixed(2)} ms`);
  console.log(`Processed ${processedCount.toLocaleString()} records`);
  if (processedCount > 0) {
    console.log(`Selects per second: ${(processedCount / (selectTime / 1000)).toFixed(2)}`);
  }

  db.close();
  console.log('Benchmark completed.');

  return { insertTime, selectTime, processedCount };
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
          for (let j = 0; j < records.length; j += BATCH_SIZE) {
            const batch = records.slice(j, j + BATCH_SIZE);
            for (const record of batch) {
              insertStmt.run(record.name, record.email);
            }
          }
        })();
        worker.terminate();
        resolve();
      };
    })
  ));
}
async function runTimeLimitedSelect(db) {
  console.log("Starting runTimeLimitedSelect function...");
  const startTime = performance.now();
  let processedCount = 0;
  const workers = [];
  const recordsPerWorker = Math.ceil(NUM_RECORDS / NUM_WORKERS);

  console.log(`Creating ${NUM_WORKERS} workers...`);
  for (let i = 0; i < NUM_WORKERS; i++) {
    const start = i * recordsPerWorker + 1;
    const end = Math.min((i + 1) * recordsPerWorker, NUM_RECORDS);

    const worker = new Worker(new URL('./worker.js', import.meta.url));
    workers.push(worker);

    console.log(`Sending message to worker ${i + 1}...`);
    worker.postMessage({ operation: 'select', start, end, dbFile: DB_FILE, logInterval: LOG_INTERVAL });
  }

  console.log("Waiting for workers to complete...");
  const workerPromises = workers.map((worker, index) =>
    new Promise((resolve) => {
      worker.onmessage = (event) => {
        if (event.data === 'done') {
          console.log(`Worker ${index + 1} completed`);
          worker.terminate();
          resolve();
        } else if (typeof event.data === 'number') {
          processedCount += event.data;
        }
      };
    })
  );

  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => reject(new Error('Select operation timed out')), MAX_SELECT_TIME);
  });

  try {
    await Promise.race([Promise.all(workerPromises), timeoutPromise]);
  } catch (error) {
    console.log('Select operation stopped:', error.message);
  } finally {
    // Terminate any remaining workers
    workers.forEach(worker => worker.terminate());
  }

  const endTime = performance.now();
  const selectTime = endTime - startTime;

  console.log("runTimeLimitedSelect function completed.");
  console.log(`Total records processed: ${processedCount.toLocaleString()}`);
  return { selectTime, processedCount };
}



async function main() {
  console.log("Running time-limited benchmark without index...");
  const withoutIndexResults = await runBenchmark(false);

  console.log("\nRunning time-limited benchmark with index...");
  const withIndexResults = await runBenchmark(true);

  console.log("\nPerformance Comparison:");
  console.log("Without Index:");
  console.log(`  Insert time: ${withoutIndexResults.insertTime.toFixed(2)} ms`);
  console.log(`  Select time: ${withoutIndexResults.selectTime.toFixed(2)} ms`);
  console.log(`  Processed records: ${withoutIndexResults.processedCount.toLocaleString()}`);
  const withoutIndexSelectsPerSecond = withoutIndexResults.processedCount / (withoutIndexResults.selectTime / 1000);
  console.log(`  Selects per second: ${withoutIndexSelectsPerSecond.toFixed(2)}`);

  console.log("With Index:");
  console.log(`  Insert time: ${withIndexResults.insertTime.toFixed(2)} ms`);
  console.log(`  Select time: ${withIndexResults.selectTime.toFixed(2)} ms`);
  console.log(`  Processed records: ${withIndexResults.processedCount.toLocaleString()}`);
  const withIndexSelectsPerSecond = withIndexResults.processedCount / (withIndexResults.selectTime / 1000);
  console.log(`  Selects per second: ${withIndexSelectsPerSecond.toFixed(2)}`);

  const selectSpeedup = ((withIndexSelectsPerSecond - withoutIndexSelectsPerSecond) / withoutIndexSelectsPerSecond) * 100;
  console.log(`\nSelect operation speedup with index: ${selectSpeedup.toFixed(2)}%`);

  // Delete the database file
  try {
    await unlink(DB_FILE);
    await unlink(`${DB_FILE}-wal`);
    await unlink(`${DB_FILE}-shm`);
    console.log(`\nDatabase file ${DB_FILE} has been deleted.`);
  } catch (error) {
    console.error(`Error deleting database file: ${error.message}`);
  }
}

main().catch(error => console.error('Error in main function:', error));