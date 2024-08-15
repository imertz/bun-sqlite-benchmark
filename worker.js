// worker.js
import { Database } from 'bun:sqlite';

self.onmessage = (event) => {
  const { operation, start, end, dbFile, logInterval } = event.data;

  if (operation === 'generate') {
    const records = [];
    for (let i = start; i < end; i++) {
      records.push({ name: `User${i}`, email: `user${i}@example.com` });
    }
    self.postMessage(records);
  } else if (operation === 'select') {
    console.log(`Worker starting select operation from ${start} to ${end}`);
    const db = new Database(dbFile);
    const stmt = db.prepare('SELECT name, email FROM users WHERE email = ?');

    let processedCount = 0;
    for (let id = start; id <= end; id++) {
      const email = `user${id}@example.com`;
      stmt.get(email);
      processedCount++;

      if (processedCount % 10 === 0) {  // Report progress more frequently
        self.postMessage(processedCount);
        processedCount = 0;
      }
    }

    if (processedCount > 0) {
      self.postMessage(processedCount);
    }

    db.close();
    console.log(`Worker completed select operation from ${start} to ${end}`);
    self.postMessage('done');
  }
};