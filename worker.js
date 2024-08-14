// worker.js

self.onmessage = (event) => {
  const { operation, start, end } = event.data;

  if (operation === 'generate') {
    const records = [];
    for (let i = start; i < end; i++) {
      records.push({ name: `User${i}`, email: `user${i}@example.com` });
    }
    self.postMessage(records);
  } else if (operation === 'select') {
    // Simulate select operation
    for (let i = start + 1; i <= end; i++) {
      // In a real scenario, we might do something with each 'selected' record
    }
    self.postMessage('done');
  }
};