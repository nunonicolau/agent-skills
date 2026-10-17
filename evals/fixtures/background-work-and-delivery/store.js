'use strict';

class MemoryStore {
  constructor() {
    this.rows = new Map();
  }

  get(intentId) {
    return this.rows.get(intentId) || null;
  }

  set(intentId, row) {
    this.rows.set(intentId, row);
  }
}

module.exports = { MemoryStore };
