'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { handleDelivery } = require('./worker');
const { MemoryStore } = require('./store');

function harness(overrides = {}) {
  const charges = [];
  const acks = [];
  const store = overrides.store || new MemoryStore();
  const deps = {
    broker: {
      ack: async (id) => {
        acks.push(id);
      },
    },
    store,
    provider: {
      charge: async (payload) => {
        charges.push(payload);
        return { id: 'ch_1', amount: payload.amount };
      },
    },
  };
  return { charges, acks, store, deps };
}

test('charges once on a single delivery', async () => {
  const { charges, acks, deps } = harness();
  const outcome = await handleDelivery(
    { id: 'd1', intentId: 'order-9', type: 'charge', payload: { amount: 500 } },
    deps,
  );

  assert.deepEqual(outcome, { id: 'ch_1', amount: 500 });
  assert.deepEqual(charges, [{ amount: 500 }]);
  assert.deepEqual(acks, ['d1']);
});
