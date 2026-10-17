'use strict';

// Remember a successful charge so a second delivery can return it.
// The store is process-local; a crash before ack drops both the memory
// and the acknowledgement, and the broker sends the intent again.

async function handleDelivery(delivery, deps) {
  if (delivery.type !== 'charge') {
    await deps.broker.ack(delivery.id);
    return { status: 'ignored' };
  }

  const seen = deps.store.get(delivery.intentId);
  if (seen && seen.payloadHash === hashPayload(delivery.payload)) {
    await deps.broker.ack(delivery.id);
    return seen.outcome;
  }

  const outcome = await deps.provider.charge(delivery.payload);
  deps.store.set(delivery.intentId, {
    payloadHash: hashPayload(delivery.payload),
    outcome,
  });
  await deps.broker.ack(delivery.id);
  return outcome;
}

function hashPayload(payload) {
  return JSON.stringify(payload);
}

module.exports = { handleDelivery };
