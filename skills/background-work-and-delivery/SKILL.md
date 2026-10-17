---
name: background-work-and-delivery
description: Makes queue consumers, webhook handlers, scheduled jobs, and outbox publishers safe under redelivery. Use when a worker, consumer, or cron can run more than once, a broker redelivers, a webhook is retried, an ack happens before the side effect, a poison message retries forever, or a charge, email, notification, or external command must not be applied twice. Use when adding a dead-letter, a delivery claim, or an outbox.
---

# Background Work and Delivery

## Overview

Queues, webhooks, and crons deliver at least once. A process can die after the effect and before the acknowledgement, and the same intent comes back — sometimes while the first attempt is still running. The handler has to make that boring. This skill is the order of operations that keeps a second delivery from charging, emailing, notifying, or commanding twice.

## When to Use

- Implementing or changing a queue consumer, webhook handler, or scheduled job
- A broker, provider, or cron overlap delivered the same work twice
- An acknowledgement, delete, or "mark processed" happens before the side effect
- A poison message retries without a cap, or a dead-letter is replayed into a duplicate
- The code commits a row and publishes a message as two separate steps
- An external command (payment, email, notification, device or partner call) is not safe to repeat

**NOT for:**

- Designing the HTTP `Idempotency-Key` contract (status codes, request hash, key retention). Follow the `api-and-interface-design` skill. This skill places that claim inside a consumer; it does not restate it.
- Deciding why a duplicate already happened in production. Use the `debugging-and-error-recovery` skill, then come back here for the guard.
- Adding logs, metrics, or alerts for the worker. Use the `observability-and-instrumentation` skill after the delivery semantics are fixed.
- Verifying a webhook signature or blocking an untrusted callback URL. Use the `security-and-hardening` skill. Authenticate the delivery first; this skill starts once the delivery is trusted.
- A one-shot local script that is not retried, not scheduled, and not re-run by a broker.

## Process

### 1. Name the effect and the intent id

Write this down before editing the handler:

```
EFFECT:    charge the card for this order
INTENT ID: order id, stable across every retry of this charge
NOT AN ID: delivery id, receive time, attempt number, process start
```

The intent id comes from the producer or the business record. The consumer does not mint a new one when a delivery arrives. If the only identifier changes on every attempt, stop and fix the producer. A handler cannot dedupe what it cannot name.

### 2. State the delivery you actually have

Treat queues, webhooks, and overlapping crons as at-least-once. A broker's "exactly-once" setting does not cover a crash between your effect and your ack, and it does not cover two scheduler replicas starting the same period. Design for two deliveries of one intent, including two that overlap.

### 3. Claim, then effect, then record, then ack

The claim itself — unique insert, payload hash, what an in-flight duplicate gets, how long the record lives — is the `api-and-interface-design` skill. Do not invent a second version. What this skill adds is the position of that claim:

```typescript
// ✗ ack first. A crash on the next line loses the work forever.
await broker.ack(delivery.id);
await provider.charge(payload);

// ✗ effect first, no claim. A crash before ack runs the charge again.
await provider.charge(payload);
await broker.ack(delivery.id);

// ✓ ack only after a committed outcome. Any other state leaves the
//   delivery outstanding so the broker can redeliver.
const claim = await claimIntent(intentId, payloadHash);
if (claim.state === 'succeeded') {
  await broker.ack(delivery.id);
  return claim.outcome;
}
if (claim.state !== 'owned') {
  return; // in-flight or unknown: do not ack, do not call the provider
}
const outcome = await provider.charge(payload);
await recordOutcome(intentId, outcome);
await broker.ack(delivery.id);
```

`claimIntent` is the unique-constraint insert from the API skill, not a read followed by a write. `owned` means this attempt inserted the row. `succeeded` means an earlier attempt already committed an outcome. Anything else — another attempt still running, or a timeout whose result is unknown — returns without acking and without calling the provider. Do not start a second effect because the first "seems stuck". A framework that acks when the function returns is not using this pattern; the ack has to be an explicit call you control.

Same intent id with a different payload is a reject: dead-letter it, leave the first outcome alone. Do not overwrite.

The claim record has to outlive every redelivery path, including a dead-letter replayed days later. A TTL shorter than that window is a duplicate waiting for the replay.

### 4. Publish through an outbox

When a state change and a message must succeed together, write both in one local transaction. Publish afterwards, from the outbox row, as its own consumer.

```typescript
await db.transaction(async (tx) => {
  await tx.insert(orders).values(order);
  await tx.insert(outbox).values({
    intentId: `ship:v1:${order.id}`,
    payload,
    publishedAt: null,
  });
});
```

The publisher claims the row, publishes, then sets `publishedAt` after the broker accepts. Publishing inside the database transaction is wrong (the broker cannot roll back). Publishing after commit with no outbox row is also wrong (a crash in the gap loses the message). The publisher follows steps 3, 5, and 6 like any other consumer.

### 5. Classify the outcome, then bound the retry

A call has three outcomes, not two.

| Outcome | How you know | What to do |
|---|---|---|
| Applied | Provider returned success, and you committed it | Ack. A redelivery returns the stored outcome. |
| Rejected | Validation, unknown account, or any error that will not change on retry | Mark failed. Dead-letter with the reason. Do not return it to the main queue. |
| Unknown | Timeout, or a connection that dropped after the bytes were sent | Leave the claim unknown. Reconcile with the same intent id. Do not mint a new id and try again. |

Retry only the transient class: a reset *before* the request was sent, a 429, a 503. Cap the attempts. Back off exponentially and add jitter so a blip does not restampede the provider. Past the cap, dead-letter. An uncapped retry is how one poison message stalls a partition or burns a provider all night.

Reconcile an unknown by asking the provider about that intent id, or by reading a status the provider already stores. "The call threw, so it did not happen" is not a classification.

### 6. Overlap only across different intents

Consumers run in parallel. Serialize only the key that conflicts.

- Two messages for different entities run together.
- Two deliveries of the same intent share the claim from step 3. The loser does not do the effect.
- A scheduled job's intent id is the period or the entity, never the wall-clock start. A second replica that wakes up during the same period finds the lease held and exits.
- Do not take a global lock "to be safe". That turns a one-intent race into a queue-wide outage.

A lease that expires means the next attempt may take over that same intent id and reconcile. It does not mean the effect failed and a new intent may start.

### 7. Prove it before calling the handler done

Follow the `test-driven-development` skill. These start red, then go green, against the repository's own test command:

1. Two sequential deliveries of the same intent call the provider once. The second returns the stored outcome.
2. A crash after the provider returns and before ack, then a redelivery, still calls the provider once.
3. Two overlapping deliveries call the provider once.
4. A timeout leaves the claim unknown and does not call the provider again under a new id.
5. A permanent failure is dead-lettered and is not retried past the cap.
6. The same intent id with a different payload is rejected, and the first outcome is unchanged.

A happy-path test that sends one delivery is not evidence. Paste the test command output.

When you add signals, use the `observability-and-instrumentation` skill. The questions worth answering are: how many deliveries were duplicates, how many claims are stuck unknown, and how old the oldest dead-letter is.

## Common Rationalizations

| Rationalization | Reality |
|---|---|
| "Ack first so the broker won't deliver it twice" | Ack-first loses the message if the process dies. The duplicate you are afraid of is fixed by the claim, not by throwing the message away. |
| "The broker is exactly-once" | That setting does not span your process. The effect and the ack are two operations. A crash between them redelivers. |
| "The cron is every five minutes, so two runs can't overlap" | A slow run plus a second replica can. The intent id is the period, and the loser exits. |
| "We'll dedupe in memory" | The redelivery arrives in another process. The claim has to be shared, and unique in one operation. |
| "It timed out, so it failed. Retry with a new id" | A timeout after send is unknown. A new id makes the second attempt look like new work. Reconcile the original id. |
| "Keep retrying, it will eventually succeed" | A permanent error will not. Cap it and dead-letter, or one message blocks the partition. |
| "The publish-after-commit window is tiny" | It is exactly as wide as a crash. The outbox row is the record that the message still needs to leave. |
| "A global lock makes ordering safe" | It makes the whole queue single-threaded. Partition or lease by the entity that actually conflicts. |

## Red Flags

- `ack`, `delete`, or "mark processed" above the side effect
- A provider call with no durable claim keyed by a stable intent id
- An intent id built from the delivery id, a timestamp, a random value, or the process start time
- A read of "have we done this?" followed by a write, with nothing unique underneath
- `catch` that retries every failure, including validation errors, with no cap
- A timeout treated as failure and retried under a fresh id
- An outbox-shaped comment with a publish that is not in the same transaction as the state change
- A claim TTL shorter than dead-letter retention
- Tests that cover one successful delivery and none of the six cases in step 7
- A process-local map used as the dedupe store

## Verification

After changing a consumer, scheduler, or publisher:

- [ ] The effect and its intent id are written down, and the id is stable across retries
- [ ] The claim is a unique insert from the `api-and-interface-design` skill, placed before the effect
- [ ] The only ack sits after the outcome commit. Other paths leave the delivery unacked or dead-letter it
- [ ] Unknown (timeout after send) is reconciled on the same intent id and is not retried as new work
- [ ] Transient retries are capped, backed off, and jittered. Permanent failures are dead-lettered
- [ ] Claim retention is longer than the longest redelivery, including dead-letter replay
- [ ] A state change and its outbound message commit in one outbox transaction, or the handler does not publish
- [ ] Overlap is leased per intent, not with a queue-wide lock
- [ ] The six tests in step 7 exist and the repository's test command was run. Output is in the change summary
