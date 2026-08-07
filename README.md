# webhook-relay

An event-driven webhook ingestion and fan-out service. Accepts webhooks over HTTP, stores them durably, publishes them to Kafka via a transactional outbox, and processes them with an idempotent consumer backed by retry and a dead-letter topic.

Java 21 · Spring Boot 3.5 · Kafka · PostgreSQL · Flyway · Testcontainers

[![CI](https://github.com/iamanandalok/webhook-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/iamanandalok/webhook-relay/actions/workflows/ci.yml)

---

## Why this exists

Webhook ingestion looks trivial until you take delivery guarantees seriously. Providers redeliver on timeout, downstream systems fail halfway, and the naive implementation — accept the request, write the row, publish to Kafka, return 200 — has a hole in it that only shows up under load.

This repository is my working through of that problem. The interesting parts are the failure modes, not the happy path.

## Running it

```bash
docker compose up --build
```

Then:

Every request must carry a valid `X-Webhook-Signature` -- an unregistered provider or a bad signature is a `401`, not a silent pass-through:

```bash
BODY='{"externalId":"evt-1001","eventType":"order.created","payload":{"orderId":"A-1"}}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac 'dev-secret-change-me' | sed 's/^.* //')

curl -X POST http://localhost:8080/api/v1/webhooks/acme \
  -H 'Content-Type: application/json' \
  -H "X-Webhook-Signature: $SIG" \
  -d "$BODY"
```

Returns `202 Accepted` with the assigned event id. Send it twice and the second returns `200 OK` with `duplicate: true` and the same id.

Tests spin up their own Postgres and Kafka via Testcontainers, so `./gradlew check` needs a running Docker daemon and nothing else.

Using [Colima](https://github.com/abiosoft/colima) instead of Docker Desktop: Testcontainers needs two env vars, since Colima's socket isn't at the default path and its bind-mount target inside the VM differs from the host-side path.

```bash
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="/var/run/docker.sock"
```

---

## Design notes

### The dual-write problem

The obvious implementation writes to the database and then publishes to Kafka:

```
save(event);
kafka.send(event);   // <- what if the process dies here?
```

There is no transaction spanning both. If the process dies between them, the event exists but nothing downstream ever hears about it. If you reverse the order, you can announce an event that was never stored. Either way you get silent, permanent inconsistency that surfaces days later as "the data doesn't match."

**Solution: transactional outbox.** The request handler writes the event row and an `outbox_message` row in a single database transaction. Both commit or neither does. A separate scheduled poller drains the outbox to Kafka afterwards.

The trade-off is latency. Publishing is no longer immediate — it waits on the poll interval, 500ms by default. For webhook fan-out that is a fine price for never losing an event. For something latency-critical it would not be, and I'd reach for Debezium and change-data-capture off the WAL instead of polling.

### Why the poller uses `FOR UPDATE SKIP LOCKED`

Run two instances of this service and both pollers wake up and query for `PENDING` rows. Without locking they select the same batch and publish everything twice.

Plain `FOR UPDATE` fixes correctness but not throughput — the second poller blocks until the first commits, so you get no parallelism from the extra instance. `SKIP LOCKED` lets the second poller step over the locked rows and take the next batch, so both make progress.

### At-least-once, deliberately

The poller publishes to Kafka and then marks the row `PUBLISHED` in the same transaction. If the send succeeds but the commit fails, the row stays `PENDING` and gets published again.

That is the intended trade. The alternative — mark published first, then send — turns the same crash into a silently dropped event. Duplicates are recoverable; loss is not. So the system is at-least-once end to end, and the consumer is built to absorb that.

### Consumer idempotency

The consumer claims each event id in `processed_event` before doing any work. That column is the primary key, so a duplicate delivery — whether from the outbox republishing, Kafka's at-least-once semantics, or the provider itself resending — loses the insert and is skipped.

The claim happens inside the same transaction as the processing. If processing throws, the claim rolls back with it and a retry can pick the event up cleanly. Claiming outside the transaction would mean a failed event is marked processed forever.

### Ordering

Messages are keyed by `externalId`, so all events for one entity land on the same partition and preserve their relative order. Events for different entities can be processed in parallel across partitions, which is where the throughput comes from.

This does mean a hot entity is a hot partition. At scale I'd want to know the key distribution before assuming the partition count is enough.

### Retry and the dead-letter topic

Consumer failures retry with exponential backoff — 500ms, then 1.5s, then 4.5s — capped at 60 seconds total, then the record goes to `webhook.events.v1.dlq`.

The backoff is the point. Four immediate retries against a downstream that is down is not a retry policy, it is four failures in twenty milliseconds. Spacing them gives the downstream a chance to recover.

Deserialization failures, `IllegalArgumentException`, and `NonRetryableWebhookException` are marked non-retryable and go straight to the DLQ. `NonRetryableWebhookException` is what `WebhookEventConsumer.handle()` throws for an event type it doesn't recognize or a payload missing required fields -- the fourth attempt sees the same malformed message as the first, so retrying just delays the alert. Anything else (a repository call failing because the database or downstream is briefly unreachable) is left uncaught and retried, because that failure mode is exactly what the backoff exists for.

**Anything in the DLQ needs a human.** Alert on depth greater than zero rather than expecting someone to check.

### Signature verification

`HmacSignatureFilter` sits in front of the controller and verifies `X-Webhook-Signature` -- a hex HMAC-SHA256 over the raw request body -- before anything is deserialized or persisted. Two decisions worth defending:

- **The signature covers the raw bytes, not the parsed object.** Jackson can only bind what it successfully parses, and re-serializing a `WebhookRequest` back to JSON is not guaranteed to reproduce the exact bytes the provider signed (key order, whitespace, number formatting). `CachedBodyHttpServletRequest` buffers the body once so the filter can hash the original bytes and the controller can still bind from them afterward.
- **Fail closed on unknown providers.** A provider with no configured secret is rejected outright, not waved through. The alternative -- skip verification when no secret is configured -- turns "haven't onboarded this provider yet" into "anyone can post as this provider," which is the kind of gap that's invisible until someone finds it.

Secrets are per-provider (`relay.hmac.secrets.<provider>`) and belong in a real secret store in production; the `dev-secret-change-me` default here is for local `curl` testing only.

### Fan-out target

The consumer syncs to `downstream_record`, a projection table deliberately separate from `webhook_event`. `webhook_event` is the raw system of record -- whatever the provider sent, unmodified. `downstream_record` is the shape the fan-out target actually wants, built up as events are processed.

A real deployment would replace the `downstream.save(...)` call with an HTTP call, a message to another system, or a write to an external store -- the interesting part isn't where the data ends up, it's that the classification of failures around that call (see above) is what keeps a flaky downstream from either stalling the whole topic or silently losing events.

### Schema ownership

Flyway owns the schema. Hibernate is set to `ddl-auto: validate`, so the application refuses to start if the entities and the migrations have drifted. `ddl-auto: update` in anything resembling production is how you end up with a schema nobody can reproduce.

The index on `outbox_message` is partial — `WHERE status = 'PENDING'` — because the poller only ever reads pending rows and the table is overwhelmingly published. Indexing every row would cost write throughput for nothing.

---

## What I'd do differently at higher volume

- **Polling becomes the bottleneck.** Move to Debezium reading the Postgres WAL. Removes the poll-interval latency and the repeated query load.
- **The outbox table grows without bound.** Needs a retention job archiving `PUBLISHED` rows older than N days, or a partitioned table with partition drops.
- **`processed_event` has the same problem**, but it can't be trimmed as aggressively — the retention window has to be longer than the maximum realistic redelivery window of any upstream provider.
- **No schema registry.** JSON strings on the topic are fine at this size, but a real deployment wants Avro or Protobuf with a registry so a producer change can't silently break every consumer.
- **Single consumer group.** Fan-out to multiple downstreams means multiple groups and thinking about which of them are allowed to be slow.

## Known gaps

Deliberately left out — this is a demonstration of the delivery-guarantee design, not a production service:

- No rate limiting on the ingest endpoint, and HMAC is the only auth -- there's no per-provider API key or request-level authorization beyond the signature
- The fan-out target is a same-database projection table (`downstream_record`), not a real external system -- see "Fan-out target" above for why the shape matters more than the target for this exercise
- No consumer-lag health indicator, though the metrics to build one are exposed
- Single-broker Kafka in compose; `acks: all` means little with replication factor 1
