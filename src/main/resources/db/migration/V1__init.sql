-- Inbound webhook events, as received. This is the system of record.
CREATE TABLE webhook_event (
    id              UUID PRIMARY KEY,
    provider        VARCHAR(64)  NOT NULL,
    external_id     VARCHAR(191) NOT NULL,
    event_type      VARCHAR(128) NOT NULL,
    payload         TEXT         NOT NULL,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_webhook_event_provider_external UNIQUE (provider, external_id)
);

-- Transactional outbox. Written in the SAME transaction as webhook_event, so a
-- crash can never leave us "persisted but not published" or the reverse.
CREATE TABLE outbox_message (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    UUID         NOT NULL,
    topic           VARCHAR(191) NOT NULL,
    message_key     VARCHAR(191) NOT NULL,
    payload         TEXT         NOT NULL,
    status          VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
    attempts        INT          NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

-- Partial index: the poller only ever reads PENDING rows, and this table is
-- overwhelmingly PUBLISHED. Indexing every row would be wasted write cost.
CREATE INDEX idx_outbox_pending ON outbox_message (created_at) WHERE status = 'PENDING';

-- Consumer-side dedupe. Providers redeliver and Kafka is at-least-once,
-- so the consumer must be idempotent. The primary key does the enforcing.
CREATE TABLE processed_event (
    event_id        UUID PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
