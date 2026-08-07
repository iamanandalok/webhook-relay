-- The downstream store the consumer fans out to. Kept in the same database for
-- this demo -- a real target would be another service or datastore -- but the
-- shape (a materialized, query-friendly projection separate from the raw event
-- log in webhook_event) is what actually matters.
CREATE TABLE downstream_record (
    event_id        UUID PRIMARY KEY,
    provider        VARCHAR(64)  NOT NULL,
    event_type      VARCHAR(128) NOT NULL,
    payload         TEXT         NOT NULL,
    synced_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
