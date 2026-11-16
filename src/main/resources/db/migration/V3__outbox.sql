-- Transactional outbox: a transfer writes its event here in the SAME transaction as the
-- money movement, so an event exists if and only if the transfer committed. A separate
-- relay publishes unpublished rows to Kafka and then sets published_at.
CREATE TABLE outbox_events (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Stable id that travels with the event; consumers dedupe on it.
    event_id      UUID        NOT NULL UNIQUE,
    event_type    TEXT        NOT NULL,
    aggregate_id  BIGINT      NOT NULL,   -- transfer id; also the Kafka message key
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

-- The relay only ever looks at unpublished rows; a partial index keeps that lookup
-- small no matter how many published rows pile up.
CREATE INDEX outbox_events_unpublished_idx ON outbox_events (id) WHERE published_at IS NULL;

-- Belongs to the example downstream consumer, not the ledger. A real consumer would keep
-- this in its own database. One row per event it has processed; deliveries > 1 means
-- Kafka delivered it again and the duplicate was ignored.
CREATE TABLE consumer_processed_events (
    event_id      UUID        PRIMARY KEY,
    transfer_id   BIGINT      NOT NULL,
    deliveries    INT         NOT NULL DEFAULT 1,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
