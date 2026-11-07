-- LightTelemetry events table (PostgreSQL).
-- `seq` gives a monotonic order; `payload` is JSONB for containment queries.

CREATE TABLE IF NOT EXISTS events (
    seq        BIGSERIAL PRIMARY KEY,
    ingress_ts BIGINT      NOT NULL,
    ts         BIGINT      NOT NULL,
    topic      TEXT        NOT NULL,
    id         TEXT        NOT NULL,
    class      TEXT        NOT NULL,
    payload    JSONB       NOT NULL DEFAULT 'null'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_events_ingress_ts ON events (ingress_ts);
CREATE INDEX IF NOT EXISTS idx_events_topic      ON events (topic);
CREATE INDEX IF NOT EXISTS idx_events_payload    ON events USING GIN (payload);
