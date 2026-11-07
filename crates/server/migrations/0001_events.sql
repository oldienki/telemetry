-- LightTelemetry events table (SQLite + PostgreSQL compatible column set).
-- `payload` is stored as JSON text; PostgreSQL uses JSONB via its own DDL.

CREATE TABLE IF NOT EXISTS events (
    ingress_ts INTEGER NOT NULL,
    ts         INTEGER NOT NULL,
    topic      TEXT    NOT NULL,
    id         TEXT    NOT NULL,
    class      TEXT    NOT NULL,
    payload    TEXT    NOT NULL DEFAULT 'null'
);

CREATE INDEX IF NOT EXISTS idx_events_ingress_ts ON events (ingress_ts);
CREATE INDEX IF NOT EXISTS idx_events_topic      ON events (topic);
