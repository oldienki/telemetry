//! PostgreSQL storage engine: JSONB payload column, `StorageEngine` impl.
//!
//! Live tests are env-gated on `TELEMETRY_TEST_PG_URL`; they skip silently
//! when no database is reachable. `payload_match` filtering stays in the
//! Rust-side `WaitFilter` for parity across all backends; the JSONB GIN
//! index exists for external/analytical consumers.

use crate::event::TelemetryEvent;
use crate::storage::{QueryFilter, StorageEngine};
use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::types::Json;
use sqlx::QueryBuilder;
use sqlx::Row;

const MIGRATION: &str = include_str!("../../migrations/0001_events_pg.sql");

/// `StorageEngine` backed by PostgreSQL.
pub struct PgEngine {
    pool: PgPool,
}

impl PgEngine {
    /// Connect to `db_url` (`postgres://user:pass@host/db`), apply the
    /// schema, and return the engine.
    pub async fn connect(db_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(db_url).await?;
        sqlx::raw_sql(MIGRATION).execute(&pool).await?;
        Ok(Self { pool })
    }

    fn row_to_event(row: &sqlx::postgres::PgRow) -> anyhow::Result<TelemetryEvent> {
        let payload: Json<serde_json::Value> = row.get("payload");
        Ok(TelemetryEvent {
            ingress_ts: row.get::<i64, _>("ingress_ts") as u64,
            ts: row.get::<i64, _>("ts") as u64,
            topic: row.get("topic"),
            id: row.get("id"),
            class: row.get("class"),
            payload: payload.0,
        })
    }
}

#[async_trait]
impl StorageEngine for PgEngine {
    async fn append(&self, event: &TelemetryEvent) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO events (ingress_ts, ts, topic, id, class, payload)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(event.ingress_ts as i64)
        .bind(event.ts as i64)
        .bind(&event.topic)
        .bind(&event.id)
        .bind(&event.class)
        .bind(Json(&event.payload))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn query(&self, filter: &QueryFilter) -> anyhow::Result<Vec<TelemetryEvent>> {
        let mut qb = QueryBuilder::new(
            "SELECT ingress_ts, ts, topic, id, class, payload FROM events WHERE 1=1",
        );
        if let Some(t) = &filter.topic {
            qb.push(" AND topic = ").push_bind(t.clone());
        }
        if let Some(c) = &filter.class {
            qb.push(" AND class = ").push_bind(c.clone());
        }
        if let Some(i) = &filter.id {
            qb.push(" AND id = ").push_bind(i.clone());
        }
        if let Some(s) = filter.since {
            qb.push(" AND ingress_ts >= ").push_bind(s as i64);
        }
        if let Some(u) = filter.until {
            qb.push(" AND ingress_ts <= ").push_bind(u as i64);
        }
        qb.push(" ORDER BY ingress_ts, seq");
        if let Some(l) = filter.limit {
            qb.push(" LIMIT ").push_bind(l as i64);
        }
        let rows = qb.build().fetch_all(&self.pool).await?;
        rows.iter().map(Self::row_to_event).collect()
    }

    async fn cleanup(&self, older_than_ts: u64) -> anyhow::Result<usize> {
        let res = sqlx::query("DELETE FROM events WHERE ingress_ts < $1")
            .bind(older_than_ts as i64)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::conformance;

    /// Live Postgres URL; absent → integration tests skip.
    fn test_url() -> Option<String> {
        std::env::var("TELEMETRY_TEST_PG_URL").ok()
    }

    #[tokio::test]
    async fn passes_conformance_suite() {
        let Some(url) = test_url() else {
            eprintln!("TELEMETRY_TEST_PG_URL not set; skipping pg conformance");
            return;
        };
        let eng = PgEngine::connect(&url).await.unwrap();
        sqlx::query("TRUNCATE events")
            .execute(&eng.pool)
            .await
            .unwrap();
        conformance(&eng).await;
        sqlx::query("TRUNCATE events")
            .execute(&eng.pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unreachable_db_is_error_not_panic() {
        assert!(
            PgEngine::connect("postgres://127.0.0.1:1/definitely-not-there")
                .await
                .is_err()
        );
    }
}
