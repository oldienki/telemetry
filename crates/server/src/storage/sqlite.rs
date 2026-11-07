//! SQLite storage engine: WAL mode, embedded migration, `StorageEngine` impl.

use crate::event::TelemetryEvent;
use crate::storage::{QueryFilter, StorageEngine};
use async_trait::async_trait;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use sqlx::QueryBuilder;
use sqlx::Row;
use std::str::FromStr;

const MIGRATION: &str = include_str!("../../migrations/0001_events.sql");

/// `StorageEngine` backed by a SQLite database file.
pub struct SqliteEngine {
    pool: SqlitePool,
}

impl SqliteEngine {
    /// Connect to `db_url` (`sqlite:path.db`, `sqlite::memory:`), apply WAL
    /// mode and the schema, and return the engine.
    ///
    /// In-memory URLs pin the pool to one connection so all statements share
    /// the same database.
    pub async fn connect(db_url: &str) -> anyhow::Result<Self> {
        let in_memory = db_url.contains(":memory:") || db_url.contains("mode=memory");
        let opts = SqliteConnectOptions::from_str(db_url)?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);
        let builder = SqlitePoolOptions::new().max_connections(if in_memory { 1 } else { 8 });
        let pool = builder.connect_with(opts).await?;
        sqlx::raw_sql(MIGRATION).execute(&pool).await?;
        Ok(Self { pool })
    }

    fn row_to_event(row: &sqlx::sqlite::SqliteRow) -> anyhow::Result<TelemetryEvent> {
        let payload_text: String = row.get("payload");
        Ok(TelemetryEvent {
            ingress_ts: row.get::<i64, _>("ingress_ts") as u64,
            ts: row.get::<i64, _>("ts") as u64,
            topic: row.get("topic"),
            id: row.get("id"),
            class: row.get("class"),
            payload: serde_json::from_str(&payload_text).unwrap_or(Value::Null),
        })
    }
}

#[async_trait]
impl StorageEngine for SqliteEngine {
    async fn append(&self, event: &TelemetryEvent) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO events (ingress_ts, ts, topic, id, class, payload)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(event.ingress_ts as i64)
        .bind(event.ts as i64)
        .bind(&event.topic)
        .bind(&event.id)
        .bind(&event.class)
        .bind(event.payload.to_string())
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
        qb.push(" ORDER BY ingress_ts, rowid");
        if let Some(l) = filter.limit {
            qb.push(" LIMIT ").push_bind(l as i64);
        }
        let rows = qb.build().fetch_all(&self.pool).await?;
        rows.iter().map(Self::row_to_event).collect()
    }

    async fn cleanup(&self, older_than_ts: u64) -> anyhow::Result<usize> {
        let res = sqlx::query("DELETE FROM events WHERE ingress_ts < ?")
            .bind(older_than_ts as i64)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::{conformance, ev};
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn passes_conformance_suite() {
        let eng = SqliteEngine::connect("sqlite::memory:").await.unwrap();
        conformance(&eng).await;
    }

    #[tokio::test]
    async fn empty_db_query_returns_empty() {
        let eng = SqliteEngine::connect("sqlite::memory:").await.unwrap();
        let hits = eng.query(&QueryFilter::default()).await.unwrap();
        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn nested_payload_round_trips() {
        let eng = SqliteEngine::connect("sqlite::memory:").await.unwrap();
        let mut e = ev("t", 7);
        e.payload = json!({"deep": {"list": [1, "two", {"three": true}]}, "n": 1.5});
        eng.append(&e).await.unwrap();
        let hits = eng.query(&QueryFilter::default()).await.unwrap();
        assert_eq!(hits[0].payload, e.payload);
    }

    #[tokio::test]
    async fn invalid_url_is_error_not_panic() {
        assert!(
            SqliteEngine::connect("sqlite:/nonexistent-dir-xyz-12345/db.sqlite")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn dyn_dispatch_works() {
        let eng: Arc<dyn StorageEngine> =
            Arc::new(SqliteEngine::connect("sqlite::memory:").await.unwrap());
        eng.append(&ev("t", 1)).await.unwrap();
        assert_eq!(eng.query(&QueryFilter::default()).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cleanup_boundary_is_strict() {
        let eng = SqliteEngine::connect("sqlite::memory:").await.unwrap();
        eng.append(&ev("t", 5)).await.unwrap();
        eng.append(&ev("t", 10)).await.unwrap();
        assert_eq!(eng.cleanup(5).await.unwrap(), 0);
        assert_eq!(eng.cleanup(10).await.unwrap(), 1);
    }
}
