//! `SQLite` storage port — write-through persistence with in-memory live fanout.

use std::pin::Pin;
use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use futures::stream::Stream;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use tokio::sync::broadcast;
use uuid::Uuid;

use photon_backend::models::Event;
use photon_backend::{
    open_stored_event, seal_event_for_storage, topic_filter_matches, PhotonError, Result,
    StorageCapabilities, StoragePort, TransportCrypto,
};

use crate::config::sqlite_path_from_env;

fn partition_key(topic_name: &str, topic_key: Option<&str>) -> String {
    format!("{}:{}", topic_name, topic_key.unwrap_or("__null__"))
}

fn checkpoint_key(sub: &str, topic: &str, topic_key: Option<&str>) -> String {
    format!("{sub}:{}:{}", topic, topic_key.unwrap_or("__null__"))
}

fn map_sqlx(err: sqlx::Error) -> PhotonError {
    PhotonError::persistence("sqlite", err)
}

/// Embedded `SQLite` storage for the `sqlite` adapter tier.
///
/// Write-through persistence with in-memory live fanout. Enable the `sqlite` feature on the
/// `photon` public crate and install via
/// [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port).
///
/// Path / env: see module `config` (`PHOTON_SQLITE_PATH`).
///
/// Getting started: [Embedded durable](https://docs.rs/uf-photon/latest/photon/#embedded-one-binary).
/// Runnable: `cargo run -p uf-photon --example embedded_sqlite --features runtime,sqlite`.
///
/// # Examples
///
/// ## Embedded host (publish + handlers)
///
/// One binary owns both publish and `#[subscribe]` dispatch via `start_executor`.
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// use photon_backend_sqlite::SqliteStoragePort;
/// use photon_core::JsonIdentityFactory;
/// use photon_runtime::Photon;
///
/// # async fn boot() -> photon_backend::Result<()> {
/// let port = Arc::new(SqliteStoragePort::open("/var/lib/photon/events.db").await?);
/// let photon = Photon::builder()
///     .storage_port(port)
///     .auto_registry()
///     .build()?;
/// photon.start_executor(Arc::new(JsonIdentityFactory))?;
/// // EventType { … }.publish_on(&photon).await?;
/// # let _ = photon;
/// # Ok(())
/// # }
/// ```
pub struct SqliteStoragePort {
    pool: SqlitePool,
    crypto: TransportCrypto,
    tx: broadcast::Sender<Event>,
    events: Arc<DashMap<String, Event>>,
    delivery_pins: Arc<DashMap<String, i64>>,
}

impl SqliteStoragePort {
    /// Open or create a database at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or migrated.
    pub async fn open(path: &str) -> Result<Self> {
        if path.trim().is_empty() {
            return Err(PhotonError::Internal(
                "SQLite database path must not be empty".into(),
            ));
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(map_sqlx)?;
        Self::with_pool(pool).await
    }

    /// Open using [`sqlite_path_from_env`].
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or migrated.
    pub async fn from_env() -> Result<Self> {
        Self::open(&sqlite_path_from_env()).await
    }

    async fn with_pool(pool: SqlitePool) -> Result<Self> {
        Self::migrate(&pool).await?;
        let (tx, _) = broadcast::channel(1024);
        Ok(Self {
            pool,
            crypto: TransportCrypto::from_env()?,
            tx,
            events: Arc::new(DashMap::new()),
            delivery_pins: Arc::new(DashMap::new()),
        })
    }

    async fn migrate(pool: &SqlitePool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                topic_name TEXT NOT NULL,
                topic_key TEXT,
                seq INTEGER NOT NULL,
                actor_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
        )
        .execute(pool)
        .await
        .map_err(map_sqlx)?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_events_topic_seq
             ON events(topic_name, topic_key, seq)",
        )
        .execute(pool)
        .await
        .map_err(map_sqlx)?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS checkpoints (
                checkpoint_key TEXT PRIMARY KEY,
                last_seq INTEGER NOT NULL
            )",
        )
        .execute(pool)
        .await
        .map_err(map_sqlx)?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS seq_counters (
                partition_key TEXT PRIMARY KEY,
                next_seq INTEGER NOT NULL
            )",
        )
        .execute(pool)
        .await
        .map_err(map_sqlx)?;

        Ok(())
    }

    async fn next_seq(&self, topic_name: &str, topic_key: Option<&str>) -> Result<i64> {
        let pk = partition_key(topic_name, topic_key);
        let row = sqlx::query(
            "INSERT INTO seq_counters (partition_key, next_seq) VALUES (?, 1)
             ON CONFLICT(partition_key) DO UPDATE SET next_seq = seq_counters.next_seq + 1
             RETURNING next_seq",
        )
        .bind(&pk)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(row.get::<i64, _>(0))
    }

    async fn load_replay_events(
        pool: &SqlitePool,
        crypto: &TransportCrypto,
        topic_name: &str,
        topic_key_filter: Option<&str>,
        after_seq: i64,
    ) -> Result<Vec<Event>> {
        let rows = if let Some(key) = topic_key_filter {
            sqlx::query(
                "SELECT event_id, topic_name, topic_key, seq, actor_json, payload_json, created_at
                 FROM events
                 WHERE topic_name = ? AND topic_key = ? AND seq > ?
                 ORDER BY seq ASC",
            )
            .bind(topic_name)
            .bind(key)
            .bind(after_seq)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query(
                "SELECT event_id, topic_name, topic_key, seq, actor_json, payload_json, created_at
                 FROM events
                 WHERE topic_name = ? AND seq > ?
                 ORDER BY seq ASC",
            )
            .bind(topic_name)
            .bind(after_seq)
            .fetch_all(pool)
            .await
        }
        .map_err(map_sqlx)?;

        rows.iter()
            .map(row_to_event)
            .map(|event| event.and_then(|event| open_stored_event(crypto, event)))
            .collect()
    }

    async fn fetch_event_by_id(&self, event_id: &str) -> Result<Option<Event>> {
        let row = sqlx::query(
            "SELECT event_id, topic_name, topic_key, seq, actor_json, payload_json, created_at
             FROM events WHERE event_id = ?",
        )
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.as_ref()
            .map(row_to_event)
            .transpose()?
            .map(|event| open_stored_event(&self.crypto, event))
            .transpose()
    }

    async fn fetch_list_by_topic(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<Event>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let after = after_seq.unwrap_or(0);
        let limit_i = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = if let Some(key) = topic_key {
            sqlx::query(
                "SELECT event_id, topic_name, topic_key, seq, actor_json, payload_json, created_at
                 FROM events
                 WHERE topic_name = ? AND topic_key = ? AND seq > ?
                 ORDER BY seq ASC
                 LIMIT ?",
            )
            .bind(topic_name)
            .bind(key)
            .bind(after)
            .bind(limit_i)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT event_id, topic_name, topic_key, seq, actor_json, payload_json, created_at
                 FROM events
                 WHERE topic_name = ? AND seq > ?
                 ORDER BY seq ASC
                 LIMIT ?",
            )
            .bind(topic_name)
            .bind(after)
            .bind(limit_i)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(map_sqlx)?;

        rows.iter()
            .map(row_to_event)
            .map(|event| event.and_then(|event| open_stored_event(&self.crypto, event)))
            .collect()
    }

    async fn fetch_list_recent(&self, limit: usize) -> Result<Vec<Event>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit_i = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = sqlx::query(
            "SELECT event_id, topic_name, topic_key, seq, actor_json, payload_json, created_at
             FROM events
             ORDER BY created_at DESC, seq DESC
             LIMIT ?",
        )
        .bind(limit_i)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter()
            .map(row_to_event)
            .map(|event| event.and_then(|event| open_stored_event(&self.crypto, event)))
            .collect()
    }
}

fn row_to_event(row: &sqlx::sqlite::SqliteRow) -> Result<Event> {
    let created_raw: String = row.get("created_at");
    let created_at = chrono::DateTime::parse_from_rfc3339(&created_raw)
        .map_err(|e| PhotonError::persistence("sqlite decode", e))?
        .with_timezone(&Utc);
    let actor_json: String = row.get("actor_json");
    let payload_json: String = row.get("payload_json");
    Ok(Event {
        event_id: row.get("event_id"),
        topic_name: row.get("topic_name"),
        topic_key: row.get("topic_key"),
        seq: row.get("seq"),
        actor_json: serde_json::from_str(&actor_json)?,
        payload_json: serde_json::from_str(&payload_json)?,
        created_at,
    })
}

#[async_trait]
impl StoragePort for SqliteStoragePort {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::sqlite()
    }

    async fn append(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<Event> {
        let seq = self.next_seq(topic_name, topic_key).await?;
        let event = Event {
            event_id: Uuid::new_v4().to_string(),
            topic_name: topic_name.to_string(),
            topic_key: topic_key.map(String::from),
            seq,
            actor_json,
            payload_json,
            created_at: Utc::now(),
        };
        let (plain, sealed) = seal_event_for_storage(&self.crypto, event)?;

        sqlx::query(
            "INSERT INTO events
             (event_id, topic_name, topic_key, seq, actor_json, payload_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&sealed.event_id)
        .bind(&sealed.topic_name)
        .bind(&sealed.topic_key)
        .bind(sealed.seq)
        .bind(sealed.actor_json.to_string())
        .bind(sealed.payload_json.to_string())
        .bind(sealed.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;

        self.events.insert(sealed.event_id.clone(), sealed);
        let _ = self.tx.send(plain.clone());
        Ok(plain)
    }

    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>> {
        let pool = self.pool.clone();
        let crypto = self.crypto.clone();
        let mut live_rx = self.tx.subscribe();
        let topic = topic_name.clone();
        let filter = topic_key_filter;
        let delivery_pins = Arc::clone(&self.delivery_pins);

        Box::pin(stream! {
            if let Some(seq) = after_seq {
                match Self::load_replay_events(&pool, &crypto, &topic_name, filter.as_deref(), seq)
                    .await
                {
                    Ok(events) => {
                        for evt in events {
                            if topic_filter_matches(&evt, &topic, filter.as_ref()) {
                                yield Ok(evt);
                            }
                        }
                    }
                    Err(e) => yield Err(e),
                }
            }

            loop {
                match live_rx.recv().await {
                    Ok(ev) => {
                        if !topic_filter_matches(&ev, &topic, filter.as_ref()) {
                            continue;
                        }
                        if after_seq.is_some_and(|s| ev.seq <= s) {
                            continue;
                        }
                        let pin_key = partition_key(&ev.topic_name, ev.topic_key.as_deref());
                        delivery_pins.insert(pin_key, ev.seq);
                        yield Ok(ev);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Live fanout dropped messages; catch up from durable store.
                        let pin_key = partition_key(&topic, filter.as_deref());
                        let after = delivery_pins
                            .get(&pin_key)
                            .map(|v| *v)
                            .or(after_seq)
                            .unwrap_or(0);
                        match Self::load_replay_events(
                            &pool,
                            &crypto,
                            &topic_name,
                            filter.as_deref(),
                            after,
                        )
                        .await
                        {
                            Ok(events) => {
                                for evt in events {
                                    if topic_filter_matches(&evt, &topic, filter.as_ref()) {
                                        let pk = partition_key(
                                            &evt.topic_name,
                                            evt.topic_key.as_deref(),
                                        );
                                        delivery_pins.insert(pk, evt.seq);
                                        yield Ok(evt);
                                    }
                                }
                            }
                            Err(e) => yield Err(e),
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    async fn get_event(&self, event_id: &str) -> Result<Option<Event>> {
        if let Some(ev) = self.events.get(event_id) {
            return Ok(Some(open_stored_event(&self.crypto, ev.clone())?));
        }
        self.fetch_event_by_id(event_id).await
    }

    async fn list_by_topic(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<Event>> {
        self.fetch_list_by_topic(topic_name, topic_key, after_seq, limit)
            .await
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<Event>> {
        self.fetch_list_recent(limit).await
    }

    async fn load_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>> {
        let key = checkpoint_key(subscription_name, topic_name, topic_key);
        let row = sqlx::query("SELECT last_seq FROM checkpoints WHERE checkpoint_key = ?")
            .bind(&key)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(row.map(|r| r.get::<i64, _>(0)))
    }

    async fn commit_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()> {
        let key = checkpoint_key(subscription_name, topic_name, topic_key);
        sqlx::query(
            "INSERT INTO checkpoints (checkpoint_key, last_seq) VALUES (?, ?)
             ON CONFLICT(checkpoint_key) DO UPDATE
             SET last_seq = MAX(checkpoints.last_seq, excluded.last_seq)",
        )
        .bind(&key)
        .bind(last_seq)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn delivery_seq_pin(&self, topic_name: &str, topic_key: Option<&str>) -> Option<i64> {
        let key = partition_key(topic_name, topic_key);
        self.delivery_pins.get(&key).map(|v| *v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_rejects_empty_path() {
        assert!(matches!(
            SqliteStoragePort::open(" ").await,
            Err(PhotonError::Internal(_))
        ));
    }
}
