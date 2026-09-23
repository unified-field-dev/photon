//! `SQLite` storage port contract tests (no external broker).

#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use photon_backend::StoragePort;
use photon_backend_sqlite::SqliteStoragePort;
use sqlx::Row;
use tempfile::NamedTempFile;
use tokio::task::JoinSet;

fn ensure_dev_transport_key() {
    if std::env::var("PHOTON_TRANSPORT_KEY").is_err() {
        std::env::set_var(
            "PHOTON_TRANSPORT_KEY",
            "cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=",
        );
    }
}

async fn open_temp_port() -> (SqliteStoragePort, NamedTempFile) {
    ensure_dev_transport_key();
    let file = NamedTempFile::new().expect("temp db");
    let path = file.path().to_string_lossy().into_owned();
    let port = SqliteStoragePort::open(&path).await.expect("open sqlite");
    (port, file)
}

#[tokio::test]
async fn sqlite_append_subscribe_checkpoint_roundtrip() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.contract.{}", uuid::Uuid::new_v4());
    let actor = serde_json::json!({"test": "actor"});
    let payload = serde_json::json!({"contract": true});

    let published = port
        .append(&topic, None, actor, payload)
        .await
        .expect("append");
    assert!(published.seq > 0);

    let mut stream = port.subscribe(topic.clone(), None, Some(0));
    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("subscribe timeout")
        .expect("stream ended")
        .expect("event");

    assert_eq!(received.event_id, published.event_id);

    port.commit_checkpoint("sub-a", &topic, None, published.seq)
        .await
        .expect("commit");
    let loaded = port
        .load_checkpoint("sub-a", &topic, None)
        .await
        .expect("load");
    assert_eq!(loaded, Some(published.seq));
}

#[tokio::test]
async fn sqlite_head_seq_tracks_appended_events() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.contract.{}", uuid::Uuid::new_v4());

    let before = port.head_seq(&topic, None).await.expect("read head seq");
    assert_eq!(before, None);

    for _ in 0..3 {
        port.append(&topic, None, serde_json::json!({}), serde_json::json!({}))
            .await
            .expect("append");
    }

    let after = port.head_seq(&topic, None).await.expect("read head seq");
    assert_eq!(after, Some(3));
}

#[tokio::test]
async fn sqlite_get_event_and_keyed_filter() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.keyed.{}", uuid::Uuid::new_v4());
    let key = "shard-a";

    let published = port
        .append(
            &topic,
            Some(key),
            serde_json::json!({}),
            serde_json::json!({"k": key}),
        )
        .await
        .expect("append");

    let fetched = port
        .get_event(&published.event_id)
        .await
        .expect("get_event")
        .expect("found");
    assert_eq!(fetched.seq, published.seq);

    let mut stream = port.subscribe(topic.clone(), Some(key.to_string()), Some(0));
    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout")
        .expect("stream")
        .expect("event");
    assert_eq!(received.topic_key.as_deref(), Some(key));
}

#[tokio::test]
async fn sqlite_persists_ciphertext_and_returns_plaintext() {
    ensure_dev_transport_key();
    let file = NamedTempFile::new().expect("temp db");
    let path = file.path().to_string_lossy().into_owned();
    let port = SqliteStoragePort::open(&path).await.expect("open sqlite");
    let marker = "SECRET_PLAINTEXT_MARKER_xyz";
    let published = port
        .append(
            "testkit.ciphertext",
            None,
            serde_json::json!({"actor": "test"}),
            serde_json::json!({"secret": marker}),
        )
        .await
        .expect("append");

    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{path}"))
        .await
        .expect("open raw sqlite");
    let row = sqlx::query("SELECT payload_json FROM events WHERE event_id = ?")
        .bind(&published.event_id)
        .fetch_one(&pool)
        .await
        .expect("stored row");
    let stored_payload: String = row.get("payload_json");
    assert!(!stored_payload.contains(marker));

    let fetched = port
        .get_event(&published.event_id)
        .await
        .expect("get event")
        .expect("event");
    assert_eq!(fetched.payload_json, published.payload_json);

    let mut stream = port.subscribe("testkit.ciphertext".into(), None, Some(0));
    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("subscribe timeout")
        .expect("stream ended")
        .expect("event");
    assert_eq!(received.payload_json, published.payload_json);
}

#[tokio::test]
async fn sqlite_replay_survives_reopen() {
    ensure_dev_transport_key();
    let file = NamedTempFile::new().expect("temp db");
    let path = file.path().to_string_lossy().into_owned();
    let topic = format!("testkit.reopen.{}", uuid::Uuid::new_v4());

    let published = {
        let port = SqliteStoragePort::open(&path).await.expect("open");
        port.append(
            &topic,
            None,
            serde_json::json!({}),
            serde_json::json!({"persist": true}),
        )
        .await
        .expect("append")
    };

    let reopened = SqliteStoragePort::open(&path).await.expect("reopen");
    let mut stream = reopened.subscribe(topic, None, Some(0));
    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout")
        .expect("stream")
        .expect("event");
    assert_eq!(received.event_id, published.event_id);
}

#[tokio::test]
async fn sqlite_list_by_topic_and_list_recent_honor_limit() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.list.{}", uuid::Uuid::new_v4());
    let mut ids = Vec::new();
    for i in 0..4 {
        let ev = port
            .append(
                &topic,
                None,
                serde_json::json!({}),
                serde_json::json!({"n": i}),
            )
            .await
            .expect("append");
        ids.push(ev.event_id);
    }
    let _other = port
        .append(
            &format!("testkit.other.{}", uuid::Uuid::new_v4()),
            None,
            serde_json::json!({}),
            serde_json::json!({"n": 99}),
        )
        .await
        .expect("append other");

    let page = port
        .list_by_topic(&topic, None, None, 2)
        .await
        .expect("list_by_topic");
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].event_id, ids[0]);
    assert_eq!(page[1].event_id, ids[1]);
    assert!(page.windows(2).all(|w| w[0].seq < w[1].seq));

    let after = port
        .list_by_topic(&topic, None, Some(1), 10)
        .await
        .expect("after_seq");
    assert!(after.iter().all(|e| e.seq > 1));
    assert_eq!(after.len(), 3);

    let recent = port.list_recent(2).await.expect("list_recent");
    assert_eq!(recent.len(), 2);
    assert!(recent[0].created_at >= recent[1].created_at);

    assert!(port
        .list_by_topic(&topic, None, None, 0)
        .await
        .expect("zero")
        .is_empty());
}

#[tokio::test]
async fn sqlite_file_uses_wal_journal() {
    let (port, file) = open_temp_port().await;
    drop(port);
    let path = file.path().to_string_lossy().into_owned();
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{path}"))
        .await
        .expect("reopen");
    let row = sqlx::query("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await
        .expect("pragma");
    let mode: String = row.get(0);
    assert_eq!(mode.to_ascii_lowercase(), "wal");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_sustained_checkpoint_commits_at_pd2_floor() {
    let (port, _file) = open_temp_port().await;
    let topic = format!("testkit.wal-chk.{}", uuid::Uuid::new_v4());
    let port = Arc::new(port);
    let start = Instant::now();
    let duration = Duration::from_secs(30);
    let mut joins = JoinSet::new();
    for sub in 0..4u32 {
        let port = Arc::clone(&port);
        let topic = topic.clone();
        joins.spawn(async move {
            let mut seq = 0i64;
            while start.elapsed() < duration {
                seq += 1;
                port.commit_checkpoint(&format!("sub-{sub}"), &topic, None, seq)
                    .await
                    .expect("checkpoint");
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            seq
        });
    }
    let mut total = 0i64;
    while let Some(res) = joins.join_next().await {
        total += res.expect("join");
    }
    assert!(
        total >= 1_400,
        "expected ~50 checkpoints/s for 30s, got {total}"
    );
    let loaded = port
        .load_checkpoint("sub-0", &topic, None)
        .await
        .expect("load");
    assert!(loaded.is_some_and(|seq| seq > 0));
}

#[tokio::test]
async fn sqlite_open_rejects_directory_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let result = SqliteStoragePort::open(dir.path().to_str().expect("utf8")).await;
    assert!(result.is_err(), "directory is not a sqlite file");
}
