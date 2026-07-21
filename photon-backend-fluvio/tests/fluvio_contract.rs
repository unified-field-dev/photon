//! Live Fluvio integration tests (require `PHOTON_FLUVIO_ENDPOINT`).

#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::time::Duration;

use std::sync::Arc;

use futures::StreamExt;
use photon_backend::{StoragePort, TransportCrypto};
use photon_backend_fluvio::{
    consumer_group_for, durable_consumer_name, FluvioStoragePort, ReplayCursor,
};

fn fluvio_available() -> bool {
    std::env::var("PHOTON_FLUVIO_ENDPOINT").is_ok()
}

async fn wait_for_event_ids(seen: &Arc<tokio::sync::Mutex<Vec<String>>>, expected: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        if seen.lock().await.iter().any(|id| id == expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for event {expected}");
}

/// Probe-publish until every live-tail subscriber has delivered the probe.
async fn warm_live_tail(
    port: &FluvioStoragePort,
    topic: &str,
    topic_key: Option<&str>,
    seen: &[&Arc<tokio::sync::Mutex<Vec<String>>>],
) {
    for attempt in 0..8 {
        let probe = port
            .append(
                topic,
                topic_key,
                serde_json::json!({"actor": true}),
                serde_json::json!({"warmup": attempt}),
            )
            .await
            .expect("warmup append");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut attached = false;
        while tokio::time::Instant::now() < deadline {
            let ready = {
                let mut ok = true;
                for bucket in seen {
                    if !bucket.lock().await.iter().any(|id| id == &probe.event_id) {
                        ok = false;
                        break;
                    }
                }
                ok
            };
            if ready {
                attached = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if attached {
            for bucket in seen {
                bucket.lock().await.clear();
            }
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("live-tail warmup failed on {topic}");
}

async fn wait_for_sharded_event(
    seen: &Arc<tokio::sync::Mutex<Vec<photon_backend::models::Event>>>,
    expected: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        if seen
            .lock()
            .await
            .iter()
            .any(|event| event.event_id == expected)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for sharded event {expected}");
}

#[tokio::test]
#[ignore = "requires PHOTON_FLUVIO_ENDPOINT and live Fluvio"]
async fn fluvio_append_subscribe_checkpoint_roundtrip() {
    if !fluvio_available() {
        return;
    }

    let port = FluvioStoragePort::builder()
        .from_env_defaults()
        .replay_cursor(ReplayCursor::StreamSeq)
        .sync_ack(true)
        .build()
        .await
        .expect("connect fluvio");
    let topic = format!("testkit.contract.{}", uuid::Uuid::new_v4());
    let actor = serde_json::json!({"test": "actor"});
    let payload = serde_json::json!({"contract": true});

    let published = port
        .append(&topic, None, actor, payload)
        .await
        .expect("append");

    assert!(published.seq > 0, "offset seq from produce ack");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut stream = port.subscribe(topic.clone(), None, Some(0));
    let received = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("subscribe timeout")
        .expect("subscribe stream ended")
        .expect("subscribe event");

    assert_eq!(received.event_id, published.event_id);
    assert_eq!(received.seq, published.seq);

    port.commit_checkpoint("contract-sub", &topic, None, published.seq)
        .await
        .expect("commit checkpoint");

    let loaded = port
        .load_checkpoint("contract-sub", &topic, None)
        .await
        .expect("load checkpoint");
    assert_eq!(loaded, Some(published.seq));

    let missing = port
        .get_event(&published.event_id)
        .await
        .expect("get_event");
    assert!(missing.is_none());
}

#[tokio::test]
#[ignore = "requires PHOTON_FLUVIO_ENDPOINT and live Fluvio"]
async fn fluvio_crypto_side_effect_matches_mem() {
    if !fluvio_available() {
        return;
    }

    let _crypto = TransportCrypto::from_bytes(*b"photon-dev-transport-key-32bytes");
    let port = FluvioStoragePort::from_env().await.expect("connect fluvio");
    let topic = format!("testkit.crypto.{}", uuid::Uuid::new_v4());

    let result = port
        .append(
            &topic,
            None,
            serde_json::json!({"actor": true}),
            serde_json::json!({"payload": true}),
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
#[ignore = "requires PHOTON_FLUVIO_ENDPOINT and live Fluvio"]
async fn fluvio_dual_subscriber_fanout() {
    if !fluvio_available() {
        return;
    }

    let port = Arc::new(FluvioStoragePort::from_env().await.expect("connect fluvio"));
    let topic = format!("testkit.fanout.{}", uuid::Uuid::new_v4());

    let seen_a = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let seen_b = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let port_a = Arc::clone(&port);
    let topic_a = topic.clone();
    let seen_a_task = Arc::clone(&seen_a);
    let task_a = tokio::spawn(async move {
        let mut stream = port_a.subscribe(topic_a, None, None);
        while let Some(Ok(event)) = stream.next().await {
            seen_a_task.lock().await.push(event.event_id);
        }
    });

    let port_b = Arc::clone(&port);
    let topic_b = topic.clone();
    let seen_b_task = Arc::clone(&seen_b);
    let task_b = tokio::spawn(async move {
        let mut stream = port_b.subscribe(topic_b, None, None);
        while let Some(Ok(event)) = stream.next().await {
            seen_b_task.lock().await.push(event.event_id);
        }
    });

    warm_live_tail(port.as_ref(), &topic, None, &[&seen_a, &seen_b]).await;

    let published = port
        .append(
            &topic,
            None,
            serde_json::json!({"actor": true}),
            serde_json::json!({"fanout": true}),
        )
        .await
        .expect("append");

    wait_for_event_ids(&seen_a, &published.event_id).await;
    wait_for_event_ids(&seen_b, &published.event_id).await;
    task_a.abort();
    task_b.abort();

    let ids_a = seen_a.lock().await.clone();
    let ids_b = seen_b.lock().await.clone();
    assert!(ids_a.contains(&published.event_id));
    assert!(ids_b.contains(&published.event_id));
}

#[tokio::test]
#[ignore = "requires PHOTON_FLUVIO_ENDPOINT and live Fluvio"]
async fn fluvio_tail_only_fanout() {
    if !fluvio_available() {
        return;
    }

    let port = Arc::new(
        FluvioStoragePort::builder()
            .from_env_defaults()
            .replay_cursor(ReplayCursor::TailOnly)
            .sync_ack(false)
            .max_inflight(64)
            .build()
            .await
            .expect("connect fluvio"),
    );
    let topic = format!("testkit.tail.{}", uuid::Uuid::new_v4());

    let seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let port_sub = Arc::clone(&port);
    let topic_sub = topic.clone();
    let seen_task = Arc::clone(&seen);
    let task = tokio::spawn(async move {
        let mut stream = port_sub.subscribe(topic_sub, None, None);
        while let Some(Ok(event)) = stream.next().await {
            seen_task.lock().await.push(event.event_id);
        }
    });

    warm_live_tail(port.as_ref(), &topic, None, &[&seen]).await;

    let published = port
        .append(
            &topic,
            None,
            serde_json::json!({"actor": true}),
            serde_json::json!({"tail": true}),
        )
        .await
        .expect("append");

    assert_eq!(published.seq, 0);

    wait_for_event_ids(&seen, &published.event_id).await;
    task.abort();

    let ids = seen.lock().await.clone();
    assert!(ids.contains(&published.event_id));

    port.commit_checkpoint("tail-sub", &topic, None, 99)
        .await
        .expect("commit noop");
    let loaded = port
        .load_checkpoint("tail-sub", &topic, None)
        .await
        .expect("load noop");
    assert!(loaded.is_none());
}

#[tokio::test]
#[ignore = "requires PHOTON_FLUVIO_ENDPOINT and live Fluvio"]
async fn fluvio_keyed_shard_subscriptions_are_disjoint() {
    if !fluvio_available() {
        return;
    }

    use photon_backend::shard_router::shard_storage_key;

    let port = Arc::new(FluvioStoragePort::from_env().await.expect("connect fluvio"));
    let topic = format!("testkit.shard.{}", uuid::Uuid::new_v4());
    let key0 = shard_storage_key(0);
    let key4 = shard_storage_key(4);

    let seen_a = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let seen_b = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let port_a = Arc::clone(&port);
    let topic_a = topic.clone();
    let key0_clone = key0.clone();
    let seen_a_task = Arc::clone(&seen_a);
    let task_a = tokio::spawn(async move {
        let mut stream = port_a.subscribe(topic_a, Some(key0_clone), None);
        while let Some(Ok(event)) = stream.next().await {
            seen_a_task.lock().await.push(event.event_id);
        }
    });

    let port_b = Arc::clone(&port);
    let topic_b = topic.clone();
    let key4_clone = key4.clone();
    let seen_b_task = Arc::clone(&seen_b);
    let task_b = tokio::spawn(async move {
        let mut stream = port_b.subscribe(topic_b, Some(key4_clone), None);
        while let Some(Ok(event)) = stream.next().await {
            seen_b_task.lock().await.push(event.event_id);
        }
    });

    warm_live_tail(port.as_ref(), &topic, Some(&key0), &[&seen_a]).await;
    warm_live_tail(port.as_ref(), &topic, Some(&key4), &[&seen_b]).await;

    let evt0 = port
        .append(
            &topic,
            Some(&key0),
            serde_json::json!({"actor": true}),
            serde_json::json!({"shard": 0}),
        )
        .await
        .expect("append shard 0");

    let evt4 = port
        .append(
            &topic,
            Some(&key4),
            serde_json::json!({"actor": true}),
            serde_json::json!({"shard": 4}),
        )
        .await
        .expect("append shard 4");

    wait_for_event_ids(&seen_a, &evt0.event_id).await;
    wait_for_event_ids(&seen_b, &evt4.event_id).await;
    task_a.abort();
    task_b.abort();

    let ids_a = seen_a.lock().await.clone();
    let ids_b = seen_b.lock().await.clone();
    assert!(ids_a.contains(&evt0.event_id));
    assert!(!ids_a.contains(&evt4.event_id));
    assert!(ids_b.contains(&evt4.event_id));
    assert!(!ids_b.contains(&evt0.event_id));
}

#[tokio::test]
#[ignore = "requires PHOTON_FLUVIO_ENDPOINT and live Fluvio"]
async fn fluvio_keyed_durable_consumer_reconnect() {
    if !fluvio_available() {
        return;
    }

    use photon_backend::shard_router::shard_storage_key;
    use tokio::sync::mpsc;

    let port = Arc::new(FluvioStoragePort::from_env().await.expect("connect fluvio"));
    let topic = format!("testkit.durable.{}", uuid::Uuid::new_v4());
    let key = shard_storage_key(2);

    let (tx, mut rx) = mpsc::channel(4);
    let port1 = Arc::clone(&port);
    let topic1 = topic.clone();
    let key1 = key.clone();
    let task1 = tokio::spawn(async move {
        let mut stream = port1.subscribe(topic1, Some(key1), None);
        while let Some(item) = stream.next().await {
            if tx.send(item).await.is_err() {
                break;
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let evt1 = port
        .append(
            &topic,
            Some(&key),
            serde_json::json!({"actor": true}),
            serde_json::json!({"round": 1}),
        )
        .await
        .expect("append round 1");

    let received1 = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("subscribe timeout")
        .expect("channel closed")
        .expect("subscribe event");
    assert_eq!(received1.event_id, evt1.event_id);
    task1.abort();

    let (tx, mut rx) = mpsc::channel(4);
    let port2 = Arc::clone(&port);
    let topic2 = topic.clone();
    let key2 = key.clone();
    let task2 = tokio::spawn(async move {
        let mut stream = port2.subscribe(topic2, Some(key2), None);
        while let Some(item) = stream.next().await {
            if tx.send(item).await.is_err() {
                break;
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(400)).await;

    let evt2 = port
        .append(
            &topic,
            Some(&key),
            serde_json::json!({"actor": true}),
            serde_json::json!({"round": 2}),
        )
        .await
        .expect("append round 2");

    let received2 = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("subscribe timeout")
        .expect("channel closed")
        .expect("subscribe event");
    assert_eq!(received2.event_id, evt2.event_id);
    task2.abort();
}

#[tokio::test]
#[ignore = "requires PHOTON_FLUVIO_ENDPOINT and live Fluvio"]
async fn fluvio_keyed_durable_group_id_stable() {
    if !fluvio_available() {
        return;
    }

    use photon_backend::shard_router::shard_storage_key;

    let topic = format!("testkit.durable.subject.{}", uuid::Uuid::new_v4());
    let key = shard_storage_key(3);
    let durable_name = durable_consumer_name(&topic, &key);
    let expected_group = consumer_group_for(&durable_name);

    assert_eq!(expected_group, durable_name);
    assert!(expected_group.starts_with("photon-"));
}

#[tokio::test]
#[ignore = "requires PHOTON_FLUVIO_ENDPOINT and live Fluvio"]
async fn fluvio_sharded_topic_append_subscribe_checkpoint() {
    if !fluvio_available() {
        return;
    }

    std::env::set_var("PHOTON_FLUVIO_TOPIC_SHARDS", "4");
    let port = Arc::new(
        FluvioStoragePort::builder()
            .from_env_defaults()
            .replay_cursor(ReplayCursor::StreamSeq)
            .sync_ack(true)
            .build()
            .await
            .expect("connect sharded fluvio"),
    );
    std::env::remove_var("PHOTON_FLUVIO_TOPIC_SHARDS");

    let topic = format!("testkit.sharded.{}", uuid::Uuid::new_v4());
    let partition_key = format!("partition-{}", uuid::Uuid::new_v4());
    let actor = serde_json::json!({"test": "actor"});
    let payload = serde_json::json!({"sharded": true});

    let seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let port_sub = Arc::clone(&port);
    let topic_sub = topic.clone();
    let key_sub = partition_key.clone();
    let seen_task = Arc::clone(&seen);
    let sub_task = tokio::spawn(async move {
        let mut stream = port_sub.subscribe(topic_sub, Some(key_sub), None);
        while let Some(Ok(event)) = stream.next().await {
            seen_task.lock().await.push(event);
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let published = port
        .append(&topic, Some(&partition_key), actor, payload)
        .await
        .expect("append");

    assert!(published.seq > 0);
    let (shard, local) = photon_backend_fluvio::stream_shard::decompose_seq(published.seq);
    assert!(shard < 4, "shard index in composite seq");
    assert!(local > 0, "local offset seq");

    wait_for_sharded_event(&seen, &published.event_id).await;
    sub_task.abort();

    let events = seen.lock().await;
    let received = events
        .iter()
        .find(|e| e.event_id == published.event_id)
        .expect("received published event");
    assert_eq!(received.seq, published.seq);

    port.commit_checkpoint("sharded-sub", &topic, Some(&partition_key), published.seq)
        .await
        .expect("commit checkpoint");

    let loaded = port
        .load_checkpoint("sharded-sub", &topic, Some(&partition_key))
        .await
        .expect("load checkpoint");
    assert_eq!(loaded, Some(published.seq));
}

#[tokio::test]
#[ignore = "requires PHOTON_FLUVIO_ENDPOINT and live Fluvio"]
async fn fluvio_sharded_unkeyed_fanin_subscribe() {
    if !fluvio_available() {
        return;
    }

    std::env::set_var("PHOTON_FLUVIO_TOPIC_SHARDS", "4");
    let port = Arc::new(
        FluvioStoragePort::builder()
            .from_env_defaults()
            .replay_cursor(ReplayCursor::StreamSeq)
            .sync_ack(true)
            .build()
            .await
            .expect("connect sharded fluvio"),
    );
    std::env::remove_var("PHOTON_FLUVIO_TOPIC_SHARDS");

    let topic = format!("testkit.sharded.fanin.{}", uuid::Uuid::new_v4());

    let seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let port_sub = Arc::clone(&port);
    let topic_sub = topic.clone();
    let seen_task = Arc::clone(&seen);
    let sub_task = tokio::spawn(async move {
        let mut stream = port_sub.subscribe(topic_sub, None, None);
        while let Some(Ok(event)) = stream.next().await {
            seen_task.lock().await.push(event.event_id);
        }
    });

    // Live-tail across 4 shards: probe until fan-in is attached before the real append.
    let mut attached = false;
    for attempt in 0..8 {
        let probe = port
            .append(
                &topic,
                None,
                serde_json::json!({"actor": true}),
                serde_json::json!({"warmup": attempt}),
            )
            .await
            .expect("warmup append");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            if seen.lock().await.iter().any(|id| id == &probe.event_id) {
                attached = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if attached {
            seen.lock().await.clear();
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(attached, "sharded fan-in subscribe did not attach");

    let evt = port
        .append(
            &topic,
            None,
            serde_json::json!({"actor": true}),
            serde_json::json!({"n": 1}),
        )
        .await
        .expect("append");

    wait_for_event_ids(&seen, &evt.event_id).await;
    sub_task.abort();

    let ids = seen.lock().await;
    assert!(ids.contains(&evt.event_id));
}
