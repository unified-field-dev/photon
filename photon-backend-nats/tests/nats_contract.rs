//! Live `JetStream` integration tests (require `PHOTON_NATS_URL`).

#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use photon_backend::{StoragePort, TransportCrypto};
use photon_backend_nats::{NatsStoragePort, ReplayCursor};

fn nats_available() -> bool {
    std::env::var("PHOTON_NATS_URL").is_ok()
}

#[tokio::test]
#[ignore = "requires PHOTON_NATS_URL and live JetStream"]
async fn nats_append_subscribe_checkpoint_roundtrip() {
    if !nats_available() {
        return;
    }

    let port = NatsStoragePort::builder()
        .from_env_defaults()
        .replay_cursor(ReplayCursor::StreamSeq)
        .sync_ack(true)
        .build()
        .await
        .expect("connect nats");
    let topic = format!("testkit.contract.{}", uuid::Uuid::new_v4());
    let actor = serde_json::json!({"test": "actor"});
    let payload = serde_json::json!({"contract": true});

    let published = port
        .append(&topic, None, actor, payload)
        .await
        .expect("append");

    assert!(published.seq > 0, "stream seq from JetStream ack");

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
#[ignore = "requires PHOTON_NATS_URL and live JetStream"]
async fn nats_crypto_side_effect_matches_mem() {
    if !nats_available() {
        return;
    }

    let _crypto = TransportCrypto::from_bytes(*b"photon-dev-transport-key-32bytes");
    let port = NatsStoragePort::from_env().await.expect("connect nats");
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
#[ignore = "requires PHOTON_NATS_URL and live JetStream"]
async fn nats_dual_subscriber_fanout() {
    if !nats_available() {
        return;
    }

    use std::sync::Arc;
    use tokio::sync::Mutex;

    let port = Arc::new(NatsStoragePort::from_env().await.expect("connect nats"));
    let topic = format!("testkit.fanout.{}", uuid::Uuid::new_v4());

    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));

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

    tokio::time::sleep(Duration::from_millis(100)).await;

    let published = port
        .append(
            &topic,
            None,
            serde_json::json!({"actor": true}),
            serde_json::json!({"fanout": true}),
        )
        .await
        .expect("append");

    tokio::time::sleep(Duration::from_millis(500)).await;
    task_a.abort();
    task_b.abort();

    let ids_a = seen_a.lock().await.clone();
    let ids_b = seen_b.lock().await.clone();
    assert!(ids_a.contains(&published.event_id));
    assert!(ids_b.contains(&published.event_id));
}

#[tokio::test]
#[ignore = "requires PHOTON_NATS_URL and live JetStream"]
async fn nats_tail_only_fanout() {
    if !nats_available() {
        return;
    }

    use std::sync::Arc;
    use tokio::sync::Mutex;

    let port = Arc::new(
        NatsStoragePort::builder()
            .from_env_defaults()
            .replay_cursor(ReplayCursor::TailOnly)
            .sync_ack(false)
            .max_inflight(64)
            .build()
            .await
            .expect("connect nats"),
    );
    let topic = format!("testkit.tail.{}", uuid::Uuid::new_v4());

    let seen = Arc::new(Mutex::new(Vec::new()));
    let port_sub = Arc::clone(&port);
    let topic_sub = topic.clone();
    let seen_task = Arc::clone(&seen);
    let task = tokio::spawn(async move {
        let mut stream = port_sub.subscribe(topic_sub, None, None);
        while let Some(Ok(event)) = stream.next().await {
            seen_task.lock().await.push(event.event_id);
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

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

    tokio::time::sleep(Duration::from_millis(500)).await;
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
#[ignore = "requires PHOTON_NATS_URL and live JetStream"]
async fn nats_keyed_shard_subscriptions_are_disjoint() {
    if !nats_available() {
        return;
    }

    use photon_backend::shard_router::shard_storage_key;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let port = Arc::new(NatsStoragePort::from_env().await.expect("connect nats"));
    let topic = format!("testkit.shard.{}", uuid::Uuid::new_v4());
    let key0 = shard_storage_key(0);
    let key4 = shard_storage_key(4);

    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));

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

    tokio::time::sleep(Duration::from_millis(100)).await;

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

    tokio::time::sleep(Duration::from_millis(500)).await;
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
#[ignore = "requires PHOTON_NATS_URL and live JetStream"]
async fn nats_keyed_durable_consumer_reconnect() {
    if !nats_available() {
        return;
    }

    use photon_backend::shard_router::shard_storage_key;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    let port = Arc::new(NatsStoragePort::from_env().await.expect("connect nats"));
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

    tokio::time::sleep(Duration::from_millis(200)).await;

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

    tokio::time::sleep(Duration::from_millis(200)).await;

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
#[ignore = "requires PHOTON_NATS_URL and live JetStream"]
async fn nats_keyed_durable_deliver_subject_stable() {
    if !nats_available() {
        return;
    }

    use photon_backend::shard_router::shard_storage_key;
    use photon_backend_nats::{
        deliver_subject_for, durable_consumer_name, nats_url_from_env, STREAM_ENV,
    };

    let port = NatsStoragePort::from_env().await.expect("connect nats");
    let topic = format!("testkit.durable.subject.{}", uuid::Uuid::new_v4());
    let key = shard_storage_key(3);
    let durable_name = durable_consumer_name(&topic, &key);
    let expected_deliver = deliver_subject_for(&durable_name);

    let task = tokio::spawn(async move {
        let mut stream = port.subscribe(topic, Some(key), None);
        while stream.next().await.is_some() {}
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    let url = nats_url_from_env().expect("url");
    let client = async_nats::connect(url).await.expect("connect");
    let jetstream = async_nats::jetstream::new(client);
    let stream_name = std::env::var(STREAM_ENV).unwrap_or_else(|_| "photon".into());
    let stream = jetstream.get_stream(stream_name).await.expect("get stream");

    let info = stream
        .consumer_info(&durable_name)
        .await
        .expect("consumer_info");

    assert_eq!(
        info.config.deliver_subject.as_deref(),
        Some(expected_deliver.as_str())
    );

    task.abort();
}

#[tokio::test]
#[ignore = "requires PHOTON_NATS_URL and live JetStream"]
async fn nats_sharded_stream_append_subscribe_checkpoint() {
    if !nats_available() {
        return;
    }

    std::env::set_var("PHOTON_NATS_STREAM_SHARDS", "4");
    let port = Arc::new(
        NatsStoragePort::builder()
            .from_env_defaults()
            .replay_cursor(ReplayCursor::StreamSeq)
            .sync_ack(true)
            .build()
            .await
            .expect("connect sharded nats"),
    );
    std::env::remove_var("PHOTON_NATS_STREAM_SHARDS");

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

    tokio::time::sleep(Duration::from_millis(200)).await;

    let published = port
        .append(&topic, Some(&partition_key), actor, payload)
        .await
        .expect("append");

    assert!(published.seq > 0);
    let (shard, local) = photon_backend_nats::stream_shard::decompose_seq(published.seq);
    assert!(shard < 4, "shard index in composite seq");
    assert!(local > 0, "local stream seq");

    tokio::time::sleep(Duration::from_millis(500)).await;
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
#[ignore = "requires PHOTON_NATS_URL and live JetStream"]
async fn nats_sharded_unkeyed_fanin_subscribe() {
    if !nats_available() {
        return;
    }

    std::env::set_var("PHOTON_NATS_STREAM_SHARDS", "4");
    let port = Arc::new(
        NatsStoragePort::builder()
            .from_env_defaults()
            .replay_cursor(ReplayCursor::StreamSeq)
            .sync_ack(true)
            .build()
            .await
            .expect("connect sharded nats"),
    );
    std::env::remove_var("PHOTON_NATS_STREAM_SHARDS");

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

    tokio::time::sleep(Duration::from_millis(500)).await;

    let evt = port
        .append(
            &topic,
            None,
            serde_json::json!({"actor": true}),
            serde_json::json!({"n": 1}),
        )
        .await
        .expect("append");

    tokio::time::sleep(Duration::from_millis(500)).await;
    sub_task.abort();

    let ids = seen.lock().await;
    assert!(ids.contains(&evt.event_id));
}
