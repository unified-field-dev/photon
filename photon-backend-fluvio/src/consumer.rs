//! Fluvio consumer setup for Photon subscriptions.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_stream::stream;
use fluvio::consumer::{ConsumerConfigExtBuilder, OffsetManagementStrategy};
use fluvio::Offset;
use futures::stream::{Stream as FuturesStream, StreamExt};
use photon_backend::models::Event;
use photon_backend::topic_filter_matches;
use photon_backend::{PhotonError, Result};
use tokio::sync::mpsc;

use crate::checkpoint::CheckpointStore;
use crate::config::{FluvioConfig, ReplayCursor};
use crate::connect::SharedClient;
use crate::message::{decode_record, record_sequence};
use crate::stream_shard::{composite_seq, fluvio_topic_for, local_after_seq_for_shard, pick_shard};

/// Build a Fluvio-safe durable consumer id for a keyed subscription.
#[must_use]
pub fn durable_consumer_name(topic: &str, topic_key: &str) -> String {
    format!(
        "photon-{}-{}",
        sanitize_name_part(topic),
        sanitize_name_part(topic_key)
    )
}

/// Stable group id alias (NATS deliver-subject analogue).
#[must_use]
pub fn consumer_group_for(durable_name: &str) -> String {
    durable_name.to_string()
}

fn sanitize_name_part(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            ' ' | '.' | '*' | '>' | '/' | ':' => '_',
            c if c.is_ascii_alphanumeric() || c == '-' || c == '_' => c,
            _ => '_',
        })
        .collect()
}

fn offset_start(replay_cursor: ReplayCursor, after_seq: Option<i64>) -> Result<Offset> {
    match replay_cursor {
        ReplayCursor::TailOnly => Ok(Offset::end()),
        ReplayCursor::StreamSeq => after_seq.filter(|&s| s >= 0).map_or_else(
            || Ok(Offset::end()),
            |seq| {
                Offset::absolute(seq)
                    .map_err(|e| PhotonError::caused(format!("fluvio subscribe offset {seq}"), e))
            },
        ),
    }
}

fn normalize_event_seq(event: &mut Event, offset: i64, config: &FluvioConfig, topic_shard: u32) {
    if config.replay_cursor != ReplayCursor::StreamSeq {
        return;
    }
    let shard = if config.is_sharded() {
        event
            .topic_key
            .as_deref()
            .map_or(topic_shard, |key| pick_shard(key, config.topic_shards))
    } else {
        0
    };
    if event.seq == 0 {
        let local = record_sequence(offset);
        event.seq = if config.is_sharded() {
            composite_seq(shard, u64::try_from(local.max(0)).unwrap_or(0))
        } else {
            local
        };
    } else if config.is_sharded() && event.seq < crate::stream_shard::SEQ_STRIDE {
        event.seq = composite_seq(shard, u64::try_from(event.seq.max(0)).unwrap_or(0));
    }
}

async fn ensure_topic_cached(
    client: &SharedClient,
    config: &FluvioConfig,
    ensured_topics: &Arc<dashmap::DashSet<String>>,
    fluvio_topic: &str,
) -> Result<()> {
    if ensured_topics.contains(fluvio_topic) {
        return Ok(());
    }
    crate::topic::ensure_data_topic(client, config, fluvio_topic).await?;
    ensured_topics.insert(fluvio_topic.to_string());
    Ok(())
}

/// Open a consumer stream for a topic partition subscription.
#[must_use]
pub fn subscribe_stream(
    client: SharedClient,
    config: FluvioConfig,
    _checkpoint_store: CheckpointStore,
    ensured_topics: Arc<dashmap::DashSet<String>>,
    topic_name: String,
    topic_key_filter: Option<String>,
    after_seq: Option<i64>,
) -> Pin<Box<dyn FuturesStream<Item = Result<Event>> + Send>> {
    if config.topic_shards <= 1 {
        return subscribe_single(
            client,
            config,
            ensured_topics,
            0,
            topic_name,
            topic_key_filter,
            after_seq,
        );
    }

    if let Some(ref key) = topic_key_filter {
        let shard = pick_shard(key, config.topic_shards);
        let local_after = local_after_seq_for_shard(shard, after_seq);
        return subscribe_single(
            client,
            config,
            ensured_topics,
            shard,
            topic_name,
            Some(key.clone()),
            local_after,
        );
    }

    subscribe_merge_shards(client, config, ensured_topics, topic_name, after_seq)
}

fn subscribe_merge_shards(
    client: SharedClient,
    config: FluvioConfig,
    ensured_topics: Arc<dashmap::DashSet<String>>,
    topic_name: String,
    after_seq: Option<i64>,
) -> Pin<Box<dyn FuturesStream<Item = Result<Event>> + Send>> {
    let shard_count = config.topic_shards;
    Box::pin(stream! {
        let cursors = after_seq_by_shard(&config, &HashMap::new(), after_seq);
        let (tx, mut rx) = mpsc::channel::<Result<Event>>(256);
        for shard in 0..shard_count {
            let local_after = cursors.get(&shard).copied().flatten();
            let mut sub = subscribe_single(
                client.clone(),
                config.clone(),
                Arc::clone(&ensured_topics),
                shard,
                topic_name.clone(),
                None,
                local_after,
            );
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(item) = sub.next().await {
                    if tx.send(item).await.is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        while let Some(item) = rx.recv().await {
            yield item;
        }
    })
}

fn subscribe_single(
    client: SharedClient,
    config: FluvioConfig,
    ensured_topics: Arc<dashmap::DashSet<String>>,
    topic_shard: u32,
    topic_name: String,
    topic_key_filter: Option<String>,
    after_seq: Option<i64>,
) -> Pin<Box<dyn FuturesStream<Item = Result<Event>> + Send>> {
    let filter = topic_key_filter;
    let fluvio_topic = fluvio_topic_for(&config, topic_shard, &topic_name);
    let effective_after = if config.replay_cursor == ReplayCursor::TailOnly {
        None
    } else {
        after_seq
    };

    Box::pin(stream! {
        if let Err(e) = ensure_topic_cached(&client, &config, &ensured_topics, &fluvio_topic).await {
            yield Err(e);
            return;
        }

        let start = match offset_start(config.replay_cursor, effective_after) {
            Ok(offset) => offset,
            Err(e) => {
                yield Err(e);
                return;
            }
        };

        let consumer_id = format!(
            "photon-fanout-{}-{}",
            uuid::Uuid::new_v4(),
            fluvio_topic.replace('.', "-")
        );

        let consumer_config = match ConsumerConfigExtBuilder::default()
            .topic(fluvio_topic.clone())
            .partition(0)
            .offset_start(start)
            .offset_consumer(consumer_id)
            .offset_strategy(OffsetManagementStrategy::None)
            .build()
        {
            Ok(cfg) => cfg,
            Err(e) => {
                yield Err(PhotonError::Internal(format!(
                    "fluvio consumer config {fluvio_topic}: {e}"
                )));
                return;
            }
        };

        let mut consumer_stream = match client.consumer_with_config(consumer_config).await {
            Ok(stream) => stream,
            Err(e) => {
                yield Err(PhotonError::Internal(format!(
                    "fluvio consumer {fluvio_topic}: {e}"
                )));
                return;
            }
        };

        let topic = topic_name.clone();
        while let Some(result) = consumer_stream.next().await {
            match result {
                Ok(record) => {
                    let offset = record.offset();
                    let payload = record.value();
                    match decode_record(payload, offset) {
                        Ok(mut decoded) => {
                            normalize_event_seq(
                                &mut decoded.event,
                                offset,
                                &config,
                                topic_shard,
                            );
                            if topic_filter_matches(&decoded.event, &topic, filter.as_ref())
                                && effective_after.is_none_or(|seq| decoded.event.seq > seq)
                            {
                                yield Ok(decoded.event);
                            }
                        }
                        Err(e) => yield Err(e),
                    }
                }
                Err(e) => {
                    yield Err(PhotonError::Internal(format!(
                        "fluvio subscribe recv {fluvio_topic}: {e:?}"
                    )));
                }
            }
        }
    })
}

/// Build per-shard replay cursors for unkeyed sharded subscriptions.
#[must_use]
pub fn after_seq_by_shard(
    config: &FluvioConfig,
    stored: &HashMap<u32, i64>,
    after_seq: Option<i64>,
) -> HashMap<u32, Option<i64>> {
    let mut out = HashMap::new();
    for shard in 0..config.topic_shards.max(1) {
        let from_store = stored.get(&shard).copied();
        let from_arg = local_after_seq_for_shard(shard, after_seq);
        let merged = match (from_store, from_arg) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        out.insert(shard, merged);
    }
    out
}
