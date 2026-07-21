//! `JetStream` push consumer setup for Photon subscriptions.

use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

use async_nats::jetstream::consumer::{self, push, AckPolicy, DeliverPolicy};
use async_nats::jetstream::stream::Stream;
use async_stream::stream;
use futures::stream::{Stream as FuturesStream, StreamExt};
use photon_backend::models::Event;
use photon_backend::topic_filter_matches;
use photon_backend::{PhotonError, Result};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::checkpoint::CheckpointStore;
use crate::config::{NatsConfig, ReplayCursor};
use crate::message::{decode_event, stream_sequence};
use crate::stream_shard::{
    composite_seq, local_after_seq_for_shard, photon_subject_for, pick_shard, stream_name_for,
};

const EPHEMERAL_INACTIVE: Duration = Duration::from_mins(5);

/// Build a JetStream-safe durable consumer name for a keyed subscription.
#[must_use]
pub fn durable_consumer_name(topic: &str, topic_key: &str) -> String {
    format!(
        "photon-{}-{}",
        sanitize_name_part(topic),
        sanitize_name_part(topic_key)
    )
}

/// Fixed deliver subject for a durable consumer name.
#[must_use]
pub fn deliver_subject_for(durable_name: &str) -> String {
    format!("_INBOX.photon.{durable_name}")
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

fn deliver_policy(replay_cursor: ReplayCursor, after_seq: Option<i64>) -> DeliverPolicy {
    match replay_cursor {
        ReplayCursor::TailOnly => DeliverPolicy::New,
        // `Some(0)` means "replay from start" (testkit verify / catch-up). Use All so we are
        // not coupled to durable ack floors left by a prior DeliverPolicy::New consumer.
        ReplayCursor::StreamSeq => match after_seq {
            Some(0) => DeliverPolicy::All,
            Some(seq) if seq > 0 => {
                let start = u64::try_from(seq.saturating_add(1)).unwrap_or(1);
                DeliverPolicy::ByStartSequence {
                    start_sequence: start,
                }
            }
            _ => DeliverPolicy::New,
        },
    }
}

fn policies_compatible(existing: DeliverPolicy, desired: DeliverPolicy) -> bool {
    match (&existing, &desired) {
        (DeliverPolicy::New, DeliverPolicy::New) | (DeliverPolicy::All, DeliverPolicy::All) => true,
        (
            DeliverPolicy::ByStartSequence { start_sequence: a },
            DeliverPolicy::ByStartSequence { start_sequence: b },
        ) => a == b,
        _ => false,
    }
}

fn consumer_config_matches(
    info: &consumer::Info,
    deliver_subject: &str,
    desired_policy: DeliverPolicy,
) -> bool {
    info.config.deliver_subject.as_deref() == Some(deliver_subject)
        && policies_compatible(info.config.deliver_policy, desired_policy)
}

fn shard_from_subject(subject: &str, shard_count: u32) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    let prefix = format!("{}.", crate::subject::SHARDED_SUBJECT_PREFIX);
    let Some(rest) = subject.strip_prefix(&prefix) else {
        return 0;
    };
    let Some((shard_str, _)) = rest.split_once('.') else {
        return 0;
    };
    shard_str.parse().unwrap_or(0)
}

fn normalize_event_seq(
    event: &mut Event,
    message: &async_nats::jetstream::Message,
    config: &NatsConfig,
) {
    if config.replay_cursor != ReplayCursor::StreamSeq {
        return;
    }
    let shard = if config.is_sharded() {
        event.topic_key.as_deref().map_or_else(
            || shard_from_subject(message.subject.as_str(), config.stream_shards),
            |key| pick_shard(key, config.stream_shards),
        )
    } else {
        0
    };
    if event.seq == 0 {
        if let Some(local) = stream_sequence(message) {
            event.seq = if config.is_sharded() {
                composite_seq(shard, local)
            } else {
                i64::try_from(local).unwrap_or(0)
            };
        }
    } else if config.is_sharded() && event.seq < crate::stream_shard::SEQ_STRIDE {
        event.seq = composite_seq(shard, u64::try_from(event.seq.max(0)).unwrap_or(0));
    }
}

/// Open a push consumer stream for a topic partition subscription.
#[must_use]
pub fn subscribe_push(
    jetstream: async_nats::jetstream::Context,
    config: NatsConfig,
    checkpoint_store: CheckpointStore,
    topic_name: String,
    topic_key_filter: Option<String>,
    after_seq: Option<i64>,
) -> Pin<Box<dyn FuturesStream<Item = Result<Event>> + Send>> {
    if config.stream_shards <= 1 {
        return subscribe_single(
            jetstream,
            config,
            0,
            topic_name,
            topic_key_filter,
            after_seq,
        );
    }

    if let Some(ref key) = topic_key_filter {
        let shard = pick_shard(key, config.stream_shards);
        let local_after = local_after_seq_for_shard(shard, after_seq);
        return subscribe_single(
            jetstream,
            config,
            shard,
            topic_name,
            Some(key.clone()),
            local_after,
        );
    }

    subscribe_merge_shards(jetstream, config, checkpoint_store, topic_name, after_seq)
}

fn subscribe_merge_shards(
    jetstream: async_nats::jetstream::Context,
    config: NatsConfig,
    _checkpoint_store: CheckpointStore,
    topic_name: String,
    after_seq: Option<i64>,
) -> Pin<Box<dyn FuturesStream<Item = Result<Event>> + Send>> {
    let shard_count = config.stream_shards;
    Box::pin(stream! {
        let cursors = after_seq_by_shard(&config, &HashMap::new(), after_seq);
        let (tx, mut rx) = mpsc::channel::<Result<Event>>(256);
        for shard in 0..shard_count {
            let local_after = cursors.get(&shard).copied().flatten();
            let mut sub = subscribe_single(
                jetstream.clone(),
                config.clone(),
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

#[allow(clippy::too_many_lines)]
fn subscribe_single(
    jetstream: async_nats::jetstream::Context,
    config: NatsConfig,
    stream_shard: u32,
    topic_name: String,
    topic_key_filter: Option<String>,
    after_seq: Option<i64>,
) -> Pin<Box<dyn FuturesStream<Item = Result<Event>> + Send>> {
    let filter = topic_key_filter.clone();
    let stream_name = stream_name_for(&config, stream_shard);
    let filter_subject = photon_subject_for(stream_shard, config.stream_shards, &topic_name);

    Box::pin(stream! {
        let deliver_policy = deliver_policy(config.replay_cursor, after_seq);

        let stream = match jetstream.get_stream(&stream_name).await {
            Ok(stream) => stream,
            Err(e) => {
                yield Err(PhotonError::Internal(format!(
                    "nats get stream {stream_name}: {e}"
                )));
                return;
            }
        };

        let consumer = match open_push_consumer(
            &stream,
            &topic_name,
            &filter_subject,
            topic_key_filter.as_deref(),
            deliver_policy,
        )
        .await
        {
            Ok(consumer) => consumer,
            Err(e) => {
                yield Err(e);
                return;
            }
        };

        let mut messages = match consumer.messages().await {
            Ok(messages) => messages,
            Err(e) => {
                yield Err(PhotonError::Internal(format!(
                    "nats consumer messages {filter_subject}: {e}"
                )));
                return;
            }
        };

        let topic = topic_name.clone();
        while let Some(result) = messages.next().await {
            match result {
                Ok(message) => {
                    let ack_result = message.ack().await;
                    match decode_event(&message) {
                        Ok(mut event) => {
                            normalize_event_seq(&mut event, &message, &config);
                            if topic_filter_matches(&event, &topic, filter.as_ref())
                                && after_seq.is_none_or(|seq| event.seq > seq)
                            {
                                yield Ok(event);
                            }
                        }
                        Err(e) => yield Err(e),
                    }
                    if let Err(e) = ack_result {
                        yield Err(PhotonError::caused("nats message ack:", e));
                    }
                }
                Err(e) => {
                    yield Err(PhotonError::caused("nats subscribe recv:", e));
                }
            }
        }
    })
}

async fn open_push_consumer(
    stream: &Stream,
    topic_name: &str,
    filter_subject: &str,
    topic_key_filter: Option<&str>,
    deliver_policy: DeliverPolicy,
) -> Result<async_nats::jetstream::consumer::PushConsumer> {
    match topic_key_filter {
        // Full-stream replay must not reuse keyed durables left in DeliverPolicy::New —
        // ack floors / recreate races were dropping one message per virtual shard (~8/200).
        Some(_) if matches!(deliver_policy, DeliverPolicy::All) => {
            open_ephemeral_push_consumer(stream, filter_subject, deliver_policy).await
        }
        Some(key) => {
            let durable_name = durable_consumer_name(topic_name, key);
            let deliver_subject = deliver_subject_for(&durable_name);
            ensure_keyed_durable_consumer(
                stream,
                &durable_name,
                &deliver_subject,
                filter_subject,
                deliver_policy,
            )
            .await
        }
        None => open_ephemeral_push_consumer(stream, filter_subject, deliver_policy).await,
    }
}

async fn open_ephemeral_push_consumer(
    stream: &Stream,
    filter_subject: &str,
    deliver_policy: DeliverPolicy,
) -> Result<async_nats::jetstream::consumer::PushConsumer> {
    let deliver_subject = format!("_INBOX.photon.{}", Uuid::new_v4());

    let config = push::Config {
        deliver_subject,
        filter_subject: filter_subject.to_string(),
        deliver_policy,
        ack_policy: AckPolicy::Explicit,
        inactive_threshold: EPHEMERAL_INACTIVE,
        ..Default::default()
    };

    stream
        .create_consumer(config)
        .await
        .map_err(|e| PhotonError::caused(format!("nats create consumer {filter_subject}"), e))
}

async fn ensure_keyed_durable_consumer(
    stream: &Stream,
    durable_name: &str,
    deliver_subject: &str,
    filter_subject: &str,
    deliver_policy: DeliverPolicy,
) -> Result<async_nats::jetstream::consumer::PushConsumer> {
    let reuse = match stream.consumer_info(durable_name).await {
        Ok(info) if consumer_config_matches(&info, deliver_subject, deliver_policy) => true,
        Ok(_) => {
            stream.delete_consumer(durable_name).await.map_err(|e| {
                PhotonError::caused(format!("nats delete consumer {durable_name}"), e)
            })?;
            false
        }
        Err(_) => false,
    };

    if reuse {
        return stream
            .get_consumer(durable_name)
            .await
            .map_err(|e| PhotonError::caused(format!("nats get consumer {durable_name}"), e));
    }

    let durable = durable_name.to_string();
    let config = push::Config {
        durable_name: Some(durable.clone()),
        name: Some(durable),
        deliver_subject: deliver_subject.to_string(),
        filter_subject: filter_subject.to_string(),
        deliver_policy,
        ack_policy: AckPolicy::Explicit,
        ..Default::default()
    };

    stream
        .create_consumer(config)
        .await
        .map_err(|e| PhotonError::caused(format!("nats create durable consumer {durable_name}"), e))
}

/// Build per-shard replay cursors for unkeyed sharded subscriptions.
#[must_use]
pub fn after_seq_by_shard(
    config: &NatsConfig,
    stored: &HashMap<u32, i64>,
    after_seq: Option<i64>,
) -> HashMap<u32, Option<i64>> {
    let mut out = HashMap::new();
    for shard in 0..config.stream_shards.max(1) {
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
