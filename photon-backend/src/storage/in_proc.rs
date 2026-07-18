//! In-process storage port — broadcast bus, replay buffer, checkpoints (no external deps).
//!
//! Default Mode 1 adapter. Events stay inside this process — do **not** use for multi-process
//! or fleet delivery (use a broker adapter; see
//! [Getting started → Mode 2](https://docs.rs/uf-photon/latest/photon/#mode-2--brokered-publisher--worker-binaries)).

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use futures::stream::Stream;
use serde_json::Value;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use super::partition::topic_filter_matches;
use super::port::{StorageCapabilities, StoragePort};
use crate::error::Result;
use crate::event::TransportCrypto;
use crate::models::Event;

const DEFAULT_REPLAY_BUFFER_SIZE: usize = 1000;

fn partition_key(topic_name: &str, topic_key: Option<&str>) -> String {
    format!("{}:{}", topic_name, topic_key.unwrap_or("__null__"))
}

fn replay_key(topic_name: &str, topic_key_filter: Option<&str>) -> String {
    partition_key(topic_name, topic_key_filter)
}

/// In-process storage for the `mem` adapter tier.
///
/// Single-process only: live fanout and a bounded replay buffer live in memory. Photon installs
/// this by default when you omit
/// [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port).
///
/// **When not to use:** multiple binaries or hosts must share a topic log — pick NATS/Kafka/Fluvio
/// (Mode 2) or `SQLite` for durable single-host Mode 1.
///
/// Getting started: [Mode 1](https://docs.rs/uf-photon/latest/photon/#mode-1--embedded-one-binary).
///
/// # Examples
///
/// ## Mode 1 host (publish + handlers)
///
/// One binary owns both publish and `#[subscribe]` dispatch via `start_executor`.
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};
/// use photon_core::JsonIdentityFactory;
/// use photon_runtime::Photon;
///
/// # fn main() -> photon_backend::Result<()> {
/// let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(
///     TransportCrypto::from_env()?,
/// ));
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
///
/// Default path (same mem port, no explicit `storage_port`):
/// `Photon::builder().auto_registry().build()?`.
pub struct InProcStoragePort {
    crypto: TransportCrypto,
    tx: broadcast::Sender<Event>,
    replay_buffer: Arc<RwLock<HashMap<String, VecDeque<Event>>>>,
    replay_buffer_size: usize,
    events: Arc<DashMap<String, Event>>,
    seq_counters: Arc<DashMap<String, i64>>,
    checkpoints: Arc<DashMap<String, i64>>,
    delivery_pins: Arc<DashMap<String, i64>>,
}

impl InProcStoragePort {
    /// Create a new in-process port with default replay buffer size.
    #[must_use]
    pub fn new(crypto: TransportCrypto) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            crypto,
            tx,
            replay_buffer: Arc::new(RwLock::new(HashMap::new())),
            replay_buffer_size: DEFAULT_REPLAY_BUFFER_SIZE,
            events: Arc::new(DashMap::new()),
            seq_counters: Arc::new(DashMap::new()),
            checkpoints: Arc::new(DashMap::new()),
            delivery_pins: Arc::new(DashMap::new()),
        }
    }

    fn checkpoint_key(sub: &str, topic: &str, topic_key: Option<&str>) -> String {
        format!("{sub}:{}:{}", topic, topic_key.unwrap_or("__null__"))
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn push_replay(&self, event: &Event) {
        let key = replay_key(&event.topic_name, event.topic_key.as_deref());
        let event = event.clone();
        let mut buf = self.replay_buffer.write().await;
        let queue = buf.entry(key).or_default();
        queue.push_back(event);
        while queue.len() > self.replay_buffer_size {
            queue.pop_front();
        }
    }

    fn next_seq(&self, topic_name: &str, topic_key: Option<&str>) -> i64 {
        let key = partition_key(topic_name, topic_key);
        let mut entry = self.seq_counters.entry(key).or_insert(0);
        *entry += 1;
        *entry
    }
}

#[async_trait]
impl StoragePort for InProcStoragePort {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::mem()
    }

    async fn append(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<Event> {
        let _ = self.crypto.encrypt(&actor_json, &payload_json)?;
        let seq = self.next_seq(topic_name, topic_key);
        let event = Event {
            event_id: Uuid::new_v4().to_string(),
            topic_name: topic_name.to_string(),
            topic_key: topic_key.map(String::from),
            seq,
            actor_json,
            payload_json,
            created_at: Utc::now(),
        };
        self.events
            .insert(event.event_id.clone(), event.clone());
        self.push_replay(&event).await;
        let _ = self.tx.send(event.clone());
        Ok(event)
    }

    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>> {
        let replay_key = replay_key(&topic_name, topic_key_filter.as_deref());
        let replay_buffer = Arc::clone(&self.replay_buffer);
        let mut live_rx = self.tx.subscribe();
        let topic = topic_name;
        let filter = topic_key_filter;
        let delivery_pins = Arc::clone(&self.delivery_pins);

        Box::pin(stream! {
            if let Some(seq) = after_seq {
                let buf = replay_buffer.read().await;
                if let Some(queue) = buf.get(&replay_key) {
                    for evt in queue {
                        if evt.seq > seq && topic_filter_matches(evt, &topic, filter.as_ref()) {
                            yield Ok(evt.clone());
                        }
                    }
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
                        // Live fanout dropped messages; catch up from in-memory replay buffer.
                        let pin_key = partition_key(&topic, filter.as_deref());
                        let after = delivery_pins
                            .get(&pin_key)
                            .map(|v| *v)
                            .or(after_seq)
                            .unwrap_or(0);
                        let buf = replay_buffer.read().await;
                        if let Some(queue) = buf.get(&replay_key) {
                            for evt in queue {
                                if evt.seq > after
                                    && topic_filter_matches(evt, &topic, filter.as_ref())
                                {
                                    let pk = partition_key(
                                        &evt.topic_name,
                                        evt.topic_key.as_deref(),
                                    );
                                    delivery_pins.insert(pk, evt.seq);
                                    yield Ok(evt.clone());
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    async fn get_event(&self, event_id: &str) -> Result<Option<Event>> {
        Ok(self.events.get(event_id).map(|e| e.clone()))
    }

    async fn load_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>> {
        let key = Self::checkpoint_key(subscription_name, topic_name, topic_key);
        Ok(self.checkpoints.get(&key).map(|v| *v))
    }

    async fn commit_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()> {
        let key = Self::checkpoint_key(subscription_name, topic_name, topic_key);
        self.checkpoints.insert(key, last_seq);
        Ok(())
    }

    async fn truncate_before(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        truncate_bound: i64,
    ) -> Result<u64> {
        let key = replay_key(topic_name, topic_key);
        let mut removed = 0u64;
        let mut buf = self.replay_buffer.write().await;
        if let Some(queue) = buf.get_mut(&key) {
            while queue
                .front()
                .is_some_and(|e| e.seq < truncate_bound)
            {
                if let Some(ev) = queue.pop_front() {
                    self.events.remove(&ev.event_id);
                    removed += 1;
                }
            }
        }
        drop(buf);
        Ok(removed)
    }

    async fn delivery_seq_pin(&self, topic_name: &str, topic_key: Option<&str>) -> Option<i64> {
        let key = partition_key(topic_name, topic_key);
        self.delivery_pins.get(&key).map(|v| *v)
    }
}
