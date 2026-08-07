//! Minimal [`StoragePort`] stub: wrap [`InProcStoragePort`], override a couple of methods, and
//! delegate the rest — **Extending storage** (custom adapter sketch).
//!
//! ```bash
//! export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
//! cargo run -p uf-photon --example custom_storage_port_stub --features runtime,mem
//! ```
//!
//! Real adapters (`photon-backend-sqlite`, `-nats`, `-kafka`, `-fluvio`) implement every method
//! directly against their substrate. This sketch shows the **decorator** shape instead: wrap
//! any `Arc<dyn StoragePort>` and intercept only the calls you care about (validation, auditing)
//! without reimplementing persistence or transport crypto. See `photon-backend` `StoragePort`
//! rustdoc for the full contract and `embedded_mem` for the default mem wiring.

#![allow(missing_docs)]
#![allow(clippy::print_stderr)]

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::Stream;
use photon::storage::StorageCapabilities;
use photon::{
    topic, Event, InProcStoragePort, Photon, PhotonError, Result, StoragePort, TransportCrypto,
};
use serde_json::Value;

/// Decorator over any [`StoragePort`] adding topic-name validation and append auditing.
///
/// Delegates every method except [`Self::append`] (rejects blank topic names and counts writes)
/// — the minimal shape for a custom adapter that only needs to intercept publish.
struct AuditingStoragePort {
    inner: Arc<dyn StoragePort>,
    appends: AtomicUsize,
}

impl AuditingStoragePort {
    fn new(inner: Arc<dyn StoragePort>) -> Self {
        Self {
            inner,
            appends: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StoragePort for AuditingStoragePort {
    fn capabilities(&self) -> StorageCapabilities {
        self.inner.capabilities()
    }

    async fn append(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<Event> {
        if topic_name.trim().is_empty() {
            return Err(PhotonError::InvalidTopicName(
                "topic_name must not be blank".into(),
            ));
        }
        tracing::info!(topic = %topic_name, "auditing_port: append");
        let event = self
            .inner
            .append(topic_name, topic_key, actor_json, payload_json)
            .await?;
        self.appends.fetch_add(1, Ordering::SeqCst);
        Ok(event)
    }

    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>> {
        self.inner
            .subscribe(topic_name, topic_key_filter, after_seq)
    }

    async fn get_event(&self, event_id: &str) -> Result<Option<Event>> {
        self.inner.get_event(event_id).await
    }

    async fn list_by_topic(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<Event>> {
        self.inner
            .list_by_topic(topic_name, topic_key, after_seq, limit)
            .await
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<Event>> {
        self.inner.list_recent(limit).await
    }

    async fn load_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>> {
        self.inner
            .load_checkpoint(subscription_name, topic_name, topic_key)
            .await
    }

    async fn commit_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()> {
        self.inner
            .commit_checkpoint(subscription_name, topic_name, topic_key, last_seq)
            .await
    }

    async fn truncate_before(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        truncate_bound: i64,
    ) -> Result<u64> {
        self.inner
            .truncate_before(topic_name, topic_key, truncate_bound)
            .await
    }

    async fn delivery_seq_pin(&self, topic_name: &str, topic_key: Option<&str>) -> Option<i64> {
        self.inner.delivery_seq_pin(topic_name, topic_key).await
    }
}

#[topic(name = "examples.custom_stub_ping")]
pub struct StubPing {
    pub n: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let inner: Arc<dyn StoragePort> =
        Arc::new(InProcStoragePort::new(TransportCrypto::from_env()?));
    let port = Arc::new(AuditingStoragePort::new(Arc::clone(&inner)));

    // Fail-closed proof: a blank topic_name is rejected before it reaches the wrapped port.
    assert!(port
        .append("", None, serde_json::json!({}), serde_json::json!({}))
        .await
        .is_err());

    let photon = Photon::builder()
        .storage_port(port.clone() as Arc<dyn StoragePort>)
        .auto_registry()
        .build()?;

    let event_id = StubPing { n: 1 }.publish_on(&photon).await?;
    let appends = port.appends.load(Ordering::SeqCst);
    assert!(appends >= 1, "expected at least one audited append");

    eprintln!(
        "custom_storage_port_stub: blank topic_name rejected; published {event_id} through AuditingStoragePort (appends={appends})"
    );
    Ok(())
}
