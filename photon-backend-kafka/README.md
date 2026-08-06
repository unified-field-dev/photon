# photon-backend-kafka

Apache Kafka [`StoragePort`](../photon-backend/src/storage/port.rs) adapter.

Enable on the public crate with `features = ["runtime", "kafka"]`. Topology:
[Brokered](https://docs.rs/uf-photon/latest/photon/#brokered-publisher--worker-binaries).
Teach the brokered path once with NATS examples (`nats_worker` / `nats_publisher` in
[`photon/README.md`](../photon/README.md#how-to-run-examples)); swap this builder for Kafka.

Configuration: [`KafkaStoragePortBuilder`](https://docs.rs/photon-backend-kafka/latest/photon_backend_kafka/struct.KafkaStoragePortBuilder.html) (options + example). Index: [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/#storage-adapter-builders).

Fleet runbook: `$UF_LAB_ROOT/photon/infra/aws/kafka-fleet/README.md`.
