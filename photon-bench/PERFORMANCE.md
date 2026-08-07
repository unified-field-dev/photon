# Photon performance

Measured on AWS (`c6i.large` in-VPC primary row for fleet ingress). Photon is a realtime/event bus runtime with pluggable backends (in-process, SQLite, NATS, Kafka, Fluvio, and related topologies). Full PFH ladders come from AWS campaign runs.

## Capacity shape

Publish firehose (BM-PFH, `stream_seq` / sync_ack=1 primary row):

| Broker | Per embed | Aggregate (bc=4) |
|--------|-----------|------------------|
| NATS | ~94k ops/s | ~218k ops/s |
| Fluvio | ~93k ops/s | ~400k ops/s (~1.8× NATS) |
| Kafka (Photon + rskafka) | ~1.1k (N=1) | ~5k @ bc=2 (campaign paused) |

In-process and co-located brokers deliver the highest single-host rates. Split fleets (dedicated broker hosts + bench publishers) are the right model when quoting multi-tenant or multi-AZ deployments.

## Backend guidance

Use SQLite/in-process figures for embedded products. Prefer NATS or Fluvio for high ingress; Kafka on this Photon path stays in the low thousands ops/s. Prefer same-region AWS numbers for sizing; cross-region paths need their own measurement. Subscriber fanout at PFH-scale publish rates was not measured in this study.

## How to read these results

AWS-tagged reports are authoritative for capacity. Laptop smokes validate harness wiring only.
