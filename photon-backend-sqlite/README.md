# photon-backend-sqlite

Embedded SQLite [`StoragePort`](https://docs.rs/photon-backend/latest/photon_backend/trait.StoragePort.html) for durable single-process Photon.

Topology: [Embedded](https://docs.rs/uf-photon/latest/photon/#embedded-one-binary).
Runnable: `cargo run -p uf-photon --example embedded_sqlite --features runtime,sqlite`
(see [`photon/README.md`](../photon/README.md#how-to-run-examples)).

## Wiring

```rust
let port = photon_backend_sqlite::SqliteStoragePort::open("/var/lib/photon/events.db").await?;
Photon::builder().storage_port(Arc::new(port)).auto_registry().build()?;
```

## Environment

| Variable | Purpose |
|----------|---------|
| `PHOTON_SQLITE_PATH` | Database file path (default: temp file per testkit session) |

## Validation

Run on AWS only (see `$UF_LAB_ROOT/photon/infra/aws/sqlite-smoke/README.md`):

```bash
$UF_LAB_ROOT/photon/infra/aws/sqlite-smoke/
```
