# photon-backend-sqlite

Embedded SQLite [`StoragePort`](https://docs.rs/photon-backend/latest/photon_backend/trait.StoragePort.html) for durable single-process Photon.

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

Run on AWS only (see [`infra/aws/sqlite-smoke/README.md`](../infra/aws/sqlite-smoke/README.md)):

```bash
~/aws/photon-upstream/sqlite-smoke/run-remote-smoke.sh
```
