# Contributing to Photon

## Documentation

When you change public API behavior, configuration defaults, or wiring steps:

1. Update rustdoc on the affected symbols (workspace enforces `missing_docs = "deny"`).
2. Update [`docs/configuration.md`](docs/configuration.md) when env vars, macro attributes, or builder options change (renders on [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/)).
3. Add or update a runnable example under [`photon/examples/`](photon/examples/) when introducing a new user-facing workflow.
4. Run the verification block in [`docs/VERIFICATION.md`](docs/VERIFICATION.md) before opening a PR.

### Style

- Organize facade docs by **task** (boot, topics, backend development), not reader personas.
- Put full code snippets on the item that owns the API; the facade documentation map links without duplicating.
- Use `# Contract` subsections on trait methods for semantics (empty input, monotonicity, no-op defaults).
- Backend adapter crate docs should link to their `*StoragePortBuilder` rustdoc; cross-cutting env vars stay in [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/index.html).

## Rust standards & lint policy

Workspace lints live in the root [`Cargo.toml`](Cargo.toml) (`[workspace.lints.*]`). CI runs:

```bash
CARGO_BUILD_JOBS=1 cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Enforced (restriction / correctness)

| Lint | Level | Intent |
|------|-------|--------|
| `clippy::unwrap_used` / `expect_used` | warn (CI deny) | No silent panics in library/runtime paths |
| `clippy::dbg_macro` | deny | No leftover debug macros |
| `clippy::print_stdout` | deny | Prefer `tracing` / OpsLog |
| `clippy::print_stderr` | warn | Prefer `tracing`; `ConsoleOpsLog` is the explicit stderr adapter |
| `clippy::todo` / `unimplemented` | deny | No placeholders in shipped code |
| `rust.missing_docs` | deny | Public API docs |

Pedantic + nursery are enabled at warn. Cast lints (`cast_*`) are **on** for library crates — use a local `#[allow(...)]` with a one-line reason when a cast is intentional.

### Allowed pedantic noise (workspace)

`must_use_candidate`, `similar_names`, `module_name_repetitions`, `used_underscore_binding`, `too_many_arguments`, `type_complexity`, `items_after_statements`, `significant_drop_tightening`, `too_many_lines`.

### Test / bench crates

`photon-e2e`, `photon-testkit`, and `photon-bench` declare their own `[lints.clippy]` tables (Cargo cannot override `workspace = true`) and **allow** `unwrap_used` / `expect_used`. `photon-bench` also allows `print_stdout` / `print_stderr` for CLI reports. Library crates use `#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]` for unit tests; integration tests under `tests/` carry a file-level allow when needed.

Do not re-add broad workspace `allow`s for restriction lints without discussion.

### Error handling

- **Libraries** (`photon-backend`, adapters, core): typed [`PhotonError`](photon-backend/src/error.rs) via `thiserror`. Prefer `PhotonError::caused` / `caused_error` / `persistence` so source chains survive; reserve `Internal(String)` / `PersistenceError(String)` for opaque messages with no underlying error.
- **Binaries / CLI / testkit**: `anyhow` with `.context()` at the edge is fine.

### OpsLog vs tracing (layered)

| Concern | Use |
|---------|-----|
| Counts, rates, alerts, DLQ rows, retention gauges | **OpsLog** (`record_counter` / `record_gauge` / `log_event`) — host-installable via `PhotonBuilder::ops_log` |
| Diagnostic narrative, spans, error breadcrumbs | **tracing** (`#[instrument]`, `warn!` / `debug!`) |

A failure usually warrants **both**: bump the OpsLog counter (the fact) and emit `tracing::warn!` with the error (the detail). Do not bridge OpsLog into tracing (no `TracingOpsLog`); leave `ConsoleOpsLog` as the explicit stderr adapter.

### Async model

- Tokio is the runtime; prefer `tokio::spawn` and async I/O.
- Use `tokio::sync::{Mutex, RwLock}` when a guard may be held across `.await`.
- `std::sync::{Mutex, RwLock}` is OK for short critical sections that never await (e.g. in-memory DLQ). Prefer poison recovery (`unwrap_or_else(PoisonError::into_inner)`) over `.unwrap()` on those locks.
- No `std::thread::spawn`, `spawn_blocking`, or `block_on` in library code unless there is a documented FFI/blocking escape hatch.

### Supply chain

Use **cargo-deny** (`deny.toml` + CI `deny` job) for advisories/licenses. A separate `cargo audit` job is not required while deny advisories stay enabled.

## Performance targets (mem tier)

Methodology and measured study numbers live in [`photon-bench/PERFORMANCE_STUDY.md`](photon-bench/PERFORMANCE_STUDY.md) and the JSON under `profiling/photon-bench/reports/` — do not copy snapshot tables into CONTRIBUTING or VERIFICATION.

CI smoke gate: `bench-smoke` requires `bm-p0` JSON `"pass": true`. For local Criterion compare (baselines stay under `target/`, not committed):

```bash
CARGO_BUILD_JOBS=1 cargo bench -p photon-backend --features runtime -- --save-baseline local
CARGO_BUILD_JOBS=1 cargo bench -p photon-backend --features runtime -- --baseline local
```

See [`photon-bench/EXPERIMENTS.md`](photon-bench/EXPERIMENTS.md#criterion-microbenches-bm-crit-).

## Coverage

CI `coverage` job remains non-blocking. Soft floor for the scoped mem-tier workspace slice (excludes `photon-e2e` / `photon-bench`) is set as `COVERAGE_FLOOR_PCT` in [`.github/workflows/ci.yml`](.github/workflows/ci.yml). Raise that env value deliberately as tests land — do not jump to 90% in one step. Do not commit coverage percentages or “last pass” result tables into docs.

## Verification

```bash
export CARGO_BUILD_JOBS=1
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo test -p uf-photon --doc --features runtime,mem
cargo test -p photon-runtime --doc --features runtime,mem
cargo test -p photon-macros --doc
cargo test -p photon-backend --doc --features runtime
cargo test -p photon-e2e
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

See [`.github/workflows/ci.yml`](.github/workflows/ci.yml) for the full CI matrix.
