# Supply chain policy

Photon depends on Quark via crates.io as [`uf-quark`](https://crates.io/crates/uf-quark) (`quark = { package = "uf-quark", version = "…" }` in the workspace `Cargo.toml`). Rust imports remain `use quark::…`.

## Rules

1. **Prefer crates.io** for all dependencies.
2. **New Git dependencies** require:
   - An entry in [`deny.toml`](../deny.toml) `[sources].allow-git`
   - A short note in this file (why Git, which rev, migration plan)
3. **CI** runs `cargo deny check` on every PR (advisories, licenses, sources). This covers the same RustSec advisory intent as `cargo audit`; a separate audit job is not required while deny advisories stay enabled.
4. **Ignored advisories** in [`deny.toml`](../deny.toml) must cite the transitive crate and a removal trigger (e.g. bump `async-nats` off `rustls-webpki` 0.102).

## Security-sensitive configuration

Transport encryption keys and development opt-ins are documented under
[configuration.md](configuration.md#security-sensitive-variables).
