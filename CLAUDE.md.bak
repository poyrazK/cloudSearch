# CLAUDE.md

cloudSearch is a cloud-native search engine for infrastructure and SaaS teams, targeting the common 80% of Elasticsearch/OpenSearch workflows with a simpler operating model, safer defaults, and cleaner architecture for multi-tenant SaaS environments.

## Crates

| Crate | Purpose | Depends On |
|-------|---------|------------|
| `cloudsearch-common` | Shared types, errors, config, search models | — |
| `cloudsearch-storage` | WAL, segments, doc values, snapshots | `common` |
| `cloudsearch-index` | Index catalog, registry, query execution | `storage` |
| `cloudsearch-api` | HTTP API layer (Axum) | `index` |
| `cloudsearch-node` | Binary entry point, background workers | `api` |

**Dependency hierarchy**: `common` → `storage` → `index` → `api` → `node`

All work happens in `rust/`. Commands run from `rust/` unless noted otherwise.

## Key Architecture Decisions

### WAL-First Durability
- Append-only WAL with generations, sequence numbers, and CRC32C checksums
- Writes acknowledged after WAL sync (`sync_all()`)
- Replay skips already-flushed sequences using manifest checkpoint
- Trailing partial writes are truncated, never silently skipped

### Immutable Segments
- Segments are written once and never modified
- `manifest.json` tracks all active segments per index
- `IndexManifest` replaces single `current.json`
- Deletes are soft tombstones until merge

### Background Task Loops
- **Refresh**: 1s interval — publishes pending writes as searchable views
- **Flush**: 30s interval — WAL rollover + segment write + doc values
- **Merge**: 60s interval — consolidates small segments
- **Retention**: evicts expired documents based on TTL

### Dynamic Mapping
- Default mode: `ControlledDynamic`
- Field types inferred from documents (Null→skip, Bool→Boolean, Number→Integer/Long/Double, RFC3339 string→Timestamp, else→Keyword)
- Inferred mappings are persisted for stable future writes

## Build & Test

```bash
cd rust/

# Build
cargo build --workspace --all-targets

# Test
cargo test --workspace --all-targets

# Format check / auto-fix
cargo fmt --all --check
cargo fmt --all

# Lint (enforced in CI — pedantic, warnings as errors)
cargo clippy --workspace --all-targets -- -D warnings -D clippy::pedantic

# Coverage (outputs lcov.info)
cargo llvm-cov --workspace --all-targets --lcov --output-path lcov.info
```

## CI Requirements

CI runs on every PR to `main` and blocks on:
- `fmt` — formatting must be clean
- `clippy` — pedantic linting, warnings are errors
- `unit-tests` — all crate-level tests
- `integration-tests` — node restart test
- `coverage` — reports lcov.info as artifact

Do not bypass CI or merge your own PR.

## Critical Files

| Purpose | Path |
|---------|------|
| Binary entry point | `rust/crates/cloudsearch-node/src/main.rs` |
| Index lifecycle | `rust/crates/cloudsearch-index/src/` |
| WAL & storage | `rust/crates/cloudsearch-storage/src/` |
| API handlers | `rust/crates/cloudsearch-api/src/` |
| Common types | `rust/crates/cloudsearch-common/src/` |
| Development conventions for humans | `AGENTS.md` |

## Key Patterns to Preserve

- **Query string parser**: Hand-written recursive descent parser in `cloudsearch-index` (`field:value AND (tag:foo OR tag:bar)`)
- **Arc+Semaphore**: Background operations use `Arc<Semaphore>` for concurrency control
- **Environment config**: `CLOUDSEARCH_BIND`, `CLOUDSEARCH_DATA_DIR`, `CLOUDSEARCH_REFRESH_INTERVAL_SECS`, etc.
- **Columnar doc values**: Binary sidecar files with header + packed data per type (keyword offsets, integer/long packs, double packs, boolean bit arrays)

## What NOT to Change

- Dependency hierarchy (`common` → `storage` → `index` → `api` → `node`)
- WAL checksum algorithm (CRC32C)
- Segment immutability design
- Write acknowledgment contract (WAL sync before ack)

## Source of Truth

When behavior and docs conflict, prefer: **tests > code > docs**.

If docs are wrong, fix them.

## Reference

- Full development guide for humans: `AGENTS.md`
- Architecture: `docs/architecture.md`
- Storage (WAL, segments): `docs/storage-engine.md`, `docs/wal-format.md`
- Index lifecycle: `docs/index-lifecycle.md`
- API spec: `docs/api-v1.md`
- Node runtime: `docs/node-runtime.md`
- CI workflow: `docs/ci.md`
