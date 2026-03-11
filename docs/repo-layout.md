# cloudSearch Repository Layout

## Goals

The repository should stay easy to navigate while the project grows from a single-node engine into a broader search platform.

The layout should:

- keep the Rust engine modular
- separate stable interfaces from implementation details
- leave room for future Go control-plane services
- support documentation and benchmarks as first-class assets

## Recommended Top-Level Structure

```text
cloudSearch/
  README.md
  docs/
  rust/
    Cargo.toml
    crates/
  go/
    go.work
    services/
  proto/
  benches/
  scripts/
```

This keeps the engine and platform layers visually separate without splitting into multiple repos too early.

## Rust Layout

Recommended crate structure:

```text
rust/
  Cargo.toml
  crates/
    cloudsearch-common/
    cloudsearch-api/
    cloudsearch-mappings/
    cloudsearch-query/
    cloudsearch-storage/
    cloudsearch-index/
    cloudsearch-runtime/
    cloudsearch-node/
```

### `cloudsearch-common`

- shared types
- error model
- ids and metadata primitives
- small utility code used across engine crates

### `cloudsearch-api`

- HTTP routing
- Elasticsearch-compatible request parsing
- response rendering
- request validation at the API edge

### `cloudsearch-mappings`

- field definitions
- inference logic
- mapping validation
- template matching later

### `cloudsearch-query`

- internal AST
- query rewrite
- scoring logic
- aggregation planning and reduction

### `cloudsearch-storage`

- WAL / translog
- segment readers and writers
- commit/checkpoint logic
- low-level file formats

### `cloudsearch-index`

- index metadata
- write pipeline orchestration
- refresh and flush boundaries
- search entrypoints over local index state

### `cloudsearch-runtime`

- node lifecycle
- worker scheduling
- caches
- health and metrics plumbing

### `cloudsearch-node`

- main binary
- configuration loading
- dependency wiring
- process startup and shutdown handling

## Go Layout

Go services should arrive later, but the repository can reserve the space early.

Recommended structure:

```text
go/
  go.work
  services/
    cloudsearch-controller/
    cloudsearch-gateway/
    cloudsearch-operator/
```

### `cloudsearch-controller`

- cluster metadata workflows later
- tenant and quota orchestration later
- administrative lifecycle operations

### `cloudsearch-gateway`

- future multi-tenant front door
- auth and policy integration later
- request routing once multiple nodes exist

### `cloudsearch-operator`

- Kubernetes deployment automation
- lifecycle and rolling upgrade workflows

## Proto Directory

`proto/` should hold contracts shared across Rust and Go when cross-service boundaries appear.

Do not expose internal storage details here. Only place service-level contracts in this directory.

## Benches And Scripts

### `benches/`

- reproducible ingest benchmarks
- query latency benchmarks
- merge stress scenarios

### `scripts/`

- local development helpers
- benchmark runners
- fixture generation

Keep scripts thin and non-magical.

## Documentation Rule

Architecture docs should live in `docs/` and remain close to implementation milestones.

When a major subsystem is implemented, the corresponding doc should be updated rather than abandoned.

## Evolution Strategy

Do not over-split crates too early.

If phase 1 moves faster with fewer crates, that is acceptable. The structure above is the target modular shape, not a rule that must create ceremony before code exists.

## Open Design Questions

These should be revisited once implementation begins:

- whether `cloudsearch-index` and `cloudsearch-storage` should start merged
- whether `cloudsearch-api` and `cloudsearch-node` should remain separate that early
- when to introduce `proto/` in practice rather than only in design
