# cloudSearch Roadmap

## Phase 0 - Foundation And Docs

- lock product thesis and design principles
- define v1 scope and non-goals
- define module boundaries
- document compatibility strategy

## Phase 1 - Single-Index Engine Prototype

- basic Rust project skeleton
- index metadata and mappings
- WAL / translog
- in-memory indexing buffer
- refresh into searchable segment views
- document indexing and retrieval basics
- query DSL subset: `match`, `term`, `range`, `bool`, `prefix`, `wildcard`

Exit criteria:

- create an index
- insert documents
- search documents
- recover from restart without data loss beyond chosen durability guarantees

## Phase 2 - Production-Grade Single-Node Engine

- bulk ingestion
- immutable on-disk segments
- stored source and doc values
- sorting and pagination
- aggregations: `terms`, `date_histogram`, `stats`
- merge policy and background workers
- metrics and tracing
- clearer mapping inference and conflict handling
- retention / TTL

Exit criteria:

- stable ingest and query behavior on realistic datasets
- understandable operational metrics
- predictable crash recovery

## Phase 3 - Multi-Index Node Runtime

- multiple indexes per node ✓
- better resource accounting ✓ (per-index metrics in /metrics endpoint)
- namespace-ready metadata ✓
- retention hooks and time-aware management ✓ (TTL-based document eviction)
- snapshot interfaces and backup design

Exit criteria:

- one node can host multiple indexes predictably
- metadata model is ready for future tenant attachment

## Phase 4 - Distributed Search Cluster

- node abstraction
- coordinator role
- shard and replica internals
- cluster metadata and placement
- recovery and rebalance workflows

Exit criteria:

- multi-node read and write path works
- shard ownership and recovery are understandable and observable

## Phase 5 - SaaS-Ready Platform Layer

- Go control-plane services
- tenant and project model
- auth and policy hooks
- quotas and usage accounting
- Kubernetes operator and lifecycle automation

Exit criteria:

- search engine can be embedded into a broader SaaS platform model
- tenancy can wrap indexes without redesigning the engine core

## Scope Discipline

The roadmap only works if we protect v1 from feature sprawl.

Rules:

- do not chase full Elasticsearch parity
- do not expose shard-heavy user experience early
- do not build hosted-service features before the engine is proven
- do not add advanced features before observability and recovery are solid
