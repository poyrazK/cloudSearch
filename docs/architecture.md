# cloudSearch V1 Architecture

## High-Level Shape

cloudSearch v1 is a single-node Rust service with an Elasticsearch-compatible REST surface for common workflows.

The internal design separates compatibility concerns from engine concerns.

- compatibility API layer at the edge
- clean internal query and indexing model
- immutable segment-based storage engine
- WAL-backed recovery and near-real-time refresh

This keeps the hot path in Rust and avoids premature control-plane complexity.

## Service Boundaries

### Rust Data Plane

The Rust service owns:

- index creation and metadata
- mappings and inference
- analyzers and tokenization
- write pipeline
- query parsing and execution
- segment storage
- WAL / translog
- refresh, flush, merge, and recovery
- small aggregations
- Elasticsearch-compatible data APIs in v1

### Go Control Plane Later

Go is reserved for future platform services:

- cluster metadata service
- tenancy and project model
- quotas and policies
- orchestration and automation
- Kubernetes operator
- admin and control-plane APIs

The project should avoid embedding Rust through a heavy FFI request path. When services split later, they should use explicit contracts such as protobuf or gRPC.

## V1 Node Components

### API Layer

Responsibilities:

- accept Elasticsearch-like REST requests
- validate request shape
- translate supported APIs into internal operations
- return Elasticsearch-like response shapes where practical

The API layer should not define core engine semantics.

### Index Service

Responsibilities:

- create, open, close, and delete indexes
- persist index metadata
- manage mapping updates and inference results
- track settings, including time-aware configuration

### Write Pipeline

Responsibilities:

- validate incoming document
- apply controlled dynamic mapping rules
- analyze indexed fields
- append operation to WAL
- update in-memory indexing structures
- acknowledge write using fast-by-default semantics

Default write semantics:

- fast by default
- acknowledge after WAL append
- stronger durability modes can be added later

### Search Pipeline

Responsibilities:

- parse Elasticsearch-compatible DSL subset
- translate into internal query AST
- prune segments using metadata, especially time bounds
- execute filters, scoring, sorting, and aggregation collection
- merge segment-local results into final response

### Storage Engine

Responsibilities:

- immutable segment files
- term dictionary and postings
- stored `_source`
- doc values for sort, filter, and aggregations
- delete tombstones until merge

### Runtime

Responsibilities:

- refresh scheduling
- flush scheduling
- background merge management
- recovery on startup
- metrics, tracing, and slow-operation visibility

Current implementation notes:

- the API layer now maintains an in-process metrics state and exposes it through `/metrics`
- request counts and latency summaries are tracked at the API boundary
- index registry size is surfaced as an operational gauge

## Storage Model

cloudSearch uses a Lucene-style segment architecture.

### Why

- proven model for near-real-time search
- clear recovery semantics
- good fit for Elasticsearch-compatible behavior
- future-friendly for snapshots, replication, and time-based pruning

### Core Elements

- `WAL / translog` for durability and crash recovery
- `in-memory indexing buffer` for recent writes
- `refresh` to publish new searchable views
- `flush` to persist state more fully
- `immutable segments` for stable reads
- `background merge` to reduce segment fragmentation

### Deletes

Deletes should be soft tombstones until merge. This keeps write behavior simple and aligns with segment-based designs.

## Mapping Model

### Default Mode

`controlled_dynamic` is the default mapping mode.

This means:

- unknown fields may be inferred
- inference is conservative and persisted
- conflicting field types fail clearly
- mappings remain understandable after initial indexing

### Mapping Modes

- `strict`
- `controlled_dynamic`
- `template-guided` later

### String Field Heuristics

String fields should not automatically become `text + keyword` in all cases.

Default behavior should use small, explainable heuristics such as:

- token count
- string length
- field name hints like `id`, `status`, `title`, `message`, `email`
- optional templates supplied by users

The engine should persist the chosen field type after inference so future writes remain stable.

## Time-Aware Indexing

Time-awareness is a first-class capability in v1.

### Recommended Behavior

- an index may define a primary time field
- segments record min and max timestamp bounds
- time-range queries use those bounds for pruning
- metadata includes retention hooks for future automation

This keeps normal index semantics while allowing the engine to optimize for log and event workloads.

## Query Model

The external query DSL follows supported Elasticsearch request shapes.

Internally, the engine should translate requests into a cleaner AST.

### Supported Early Queries

- `match`
- `term`
- `terms`
- `range`
- `bool`
- sorting
- pagination with `from` and `size`
- small aggregations

### Supported Early Aggregations

- `terms`
- `date_histogram`
- `stats`

## Initial Compatibility Surface

Supported endpoints in early v1 should include:

- `PUT /{index}`
- `GET /{index}`
- `DELETE /{index}`
- `POST /{index}/_doc`
- `POST /{index}/_bulk`
- `POST /{index}/_search`
- `POST /{index}/_refresh`

Unsupported or deferred features should be documented explicitly rather than silently ignored.

## Observability

Operational simplicity requires observability from the beginning.

Minimum requirements:

- metrics for ingest, query latency, refresh, and merge activity
- structured logs
- tracing hooks
- slow query and slow bulk visibility later in v1
