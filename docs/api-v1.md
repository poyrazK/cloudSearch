# cloudSearch V1 API Scope

## Compatibility Goal

cloudSearch v1 targets roughly 80% Elasticsearch compatibility for the common index, ingest, and search workflow.

Compatibility is intentionally pragmatic:

- preserve familiar endpoint shapes where they help migration
- preserve familiar request and response structures for common paths
- avoid full parity for obscure edge cases and advanced features

## Supported Index APIs

- `PUT /{index}`
- `GET /{index}`
- `DELETE /{index}`
- `POST /{index}/_refresh`
- `POST /{index}/_flush`
- `POST /{index}/_merge`

### Expected V1 Semantics

- create index with settings and mappings
- fetch stored index metadata
- delete index and its local storage
- refresh to make recent writes searchable
- flush to persist the current searchable state to a durable segment snapshot
- merge to compact the segment by deduplicating overwrites and removing deletes

Current implementation notes:

- deleting an index evicts any cached in-memory handle and removes its local storage directory
- deleting a missing index returns `404`

## Supported Document APIs

- `POST /{index}/_doc`
- `POST /{index}/_bulk`

### Expected V1 Semantics

- index full documents
- support fast-by-default acknowledgements after WAL append
- support bulk ingest as a core workflow
- defer partial update semantics if they complicate v1 too much

## Supported Search API

- `POST /{index}/_search`

## Observability API

- `GET /_health`
- `GET /metrics`

Current implementation notes:

- `/metrics` exposes Prometheus-style counters and duration summaries for core API operations
- request counters include route, method, and status labels
- runtime gauges include the current number of open cached index handles

### Supported Query DSL In V1

- `match`
- `term`
- `terms`
- `range`
- `bool`
- `prefix`
- `sort`
- `from`
- `size`

Current implementation notes:

- `term`, `terms`, `range`, `bool`, and `prefix` are implemented
- `from`, `size`, and single-field sort are implemented
- the API now accepts both the internal request shape and a closer Elasticsearch-style shape for `term`, `terms`, `range`, `bool`, `prefix`, and single-entry sort arrays
- search responses now use Elasticsearch-style `_id`, `_source`, and `hits.total.value` / `relation` wrappers
- document GET responses use `_id` and `_source`, while bulk item responses use `_id` plus a `result` field
- search currently supports top-level fields only
- bulk format is simplified JSON, not Elasticsearch NDJSON

### Supported Aggregations In V1

- `terms`
- `date_histogram`
- `stats`

## Mapping Semantics

- default mapping mode is `controlled_dynamic`
- inferred mappings are persisted
- field conflicts return explicit errors
- arrays are rejected
- strings currently infer as `keyword`

## Time-Aware Behavior

- indexes may define a primary time field
- time-range queries may prune segments using stored min and max timestamp metadata
- future retention and rollover features build on the same metadata model

## Deferred Features

These are intentionally out of v1 scope:

- scripting
- plugin support
- exact Elasticsearch edge-case parity
- advanced ingest pipelines
- cross-cluster search
- advanced security and RBAC
- shard and replica behavior exposed to users
- full cluster-management APIs

## API Design Rule

If cloudSearch does not support a feature, it should fail clearly and document the gap. Silent acceptance with partial behavior is worse than explicit non-support.
