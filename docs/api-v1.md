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
- `PUT /{index}/_settings`

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
- per-index metrics include document count, pending operations, and last sequence number per index

## Snapshot API

- `POST /{index}/_snapshot/{name}` — create a named snapshot
- `GET /{index}/_snapshot/{name}` — get snapshot metadata
- `GET /{index}/_snapshot` — list all snapshots for an index
- `DELETE /{index}/_snapshot/{name}` — delete a named snapshot
- `POST /{index}/_snapshot/{name}/_restore` — restore from a named snapshot

### Snapshot Response Shape

```json
{
  "name": "weekly-backup",
  "created_at": "2024-04-13T10:00:00Z",
  "last_sequence_number": 1500,
  "document_count": 50000,
  "checksum": 3735928559
}
```

### List Snapshots Response Shape

```json
{
  "snapshots": [
    {
      "name": "backup-1",
      "created_at": "2024-04-10T10:00:00Z",
      "last_sequence_number": 1000,
      "document_count": 40000,
      "checksum": 1234567890
    }
  ]
}
```

Current implementation notes:

- snapshots store the current flush state (searchable documents at time of snapshot)
- snapshots are stored in `indexes/{index}/segments/snapshots/`
- each snapshot consists of a data file (`{name}.json`) and a metadata file (`{name}.meta.json`)
- checksum is CRC32C of the snapshot data for integrity verification
- restoring a snapshot overwrites the current searchable state
- pending operations (not yet flushed) are lost when restoring from snapshot

### Supported Query DSL In V1

- `match`
- `term`
- `terms`
- `range`
- `bool`
- `prefix`
- `wildcard`
- `sort`
- `from`
- `size`

Current implementation notes:

- `term`, `terms`, `range`, `bool`, `prefix`, `wildcard`, and `match` are implemented
- `from`, `size`, and single-field sort are implemented
- the API now accepts both the internal request shape and a closer Elasticsearch-style shape for `term`, `terms`, `range`, `bool`, `prefix`, `wildcard`, `match`, and single-entry sort arrays
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
- retention TTL: documents are automatically evicted after `retention_secs` seconds based on their `primary_time_field` timestamp
- eviction runs as a background task on a configurable interval
- documents without a timestamp field are never evicted

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
