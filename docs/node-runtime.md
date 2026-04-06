# cloudSearch Node Runtime

## Goals

The node runtime is responsible for making the engine operable, not just functional.

For v1, it should provide:

- a clear service lifecycle
- predictable background work scheduling
- safe startup and recovery
- observability for ingest and query behavior
- a path toward multi-index and multi-tenant controls later

## Runtime Scope

In v1, `cloudSearch` runs as a single Rust node process.

That process owns:

- HTTP API serving
- index registry and metadata loading
- write and search execution pipelines
- refresh, flush, and merge scheduling
- recovery logic
- caches and resource accounting
- metrics and tracing export

## Lifecycle

Recommended node lifecycle:

1. load configuration
2. initialize logging, metrics, and tracing
3. discover local indexes
4. load committed metadata and segments
5. replay WAL state
6. initialize schedulers and background workers
7. publish ready state and accept traffic

Shutdown should be graceful where possible:

- stop new request intake
- drain or reject in-flight writes according to policy
- checkpoint safe metadata when practical
- flush telemetry

## Index Registry

The runtime should maintain an in-memory registry of local indexes.

Responsibilities:

- open and close indexes
- map index names to runtime handles
- expose index health and metadata
- coordinate background tasks per index

The registry is also the right place to introduce future ownership metadata for tenants or namespaces.

Current implementation notes:

- the in-process runtime now owns an `IndexRegistry` that caches opened `IndexHandle`s by name
- the API layer delegates index handle lookup and lifecycle operations to that registry

## Background Workers

The first version should keep worker types minimal and explicit.

Recommended workers:

- refresh worker
- flush worker
- merge worker
- recovery or maintenance worker

Each worker should have clear responsibilities and visible metrics.

## Refresh Scheduler

Refresh should run on a fixed default interval with manual override support.

Responsibilities:

- publish searchable views from pending writes
- keep near-real-time semantics understandable
- expose refresh latency and frequency metrics

Refresh should never be confused with flush in runtime APIs or logs.

Current implementation notes:

- the node now runs a simple background refresh loop over cached/open indexes
- the default refresh interval is `1s`
- manual `/_refresh` remains supported alongside background refresh

## Flush Scheduler

Flush establishes more durable checkpoints and helps control WAL growth.

Possible triggers:

- elapsed time
- WAL size threshold
- memory pressure
- administrative command later

The runtime should make flush reasons observable.

Current implementation notes:

- the node now runs a simple background flush loop over cached/open indexes
- the default flush interval is `30s`
- manual `/_flush` remains supported alongside background flush

## Merge Worker

Merge is the most important background activity to control carefully.

Recommended v1 behavior:

- limited concurrent merges
- explicit queueing
- backpressure signals when merge debt grows
- visibility into segment counts and merge timings

The runtime should prefer stability over aggressive background optimization.

## Caching

V1 should introduce only a few caches.

Recommended early caches:

- segment reader cache
- field metadata cache
- optional query-result cache later if justified

Avoid adding many cache layers before the engine has real profiling data.

## Resource Accounting

Even before multi-tenancy is exposed, the runtime should collect internal resource usage.

At minimum track per index:

- document count
- segment count
- WAL bytes
- segment bytes
- refresh frequency
- merge backlog
- query counts and latency
- ingest rates

This provides the base for future quotas and noisy-neighbor controls.

## Failure Handling

The runtime should fail clearly and recover deterministically.

Key rules:

- startup should refuse corrupted metadata it cannot reason about
- WAL replay should be explicit and logged
- background worker failures should surface in health signals
- index-level failures should not necessarily crash the whole node if isolation is possible

## Health Model

The node should expose a simple health model in v1.

Suggested states:

- `starting`
- `ready`
- `degraded`
- `recovering`
- `failed`

Health should reflect real operational conditions such as recovery progress, severe merge backlog, or unrecoverable index errors.

## Configuration Philosophy

The runtime should be opinionated.

Recommended approach:

- small set of top-level config values
- clear defaults for refresh, flush, and merge behavior
- documented limits rather than dozens of hidden knobs

If we add configuration, it should be because the operational value is obvious.

## Observability

The runtime should make background behavior easy to inspect.

Minimum visibility:

- startup and recovery logs
- per-index lifecycle events
- refresh, flush, and merge timings
- request throughput and latency
- worker queue depth
- error counters by subsystem

Current implementation notes:

- the API exposes `GET /metrics` with Prometheus-style text output
- current counters include writes, bulk requests and operations, searches, refreshes, flushes, delete-index calls, request totals, and request duration sum/count
- the metrics endpoint also reports the current number of cached open indexes

## Future Evolution Hooks

The runtime should be designed so that later we can add:

- multiple indexes with stronger isolation
- node roles
- distributed coordination
- tenant-aware scheduling
- quota enforcement
- remote snapshot workflows

V1 should not implement those yet, but it should avoid blocking them.

## Open Design Questions

These should be resolved before implementation:

- whether each index gets dedicated worker state or shared workers
- how runtime config is represented and reloaded
- whether query cancellation is needed in phase 1
- what health signals block readiness
