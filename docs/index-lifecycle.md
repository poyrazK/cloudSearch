# cloudSearch Index Lifecycle

## Goals

The index lifecycle defines how an index moves through creation, writes, search visibility, maintenance, shutdown, and recovery.

For v1, it should provide:

- a simple and explicit state model
- predictable write and read behavior
- clear separation between refresh and flush
- deterministic startup recovery
- safe delete semantics

## Core Principle

In `cloudSearch`, an index is the main user-facing unit, but internally it is a managed runtime object with storage, background work, and lifecycle state.

Users should experience a simple mental model:

- create index
- write documents
- search documents after refresh
- recover cleanly after restart
- delete index safely

The runtime should handle the more complex internal transitions.

## Index State Model

Recommended v1 states:

- `creating`
- `open`
- `recovering`
- `degraded`
- `closing`
- `deleted`
- `failed`

### `creating`

The index metadata is being initialized.

Allowed behavior:

- validate settings and mappings
- create storage directories
- initialize metadata and runtime handle

The index should not accept normal writes or searches yet.

### `open`

The normal active state.

Allowed behavior:

- accept writes
- accept search requests
- run refresh, flush, and merge work
- update mappings within supported rules

### `recovering`

The index is rebuilding runtime state after startup or internal repair.

Allowed behavior:

- load metadata and segments
- replay WAL records
- rebuild in-memory state

Search and write admission during recovery should be conservative in v1. It is acceptable to block both until recovery completes.

### `degraded`

The index is available, but background pressure or partial subsystem issues are affecting service quality.

Examples:

- merge backlog is excessive
- refresh is delayed
- storage pressure is high

This state should be observable but not catastrophic.

### `closing`

The index is being shut down or removed from the runtime registry.

Allowed behavior:

- stop accepting new work
- finish or abort safe background tasks
- persist metadata when needed

### `deleted`

The index is no longer part of the runtime and its storage has been removed or scheduled for removal.

### `failed`

The runtime cannot safely operate the index.

Examples:

- unrecoverable metadata corruption
- incompatible on-disk state
- critical storage errors

The system should surface this clearly rather than pretending the index is healthy.

## Lifecycle Transitions

Recommended normal flow:

1. `creating`
2. `open`
3. `closing`
4. `deleted`

Recommended recovery flow:

1. `recovering`
2. `open`

Exceptional flow:

- any state may transition to `failed` when correctness is at risk
- `open` may transition to `degraded` when operational quality drops
- `degraded` may return to `open` after pressure clears

## Creation Flow

Index creation should be explicit and durable.

Recommended steps:

1. validate index name
2. validate settings and mappings
3. assign internal index id
4. create index directory and initial metadata files
5. initialize empty WAL generation
6. register index in runtime
7. transition to `open`

Creation should fail atomically where possible. If initialization fails halfway, the runtime should clean up incomplete state rather than leaving a confusing partial index behind.

## Write Lifecycle

The write path should remain simple and fast by default.

Recommended steps per document or bulk item:

1. resolve target index in registry
2. verify index state is `open`
3. validate document against current mapping rules
4. infer mappings if allowed
5. append logical operation to WAL
6. apply document to in-memory indexing buffer
7. return acknowledgement

Important behavior:

- acknowledged does not mean immediately searchable
- search visibility arrives after refresh
- stronger durability modes can be added later without changing the state model

## Read Visibility Lifecycle

The read lifecycle is centered around refresh.

### Before Refresh

- document is durable to the WAL boundary
- document is not guaranteed to be searchable

### After Refresh

- document is included in the active searchable view
- query execution can see the document

### After Flush

- durable checkpoint is stronger
- older WAL generations may become trimmable

This distinction must remain extremely clear in docs, metrics, and code.

## Refresh Lifecycle

Refresh is a visibility transition, not a full persistence transition.

Recommended steps:

1. snapshot pending in-memory writes
2. build or publish a new searchable reader view
3. swap active search handle atomically
4. record refresh metrics and state

Refresh should avoid blocking ingestion longer than necessary.

## Flush Lifecycle

Flush advances the durable checkpoint.

Recommended steps:

1. ensure segment state needed for commit is persisted
2. write commit or checkpoint metadata
3. mark older WAL generations as safe to trim
4. update runtime accounting

Flush can be triggered by:

- time interval
- WAL growth
- memory pressure
- administrative request later

## Merge Lifecycle

Merge should be treated as a managed background transition for an open index.

Recommended steps:

1. choose merge candidates
2. create merged replacement segment
3. atomically publish new segment set
4. retire old segments
5. reclaim deleted-doc overhead

Merge must preserve search correctness throughout the transition.

## Delete Lifecycle

There are two kinds of delete behavior.

### Document Delete

- append delete operation to WAL
- mark tombstone in live index state
- exclude deleted doc from search results
- reclaim storage on merge later

### Index Delete

Recommended steps:

1. transition index to `closing`
2. reject new writes and searches
3. stop background workers safely
4. remove runtime registry entry
5. delete on-disk files
6. transition to `deleted`

If on-disk deletion fails, the runtime should report the failure clearly and avoid claiming full success.

## Recovery Lifecycle

Recovery is one of the most important lifecycle paths.

Recommended startup sequence for each index:

1. transition to `recovering`
2. load latest committed metadata
3. discover committed segment set
4. open durable searchable state
5. replay WAL records after last checkpoint
6. rebuild in-memory pending structures if needed
7. initialize background workers
8. transition to `open`

If recovery cannot guarantee correctness, the index should transition to `failed`.

## Mapping Updates Within Lifecycle

Mappings evolve during the open state, but only in controlled ways.

Recommended rule:

- mapping additions through controlled dynamic inference are allowed while `open`
- mapping conflicts fail the write, not the whole index
- mapping metadata updates must be serialized with index metadata changes safely

This prevents lifecycle confusion where writes change schema invisibly.

## Health And Lifecycle

Lifecycle state and health are related but not identical.

Examples:

- an index can be `open` but operationally `degraded`
- an index in `recovering` is not yet ready
- an index in `failed` is unhealthy by definition

The API should surface both when useful.

## Observability

Important lifecycle signals:

- index state transitions
- recovery duration
- refresh frequency and latency
- flush frequency and reason
- merge queue depth and duration
- delete counts and tombstone ratios
- open and failed index counts

Lifecycle events should be visible in logs and metrics from the beginning.

## V1 Simplifications

To keep the first implementation manageable:

- block writes and searches during recovery
- keep index open/close semantics simple
- avoid user-visible shard lifecycle states
- avoid hot reconfiguration of complicated index settings

These are good tradeoffs for correctness and clarity.

## Open Design Questions

These should be resolved before implementation:

- whether closing waits for in-flight searches or cancels them
- whether flush can run concurrently with refresh in phase 1
- how lifecycle state is persisted versus runtime-only
- whether index delete first moves files to a tombstone area before physical removal
