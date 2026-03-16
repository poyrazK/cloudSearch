# cloudSearch Storage Engine

## Goals

The storage engine is the heart of `cloudSearch`.

For v1, it should optimize for:

- predictable ingest behavior
- near-real-time search visibility
- simple crash recovery
- efficient time-range pruning
- understandable on-disk layout
- future compatibility with replication and snapshots

## Core Model

cloudSearch uses a Lucene-style immutable segment architecture.

Writes flow through three stages:

1. append operation to WAL
2. update in-memory indexing buffer
3. publish searchable state on refresh, then persist stable segment files

This keeps the write path fast while preserving a clean read path.

## Main Components

### WAL / Translog

The WAL is the durability boundary for v1.

Responsibilities:

- append index and delete operations sequentially
- support crash recovery before in-memory state is rebuilt
- give v1 a clear fast-by-default acknowledgement point

Recommended v1 behavior:

- acknowledge writes after WAL append
- optionally add stronger fsync policies later
- rotate WAL generations after successful flush checkpoints

Each WAL record should contain enough information to replay the logical write operation deterministically.

## In-Memory Indexing Buffer

The in-memory buffer holds recent writes before they become part of a refreshed searchable view.

Responsibilities:

- accept analyzed field output
- assign internal document ids for the pending batch
- track pending deletes and updates
- prepare segment-friendly structures for refresh

This buffer should remain implementation-private. Users should only observe refresh behavior, not internal batch boundaries.

## Segment Model

Segments are immutable read-optimized units.

Each segment should contain:

- segment metadata
- term dictionary
- postings lists
- stored `_source`
- doc values
- deleted-doc bitmap or tombstone map
- optional per-segment statistics
- min and max timestamp bounds when a time field exists

### Why Immutable Segments

- simpler concurrent reads
- simpler crash model
- easier future snapshots
- easier merge behavior
- better fit for near-real-time search

## Proposed On-Disk Logical Layout

Each index should have its own local directory.

Example structure:

```text
indexes/
  <index-id>/
    metadata.json
    wal/
      000001.log
      000002.log
    segments/
      seg_000001/
        segment.meta
        terms.dat
        postings.dat
        stored_fields.dat
        doc_values.dat
        deletes.dat
```

The exact file format can evolve, but the logical separation should remain clear.

## Refresh

Refresh is the operation that makes recent writes searchable.

Recommended v1 behavior:

- auto refresh on a sane interval
- manual refresh API for tests and explicit control
- refresh publishes a new searcher view without requiring a full durable checkpoint

Refresh should be cheaper than flush. It is about visibility, not full persistence.

## Flush

Flush establishes a more durable checkpoint and allows WAL truncation or generation rollover.

Recommended v1 responsibilities:

- persist stable segment state
- write a commit marker or checkpoint
- mark older WAL generations as recoverable/trimmable

Current implementation notes:

- flush writes a simple searchable segment snapshot to `segments/current.json`
- flush forces a WAL generation rollover
- inactive WAL generations fully covered by the flushed sequence are trimmed

Flush should run less frequently than refresh.

## Merge Policy

Merge is where simple search engines become hard. `cloudSearch` should keep the first merge policy boring and observable.

V1 merge goals:

- reduce many small segments into fewer larger ones
- clean up deleted docs
- keep read amplification manageable
- avoid starving ingest under sustained load

Recommended v1 merge rules:

- size-tiered merging
- conservative concurrency limits
- merge backpressure visible in metrics
- avoid too many tuning knobs early

## Deletes And Updates

Deletes should be soft until merge.

Recommended v1 model:

- delete marks a tombstone for the matching internal or external id
- search ignores deleted docs
- merge compacts them away

For updates, the simplest model is:

- treat update as delete plus reindex internally

If partial update semantics are deferred, the storage layer should still be designed to support rewrite-based updates later.

## Recovery Model

On startup, recovery should be deterministic and easy to reason about.

Recovery sequence:

1. load latest committed index metadata
2. discover flushed segments
3. open durable searchable state
4. replay uncommitted WAL records
5. rebuild in-memory pending state
6. publish recovered view

Current implementation notes:

- recovery loads the flushed segment snapshot first
- WAL replay then starts after the flushed sequence boundary

Recovery correctness is more important than startup speed in the first version.

## Time-Aware Storage Hooks

Because `cloudSearch` is log and event first, time-awareness should exist in the storage layer, not only in query parsing.

Each segment should record:

- min timestamp
- max timestamp
- document count
- optional rough field stats later

This enables cheap pruning for time-range queries.

## Resource And Cost Controls

The storage engine should expose enough internal accounting for future multi-tenant controls.

At minimum track:

- bytes written to WAL
- bytes written to segments
- segment counts per index
- merge work queued and completed
- deleted-doc ratios
- refresh and flush timings

These metrics matter both for operations and for future quota design.

## V1 Non-Goals

The storage engine should not attempt these in the first version:

- object storage as primary persistence
- advanced compression tuning matrix
- pluggable storage backends
- tiered storage lifecycle automation
- segment replication protocol
- zero-copy remote snapshots

## Open Design Questions

These should be answered before implementation starts:

- exact WAL record format
- exact commit/checkpoint format
- internal doc id assignment strategy
- whether refresh writes mini-segments directly or publishes in-memory readers first
- whether metadata lives in JSON first or a binary format from day one
