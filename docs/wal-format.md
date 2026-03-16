# cloudSearch WAL Format

## Goals

The write-ahead log is the first durability boundary in `cloudSearch`.

For v1, the WAL design should provide:

- fast sequential appends
- deterministic crash recovery
- clear replay boundaries
- simple checkpointing and truncation rules
- a format that can evolve without breaking old data silently

## Role Of The WAL

The WAL exists to protect acknowledged writes before they are fully incorporated into durable segment checkpoints.

In v1, the default write contract is:

- validate request
- append logical operation to WAL
- acknowledge write
- make data searchable on refresh
- make data checkpoint-safe on flush

This means the WAL must capture enough information to rebuild index state after a crash.

## Design Principles

- append-only
- sequential writes
- logical records, not low-level storage diffs
- explicit checksums and versioning
- explicit checkpoint boundaries
- replay correctness over compactness

## File Organization

Recommended per-index layout:

```text
indexes/
  <index-id>/
    wal/
      000001.log
      000002.log
      CURRENT
      checkpoints/
        000001.chk
        000002.chk
```

### Generations

The WAL should be split into ordered generations.

Each generation:

- is append-only while active
- becomes immutable after rollover
- may be trimmed after a later flush confirms it is no longer needed

This makes recovery boundaries and cleanup much easier to reason about.

## Record Model

The WAL should store logical operations, not implementation-specific memory structures.

Recommended v1 record types:

- `IndexDocument`
- `DeleteDocument`
- `MappingUpdate`
- `FlushMarker`

Possible later additions:

- `BulkBoundary`
- `NoOp`
- `IndexSettingsUpdate`

## Common Record Header

Each record should begin with a fixed header.

Suggested fields:

- format version
- record type
- record length
- sequence number
- timestamp
- checksum

Why this matters:

- version allows evolution
- type allows replay dispatch
- length allows scanning forward safely
- sequence number gives ordering guarantees
- checksum detects torn or corrupted writes

## Sequence Numbers

Each index should have a monotonically increasing WAL sequence number.

Recommended behavior:

- assign sequence number before append
- preserve ordering across all record types in one index
- use sequence numbers during recovery to detect replay position and checkpoint coverage

We do not need distributed semantics yet, but local ordering must be unambiguous.

## Record Payloads

### `IndexDocument`

Should contain enough information to replay a logical index operation.

Recommended fields:

- external document id
- operation sequence number
- serialized source document
- mapping version or metadata reference
- optional routing placeholder for future compatibility

V1 should favor explicitness over compactness.

### `DeleteDocument`

Recommended fields:

- external document id
- operation sequence number
- optional prior version metadata later

Deletes should replay as tombstone application, not physical removal.

### `MappingUpdate`

Recommended fields:

- mapping version
- changed field definitions or full mapping delta
- reason such as explicit user update or dynamic inference

This ensures recovery can restore mapping decisions consistently with writes that depended on them.

### `FlushMarker`

Recommended fields:

- flush sequence number boundary
- checkpoint metadata reference
- timestamp

This is primarily useful for observability and recovery bookkeeping, even if durable checkpoint state also lives outside the WAL.

## Encoding Strategy

For v1, the format should be easy to debug and stable enough to evolve.

Recommended approach:

- binary record header
- binary or length-delimited payload
- checksum per record

Avoid designing an overly clever compact binary format too early.

One acceptable approach is:

- fixed-size header
- payload encoded with a stable schema format such as protobuf or a custom binary layout

The exact choice matters less than keeping it versioned and deterministic.

## Append Rules

The append path should follow strict rules.

Recommended sequence:

1. build logical record
2. assign next sequence number
3. encode header and payload
4. compute checksum
5. append bytes atomically to active generation
6. update in-memory append position
7. acknowledge write

Stronger sync behavior can be added later, but ordering should remain the same.

## Checkpoints

The WAL needs explicit durable checkpoint semantics.

Recommended checkpoint content:

- latest fully committed WAL generation
- latest committed sequence number
- active segment commit identifier
- mapping metadata version
- timestamp

Checkpoint rules:

- checkpoints are written during flush
- checkpoints represent the safe recovery floor
- older WAL generations before the checkpoint may be trimmed only after checkpoint success

## Replay Rules

Replay must be deterministic and conservative.

Recommended recovery algorithm:

1. load latest valid checkpoint
2. open committed segment state referenced by checkpoint
3. scan WAL generations from the checkpoint boundary forward
4. verify record headers and checksums
5. stop at first corrupt or partial trailing record in the active generation
6. replay valid records in sequence order
7. rebuild in-memory pending state and live tombstones

Important rule:

- trailing partial writes may be discarded if they are beyond the last valid checksum boundary

## Corruption Handling

The WAL format should make corruption visible and bounded.

Recommended v1 behavior:

- detect corruption using checksum and length validation
- if corruption is found in the active tail, truncate to last valid record boundary
- if corruption occurs before the safe replay floor, mark the index as failed or require repair

Do not silently skip corrupted middle records.

## Rollover Rules

The active WAL generation should roll over under simple conditions.

Suggested triggers:

- size threshold exceeded
- flush completed and active generation is large enough
- maintenance action later

Rollover should be explicit and observable in metrics and logs.

Current implementation notes:

- flush forces a WAL generation rollover in the current engine
- appends continue in the new active generation after rollover

## Trimming Rules

WAL trimming should happen only after a successful flush checkpoint proves records are no longer needed for recovery.

Recommended rule:

- trim whole generations only in v1

This keeps deletion simple and avoids tricky partial-file compaction logic.

Current implementation notes:

- only inactive generations are eligible for trimming
- a generation is trimmed only when its max sequence is covered by the flushed snapshot sequence
- the active generation is never trimmed

## Mapping Consistency

If document writes depend on dynamically inferred mappings, recovery must preserve the same mapping history.

Two safe models exist:

- write `MappingUpdate` records into the WAL
- or make mapping metadata updates part of the same flush/checkpoint boundary with strict ordering

My recommendation for v1: include explicit `MappingUpdate` WAL records when dynamic inference changes the schema.

## Observability

The WAL subsystem should expose:

- append rate
- append latency
- current generation size
- generations retained
- replay duration
- replayed record count
- checksum failures
- truncated tail events

This is critical for debugging recovery behavior.

## V1 Non-Goals

The WAL should not attempt these initially:

- compression-heavy record packing
- cross-index shared WAL
- replication transport built directly on WAL format
- random-access update semantics
- distributed consensus metadata in WAL

## Open Design Questions

These should be answered before implementation:

- exact binary header layout
- payload encoding choice
- whether checksums use CRC32C or another algorithm
- whether `FlushMarker` is necessary if checkpoint files are authoritative
- whether bulk requests should be represented as individual records or grouped with boundaries
