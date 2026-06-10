# ADR 0004: Autocomplete Suggest Index

## Status
Accepted

## Date
2026-06-09

## Context

Users need fast autocomplete / as-you-type suggestions — returning completion candidates as the user types, before they finish a word. This is a read-heavy, latency-sensitive path (sub-10ms target). The feature should return term completions, not full documents.

Requirements:
- Sub-10ms latency for prefix lookup
- Multi-field support with per-field weights
- Score by term popularity across the corpus
- Completion suggestions (terms/phrases), not document results

## Decision

### Binary Serialization with Sorted Term Arrays

Each field's vocabulary is stored as a sorted `Vec<SuggestEntry>` (term, doc_freq, score) in a binary sidecar file (`suggest_{segment:020}.bin`). The format:

```
MAGIC (4) + VERSION (1) + PADDING (3) + FIELD_COUNT (4)
Per field: FIELD_NAME_LEN (4) + FIELD_NAME + TERM_COUNT (4)
Per term: STR_LEN (4) + TERM_BYTES + DOC_FREQ (4) + SCORE (4)
```

O(log n + m) prefix lookup via binary search (`find_first_prefix`) where n = vocabulary size, m = matching terms. No in-memory index build at query time.

### Atomic Writes via .tmp then Rename

Flush writes to `suggest_{seg:020}.tmp`, then atomically renames to `.bin`. Readers load from `.bin` only — never from `.tmp`. This guarantees readers always see consistent data.

### Per-Segment Sidecar Files

Each segment has its own suggest sidecar file. At query time, all segment sidecars are queried and results merged. This avoids rebuilding the entire suggest index on every flush — only the new segment's sidecar is written.

### doc_freq = Unique Document Count per Term

`doc_freq` counts the number of **unique documents** that contain each term in a field, not the number of token occurrences. This is the standard document-frequency semantics used by search engines.

### Segment Reader Management

- On `open_index`: all suggest sidecars loaded into `suggest_readers` Vec
- On `flush`: all suggest sidecars reloaded from manifest (not just the new one) to avoid reader accumulation
- On `merge`: all suggest sidecars reloaded after manifest update since old sidecars are invalidated

## Consequences

### Positive
- Sub-10ms lookup: binary search on sorted arrays is O(log n), no in-memory index build
- Atomic writes prevent readers from seeing partial data
- Per-segment sidecars mean flush only writes one new file, not the full vocabulary
- doc_freq semantics match standard IR practice

### Negative
- doc_freq is frozen at flush time — doesn't account for deletes or updates until next flush
- Each segment's suggest data is independent; cross-segment deduplication happens at query time (in-memory BTreeMap)
- Empty prefix returns all terms in lexical order (could be large); guarded to return empty instead

### Neutral
- The suggest sidecar is separate from the positions sidecar — two separate files per segment
- Segment readers are held in memory; memory usage grows with segment count × vocabulary size

## Alternatives Considered

### Alternative 1: In-Memory Trie
**Why rejected:** A trie would require rebuilding the entire suggest index on every flush. With large vocabularies this becomes expensive. The sorted-array binary search achieves the same O(log n + m) lookup while allowing per-segment incremental updates.

### Alternative 2: Generic B-Tree Index (e.g., RedBTree)
**Why rejected:** Adds a heavy dependency for a read-heavy, append-mostly workload. The binary serialized sorted arrays are simpler, have no runtime dependency, and serialize/deserialize cheaply.

### Alternative 3: Store suggest data inline in segment snapshot
**Why rejected:** Suggests are built during flush from the full document set; storing them inline in the segment snapshot would require re-reading all documents to rebuild suggests on every merge. Separate sidecar files allow incremental rebuilds from the merged document set only.