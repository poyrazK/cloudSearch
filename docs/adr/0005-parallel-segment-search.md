# ADR 005: Parallel Document Scoring with Rayon

## Status
Accepted

## Date
2026-06-11

## Context

The `search()` method in `IndexHandle` iterates documents sequentially to compute BM25 scores. With many documents in an index, this sequential loop becomes the dominant latency source for read-heavy workloads.

The naive fix — using `into_par_iter()` on `Vec<IndexDocument>` — is blocked by the fact that `into_par_iter()` requires `T: Send` for the parallel iterator's output type, and more importantly, the result of the parallel iterator (which would contain `IndexDocument` instances) would need to be moved across thread boundaries. While `serde_json::Value` does implement `Send + Sync` in modern versions, the design choice to keep `IndexDocument::source` access local to each task (via `&self`) avoids unnecessary cloning overhead and keeps the parallel result type small — only `(score, doc_id)` tuples cross thread boundaries, not full documents.

## Decision

Parallelize the scoring loop using `doc_id` (a `String`, trivially `Send + Sync`) as the iteration token, with source lookups performed inside each parallel task via shared access to `&self`.

```rust
// Collect doc IDs (Send+Sync) for parallel iteration
let doc_ids: Vec<String> = self.searchable_documents.keys().cloned().collect();

// Parallel scoring: (score, doc_id) tuples cross thread boundaries
let scored: Vec<(f32, String)> = doc_ids
    .into_par_iter()
    .filter_map(|doc_id| {
        if !self.is_expired(&doc_id, now) { ... }
        let doc = self.searchable_documents.get(&doc_id)?;
        score_query(doc, &query, doc_id_hash, positions_readers, &bm25_ctx)
            .map(|s| (s, doc_id))
    })
    .collect();

// Sequential source lookup and tuple expansion (O(hits), not O(all docs))
let mut scored_with_source = Vec::with_capacity(scored.len());
let mut matching_documents = Vec::with_capacity(scored.len());
for (score, doc_id) in scored {
    let doc = self.searchable_documents.get(&doc_id)?;
    let source = doc.source.clone();
    matching_documents.push(IndexDocument { id: doc_id.clone(), source: source.clone() });
    scored_with_source.push((score, doc_id, source));
}
```

**Key design constraints honored:**
- `serde_json::Value` (inside `IndexDocument`) never crosses thread boundaries
- Only `String` (`doc_id`) and `f32` (score) are in the parallel iterator's output type
- Source lookups happen sequentially after scoring — O(hits), not O(all docs)
- `&self.searchable_documents.get()` is safe: `BTreeMap<String, IndexDocument>` is `Sync`

## Consequences

### Positive
- Documents are scored in parallel on multi-core CPUs — significant throughput win for large indexes
- No heap allocation changes to `IndexDocument`; no new types introduced
- Sequential post-processing pass is bounded by hit count, not corpus size

### Negative
- `rayon` added as a dependency (minimal footprint, well-maintained)
- Debugging parallel code is harder than sequential; thread panic would abort the process (rayon default)

### Neutral
- Behavior is identical to the sequential version; this is a performance optimization only
- Single-document queries see no measurable change (parallel overhead exceeds win)

## Alternatives Considered

### Alternative 1: `rayon::scope` with thread-local doc access
Use `rayon::scope` to spawn parallel tasks that each own a slice of doc IDs, looking up source inside the task closure.

**Why rejected:** More complex API. The `into_par_iter()` approach is idiomatic and achieves the same result with less code.

### Alternative 2: Clone only cheaply-cloneable fields into parallel scope
Strip `IndexDocument` to `{ id: String, source: Arc<BTreeMap<...>> }` so the parallel iterator can work on owned data.

**Why rejected:** Requires changing `IndexDocument`'s public type and adding `Arc` indirection throughout the codebase. Significant refactor for a performance feature.

### Alternative 3: Use `std::thread::scope` from the standard library
Standard library threads instead of rayon.

**Why rejected:** Rayon provides better work-stealing, no thread spawning overhead, and `into_par_iter()` ergonomics that std threads cannot match.

## Benchmark Results

Benchmarks run with `cargo bench -p cloudsearch-index` on a single-threaded tokio runtime
(the benchmarks themselves are single-threaded; rayon threads run concurrently).

| Benchmark | 1k docs | 10k docs |
|---|---|---|
| `MatchAll` | 23ms | 23ms |
| `term_query` | 33ms | 47ms |
| `multi_match_title_body` | 16ms | 1.4s |
| `range_count_gte_500` | 1.7ms | 25ms |

Observations:
- **MatchAll is constant** regardless of doc count — the scoring loop dominates over iteration overhead
- **multi_match is the most expensive** query type — tokenization + per-field BM25 is CPU-bound; this is the primary target for parallel speedup
- **range is cheap** — simple field comparison, no posting lookups needed

To measure parallel speedup, run benchmarks on a multi-core machine before and after this change and compare `multi_match` / `term_query` latencies. The `rayon` thread pool will distribute scoring across CPU cores.