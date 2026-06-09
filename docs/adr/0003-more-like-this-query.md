# ADR-0003: More Like This (MLT) Query Support

## Status

Accepted

## Context

cloudSearch needs a "find similar documents" feature to support use cases such as:
- Related article recommendations
- Duplicate detection
- Content discovery within an index

Users coming from Elasticsearch are familiar with the `more_like_this` query and expect similar functionality.

### Design Constraints

1. **No external ML library** — MLT should use classical IR techniques (TF*IDF), not neural embeddings
2. **Reference doc exclusion** — the source document must not appear in its own results
3. **Term selection** — not all terms are equally significant; stopwords and rare terms need filtering
4. **Field boosting** — terms from more important fields should contribute more to the score
5. **Positions data required** — significant term extraction requires inverted index positions, which only exist in flushed segments

### Alternatives Considered

**Option A: Return raw `MltQuery` node as-is to segment execution**
- Would require segment executors to understand MLT semantics
- Mixes query transformation logic with execution logic
- Harder to test in isolation

**Option B: Transform MLT at search level, pass BoolQuery to standard scoring**
- Keeps segment execution unchanged
- MLT becomes a query rewriting step before standard execution
- Easier to test and debug
- Follows the existing pattern of query transformation in cloudSearch

## Decision

We implement MLT as a query transformation step that converts `MltQuery` into a `BoolQuery` with boosted `should` clauses, then delegates to the existing scoring infrastructure.

### Term Significance Formula

```
significance = sqrt(term_freq_in_doc) * log((n_docs + 1) / (doc_freq + 1))
```

Where:
- `term_freq_in_doc` = occurrences of term in the reference document's field
- `n_docs` = total searchable documents
- `doc_freq` = number of documents containing the term

This balances:
- Higher frequency in reference → more significant
- Higher document frequency → less discriminative → lower weight

### Term Selection

Terms are filtered by:
- `min_term_freq`: minimum occurrences in reference doc (default: 2)
- `min_doc_freq`: minimum occurrences across index (default: 1)
- `max_query_terms`: cap total terms (default: 25)
- `min_word_length` / `max_word_length`: optional token length bounds

### Field Boosting

Each field gets a boost factor (default: 1.0). The per-term boost is:
```
term_boost = sqrt(term_freq_in_field) * field_boost_factor
```

### Source Document Exclusion

The source document is excluded at search level, not via a `must_not` clause:
```rust
if let Some(ref exclude_id) = mlt_doc_id_to_exclude && doc.id == *exclude_id {
    return None;
}
```

This is more reliable than `_id` field filtering because `_id` may not be indexed in positions readers.

### Limitation: Unflushed Segments

MLT requires positions data from the inverted index. Documents in the WAL or unflushed segments have no positions data, so MLT returns empty results for them. This is documented and deferred as future work (segment-level in-memory MLT).

## Consequences

### Positive
- Reuses existing BoolQuery scoring infrastructure
- Easy to test in isolation (transformation function has clear inputs/outputs)
- No changes to segment execution layer
- Field-level term tracking correctly associates terms with their source field

### Negative
- Flushed segments only — users must flush before using MLT
- Per-document term frequency is counted globally, not per-segment (minor overcounting for updated docs in multiple segments)

### Neutral
- MLT adds a new query variant to the public API (`MltQuery` in `cloudsearch-common`)
- Boost field added to `TermQuery` (non-breaking, optional, defaults to `None`)

## References

- Elasticsearch `more_like_this` query: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mltr-query.html
- BM25 scoring context: `docs/query-engine.md`
- Implementation: `cloudsearch-index/src/lib.rs` — `build_mlt_bool_query` function