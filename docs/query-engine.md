# cloudSearch Query Engine

## Goals

The query engine should deliver Elasticsearch-like search behavior for common workloads without copying Elasticsearch internals too literally.

For v1, it should optimize for:

- common query DSL compatibility
- clear internal execution model
- strong filter and range performance
- acceptable full-text relevance for general use
- simple aggregation execution for common analytics

## Core Principle

The external request format may look like Elasticsearch, but the internal representation should be cleaner.

`cloudSearch` should parse supported JSON DSL into an internal query AST and execute that AST over segments.

This keeps compatibility at the boundary while protecting the core engine design.

## Execution Pipeline

Recommended query lifecycle:

1. parse request JSON
2. validate supported fields and options
3. translate into internal AST
4. rewrite or simplify the AST where useful
5. select candidate segments
6. prune segments using metadata such as time bounds
7. execute query and filters per segment
8. collect top hits and aggregation state
9. merge per-segment results
10. return Elasticsearch-like response shape

## Internal Query AST

The AST should be small and explicit in v1.

Suggested core nodes:

- `MatchQuery`
- `TermQuery`
- `TermsQuery`
- `RangeQuery`
- `BoolQuery`
- `PrefixQuery`
- `WildcardQuery`
- `MatchAllQuery`
- `MltQuery`

Supporting wrappers:

- `Filter`
- `SortSpec`
- `Pagination`
- `AggregationSpec`

The AST should be independent from HTTP and JSON details.

## Supported Query Semantics

### Match

- full-text query over analyzed text fields
- uses field analyzer rules from mapping metadata

Current implementation notes:

- tokenizes query and field values using whitespace splitting with lowercase normalization
- scoring uses term recall ratio: `matched_query_tokens / total_query_tokens`
- match requires at least one query token to be present in the field tokens
- case-insensitive matching via lowercase normalization
- operates on string values only; non-string fields return no matches

### Term And Terms

- exact match over keyword-like fields
- should also support numeric and boolean exact matching

### Range

- core feature for timestamps, numbers, and sortable fields
- must be efficient on time-oriented indexes

### Bool

- `must`
- `should`
- `filter`
- `must_not`

The engine should strongly distinguish scoring clauses from filter-only clauses.

Current implementation notes:

- `must`, `filter`, and `must_not` are enforced as boolean inclusion/exclusion checks
- `should` is required only when there are no `must` or `filter` clauses
- scoring is still effectively neutral; bool is currently used for logical composition only

### Prefix

- matches string field values that start with a given prefix
- useful for autocomplete and type-ahead patterns
- operates on string values only; non-string fields return no matches

Current implementation notes:

- prefix matching is case-sensitive
- empty prefix matches all documents with that field present

### Wildcard

- matches string field values against glob patterns with `*` (zero or more chars) and `?` (exactly one char)
- useful for flexible pattern matching and partial word searches
- operates on string values only; non-string fields return no matches

Current implementation notes:

- wildcard matching is case-sensitive
- patterns are converted to regex for matching: `*` → `.*`, `?` → `.`, special chars are escaped

### More Like This (MLT)

- finds documents similar to a reference document or raw JSON input
- useful for "find related articles", "recommendations", or "similar documents" features
- operates by extracting significant terms from the reference and building a boosted query

Current implementation notes:

- MLT takes either `doc_id` (reference document ID) or `like` (raw JSON object), not both
- significant terms are extracted using TF*IDF: `sqrt(tf) * log((n + 1) / (df + 1))`
- top `max_query_terms` terms become `should` clauses with per-field boosts
- source document is excluded from results at search level (not via query clause)
- MLT requires flushed segments to access positions data; unflushed documents return empty results
- term filtering respects `min_term_freq`, `min_doc_freq`, `min_word_length`, and `max_word_length`

## Filter-First Bias

Because `cloudSearch` is log and event first, filter execution quality matters as much as full-text relevance.

Design implications:

- filters should avoid unnecessary scoring work
- range filters should participate in early pruning
- exact-match filters should be cheap over doc values or postings-backed structures

## Segment-Level Execution

Each segment should execute queries locally and return partial results.

Local execution should produce:

- matching document ids
- local scores when relevant
- sort values for top hits
- intermediate aggregation state

The top-level engine then merges segment results into a single response.

## Scoring Model

V1 should keep scoring intentionally simple.

Recommended approach:

- BM25-style lexical scoring for `match`
- no scripting-based custom scoring in v1
- filters do not affect score unless wrapped by scored clauses

This is enough for general search without turning v1 into a relevance lab.

## Sorting And Pagination

V1 should support:

- score sort
- field sort for sortable mapped fields
- ascending and descending order
- `from` and `size`

Recommended constraint:

- keep deep pagination simple in v1
- avoid complex `search_after` and PIT semantics until later unless they become necessary early

## Aggregation Execution

V1 aggregation scope is intentionally small.

Supported early aggregations:

- `terms`
- `date_histogram`
- `stats`

Recommended model:

- per-segment collection first
- final reduction after segment execution completes
- doc values as the main aggregation substrate

This keeps aggregations predictable and aligned with the storage model.

## Time-Aware Query Optimizations

Time-awareness should shape query execution early.

Recommended behaviors:

- prune segments whose time bounds cannot match the query range
- prefer timestamp filters to run early in the filter pipeline
- keep date histogram execution close to segment-local collection logic

This is one of the key places where `cloudSearch` can feel optimized for infra workloads without exposing complexity.

## Error Handling And Compatibility

If a query uses an unsupported Elasticsearch feature, the engine should fail clearly.

Rules:

- reject unsupported clauses explicitly
- return useful error messages with the unsupported field or behavior
- do not silently ignore unsupported query sections

Predictability matters more than partial compatibility theater.

## Observability

The query engine should expose enough signals to debug behavior.

Minimum signals:

- end-to-end query latency
- per-segment execution time
- hit counts and top-k collection cost
- aggregation execution cost
- segment pruning effectiveness
- slow query logs

## V1 Non-Goals

The query engine should not aim to provide in the first version:

- scripting queries
- plugin-based queries
- vector search
- exact Elasticsearch scoring parity
- advanced rescoring pipelines
- cross-index distributed coordination

## Open Design Questions

These should be resolved during detailed design:

- exact AST shape and ownership model in Rust
- whether `match_phrase` belongs in the first compatibility wave
- whether aggregations land in phase 1 or phase 2 implementation-wise
- how much response-shape fidelity to preserve around hits metadata
