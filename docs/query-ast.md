# cloudSearch Query AST

## Goals

The internal query AST is the contract between the Elasticsearch-compatible API layer and the Rust query engine.

For v1, it should provide:

- a small and explicit internal model
- independence from HTTP and raw JSON request shapes
- enough structure for rewrite and optimization
- a stable base for segment-level execution
- clear separation between scoring and filtering

## Core Principle

`cloudSearch` should never execute Elasticsearch JSON directly.

The request flow should be:

1. parse supported JSON DSL
2. validate against mappings and supported features
3. lower into internal AST
4. normalize and rewrite AST
5. execute normalized AST over segments

This keeps compatibility at the edge and engine semantics in Rust.

## High-Level Query Request Model

At the engine boundary, a search request should be represented as one logical object.

Suggested shape:

```rust
pub struct SearchRequest {
    pub target: QueryTarget,
    pub query: QueryExpr,
    pub sort: Vec<SortSpec>,
    pub pagination: Pagination,
    pub aggs: Vec<AggregationExpr>,
    pub source: SourceSpec,
}
```

This keeps the engine-facing request compact and predictable.

## Query Root

The root query type should stay intentionally small in v1.

Suggested shape:

```rust
pub enum QueryExpr {
    MatchAll,
    Match(MatchQuery),
    Term(TermQuery),
    Terms(TermsQuery),
    Range(RangeQuery),
    Bool(BoolQuery),
    Prefix(PrefixQuery),
    Wildcard(WildcardQuery),
    Mlt(MltQuery),
}
```

This is enough for the first compatibility wave and leaves room to add more nodes later.

## Core Query Nodes

### `MatchQuery`

Purpose:

- full-text search over analyzed fields

Suggested fields:

- target field
- raw query text
- operator mode later if needed
- analyzer override later if needed

Current implementation:

- `MatchQuery` uses whitespace tokenization with case-insensitive matching
- scoring is based on term recall: `matched_tokens / query_tokens`
- empty query returns no matches
- works on string fields; non-string fields return no matches

### `TermQuery`

Purpose:

- exact matching for keyword, numeric, boolean, and timestamp fields

Suggested fields:

- field
- normalized scalar value

### `TermsQuery`

Purpose:

- exact set membership match

Suggested fields:

- field
- list of normalized scalar values

### `RangeQuery`

Purpose:

- range filtering over timestamps and ordered scalar fields

Suggested fields:

- field
- lower bound
- upper bound
- inclusive or exclusive operators

### `BoolQuery`

Purpose:

- composition of scoring and filter logic

Suggested fields:

- `must`
- `should`
- `filter`
- `must_not`
- optional `minimum_should_match` later

### `PrefixQuery`

Purpose:

- prefix matching for autocomplete-style queries over string fields

Suggested fields:

- `field`
- `value` (the prefix string to match)

Prefix matching is useful for type-ahead and autocomplete patterns where you want to find all documents whose field value starts with a given string. It matches on string values only.

### `WildcardQuery`

Purpose:

- glob-style pattern matching for string fields using `*` (matches any sequence) and `?` (matches any single character)

Suggested fields:

- `field`
- `value` (the wildcard pattern)

Wildcard matching is useful for flexible pattern queries where `*` represents zero or more characters and `?` represents exactly one character. Case-sensitive matching applies.

### `MltQuery`

Purpose:

- find documents similar to a given reference document or raw JSON input
- extract significant terms from the reference using TF*IDF-style scoring
- build a boosted BoolQuery from the top terms

Suggested fields:

- `doc_id` — reference document ID to analyze (mutually exclusive with `like`)
- `like` — raw JSON object to analyze (mutually exclusive with `doc_id`)
- `fields` — list of fields to extract terms from
- `min_term_freq` — minimum term frequency in the reference document (default: 2)
- `min_doc_freq` — minimum document frequency in the index (default: 1)
- `max_query_terms` — maximum number of terms to include in the query (default: 25)
- `min_word_length` — minimum word length to consider (optional)
- `max_word_length` — maximum word length to consider (optional)
- `field_boost_factor` — boost multiplier per field (default: 1.0)

Current implementation notes:

- `MltQuery` is transformed into a `BoolQuery` with `should` clauses before execution
- term significance is computed using a simplified TF*IDF formula: `sqrt(tf) * log((n + 1) / (df + 1))`
- the reference document is excluded from results via search-level filtering
- MLT requires flushed segments to access positions data; unflushed documents return empty results

## Scalar Value Model

The AST should not carry raw JSON values everywhere.

Use a normalized internal scalar type.

Suggested shape:

```rust
pub enum ScalarValue {
    String(String),
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Timestamp(i64),
}
```

This reduces type ambiguity during execution.

## Field References

Field references should be resolved and validated during AST construction where possible.

Suggested shape:

```rust
pub struct FieldRef {
    pub path: String,
    pub field_type: FieldType,
}
```

This lets execution avoid repeatedly rediscovering field type information.

## Bool Semantics

`BoolQuery` is the most important structured node in v1.

Recommended semantic rules:

- `must` contributes to match logic and scoring
- `filter` contributes to match logic without scoring
- `must_not` excludes matches
- `should` contributes optional scoring unless no `must` or `filter` clauses exist

The AST should preserve this distinction explicitly.

Suggested shape:

```rust
pub struct BoolQuery {
    pub must: Vec<QueryExpr>,
    pub should: Vec<QueryExpr>,
    pub filter: Vec<QueryExpr>,
    pub must_not: Vec<QueryExpr>,
}
```

## Sort Model

Sort should be explicit rather than encoded as loosely typed maps deep inside execution.

Suggested shape:

```rust
pub struct SortSpec {
    pub field: SortField,
    pub order: SortOrder,
}

pub enum SortField {
    Score,
    Field(FieldRef),
}
```

V1 should keep sort semantics simple and reject unsupported sort cases clearly.

## Pagination Model

Suggested shape:

```rust
pub struct Pagination {
    pub from: usize,
    pub size: usize,
}
```

V1 can intentionally keep this simple and avoid `search_after` or point-in-time semantics.

## Aggregation Model

Aggregations should also use a typed internal model.

Suggested root:

```rust
pub enum AggregationExpr {
    Terms(TermsAggregation),
    DateHistogram(DateHistogramAggregation),
    Stats(StatsAggregation),
}
```

Suggested design rules:

- aggregation nodes are validated against field types during AST lowering
- aggregations should depend primarily on doc values
- v1 does not need a deeply nested aggregation tree if that slows delivery too much

If nested aggregations are deferred, the AST should still leave a path for them later.

## Source Retrieval Model

V1 should support simple source behavior.

Suggested shape:

```rust
pub enum SourceSpec {
    Full,
    Disabled,
}
```

Field-level source filtering can come later.

## AST Construction Phases

AST construction should happen in clear stages.

Recommended stages:

1. parse request JSON into compatibility-layer structs
2. validate supported fields and query kinds
3. resolve field references using index mappings
4. normalize scalar values into engine types
5. build AST nodes
6. run normalization and rewrite passes

This pipeline keeps parsing concerns separate from engine concerns.

## Normalization Rules

Before execution, the AST should be normalized into a simpler equivalent form where useful.

Good v1 normalization rules:

- flatten nested bool nodes where semantics are unchanged
- remove empty clauses
- collapse single-child bool wrappers
- rewrite trivial `terms` with one value into `term`
- normalize time ranges into a common bound representation

Normalization should never silently change observable query semantics.

## Rewrite Opportunities

Even in v1, a small rewrite layer is worth having.

Examples:

- promote timestamp range filters for early pruning
- mark exact-match filters as non-scoring execution paths
- precompute field capabilities needed during execution

This does not need to be fancy. It just needs to make execution clearer and cheaper.

## Validation Rules

The AST builder should reject invalid requests before execution starts.

Examples of validation errors:

- `match` query on a numeric-only field
- `range` query on a field that is not ordered
- `terms` query with incompatible mixed scalar types
- unsupported options present in otherwise supported query shapes

Validation errors should be explicit and user-facing.

## Segment Execution Contract

The AST should feed a simple execution contract.

Suggested contract:

- input: normalized `SearchRequest` plus segment reader
- output: segment-local hits, optional scores, sort values, and aggregation partials

Suggested result shape:

```rust
pub struct SegmentQueryResult {
    pub hits: Vec<ScoredDoc>,
    pub total_hits: u64,
    pub aggs: Vec<AggregationPartial>,
}
```

This keeps the merge layer decoupled from segment internals.

## Time-Aware AST Hooks

Because `cloudSearch` is time-oriented, the AST should make timestamp filters easy to identify.

Recommended approach:

- preserve normalized range metadata clearly
- allow the planner to extract time bounds from `filter` and `must` clauses
- keep this extraction deterministic and cheap

This helps segment pruning without introducing a separate query language.

## Error Philosophy

If the compatibility layer can parse a request but the engine does not support the semantics, the AST layer should fail clearly.

Do not:

- silently drop clauses
- coerce unsupported options into defaults without notice
- guess semantics that are not defined

## V1 Non-Goals

The AST should not attempt these in the first version:

- scripting expressions
- vector query nodes
- full nested-document query semantics
- advanced query planner cost modeling
- exact Elasticsearch internal query representation

## Open Design Questions

These should be resolved before implementation:

- whether `minimum_should_match` belongs in the first bool design
- whether source filtering belongs in v1 AST or only full source on/off
- how much query metadata should be embedded in AST versus planner state
- whether aggregation expressions should support sub-aggregations in phase 1 or wait until phase 2
