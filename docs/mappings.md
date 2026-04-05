# cloudSearch Mapping Model

## Goals

The mapping system should make `cloudSearch` easier to use than Elasticsearch without making it vague or magical.

For v1, mappings should provide:

- safe onboarding for new indexes
- predictable field typing
- low accidental index bloat
- clear error behavior
- stable long-term schema behavior after inference

## Design Principle

The default mapping mode is `controlled_dynamic`.

That means:

- new fields may be inferred automatically
- inference rules are conservative and explainable
- every inferred field is persisted in index metadata
- type conflicts fail explicitly
- users can move toward stricter schemas over time

This is meant to be simpler than strict schema-only systems and safer than Elasticsearch's loose defaults.

## Mapping Modes

### `strict`

- all fields must be declared ahead of time
- unknown fields are rejected at ingest time
- best for highly controlled production schemas

### `controlled_dynamic`

- unknown fields may be added through inference
- the engine persists the chosen field mapping immediately
- later writes must follow the stored mapping
- this is the recommended default for v1

Current implementation notes:

- new top-level fields are inferred on write and persisted into `metadata.json`
- arrays are rejected rather than inferred
- inferred mappings survive reopen and restart

### `template_guided`

- dynamic inference is constrained by user-defined templates
- useful for teams that want convenience plus more control
- may arrive later in v1 or early v2

## Field Type Families

The first version should keep the field taxonomy small.

Recommended field families:

- `text`
- `keyword`
- `boolean`
- `integer`
- `long`
- `float`
- `double`
- `timestamp`
- `object`

Optional later additions:

- `date_nanos`
- `ip`
- `geo_point`
- `nested`
- `scaled_float`

The smaller the first field set, the easier it is to keep indexing and querying predictable.

## Default Inference Rules

Field inference must be stable, explicit, and easy to reason about.

### Boolean

- `true` and `false` map to `boolean`

### Integer And Long

- integer-sized numeric values map to `integer` when safe
- larger whole numbers map to `long`
- type widening should be conservative and explicit

### Float And Double

- decimal numbers map to `double` by default unless we later want a narrower numeric heuristic

Current implementation notes:

- integers fitting `i32` map to `integer`
- larger integers map to `long`
- decimals map to `double`

### Timestamp

- RFC3339 or configured timestamp formats may infer `timestamp`
- the primary time field may also be explicitly configured at index creation

Current implementation notes:

- RFC3339 strings are inferred as `timestamp`
- non-RFC3339 strings are inferred as `keyword`

### Object

- JSON objects infer `object`
- child fields are inferred recursively using the same rules

## String Field Strategy

String handling is the most important place to be better than Elasticsearch.

`cloudSearch` should not default every string to `text + keyword`.

### Recommended Heuristics

Use small, explainable rules based on:

- token count
- string length
- field name hints
- optional templates

Examples:

- fields like `id`, `status`, `level`, `email`, `service`, `host` -> `keyword`
- fields like `title`, `message`, `body`, `description` -> `text`
- very short single-token strings -> usually `keyword`
- longer multi-token strings -> usually `text`

### Dual Mapping

`text + keyword` should exist, but it should be selective.

Recommended use cases:

- fields that are likely to need both full-text search and exact aggregation
- explicit user mapping requests
- template-guided mappings later

The engine should avoid dual indexing by default when the benefit is weak.

Current implementation notes:

- v1 currently infers strings as `keyword`
- `text` semantics are not yet implemented in the engine

## Mapping Persistence

Once a field is inferred, the mapping must be persisted in index metadata.

This guarantees:

- later writes remain consistent
- query behavior remains stable
- users can inspect how fields were typed

Mapping persistence must happen as part of a controlled metadata update, not as a hidden side effect with unclear ordering.

## Conflict Handling

Field conflicts should fail early and clearly.

Examples of conflicts:

- `status` first inferred as `keyword`, later sent as object
- `latency` first inferred as numeric, later sent as text
- `timestamp` inferred as `timestamp`, later written with incompatible format

Rules:

- reject conflicting writes
- return the field name, existing type, incoming type, and likely fix
- never silently remap an existing field

Current implementation notes:

- object/scalar conflicts are rejected
- timestamp/string conflicts are rejected
- arrays are always rejected

## Nulls And Missing Fields

V1 should keep null behavior simple.

Recommended behavior:

- missing fields are ignored
- explicit `null` does not create a new mapping by itself
- explicit `null` does not overwrite the stored field type

Current implementation notes:

- `null` values are accepted but do not create or change mappings

This avoids noisy schema changes from sparse data.

## Dynamic Objects

Objects should be supported, but nested complexity should be limited in v1.

Recommended behavior:

- plain objects are flattened into path-like field mappings internally
- deeply nested dynamic structures should be monitored and limited
- `nested` semantics should be deferred until later

This keeps ingestion simple while avoiding one of the most complicated Elasticsearch behaviors too early.

## Mapping Templates

Templates are important for real workloads and should exist in the design even if implemented after the first ingest path.

Useful template cases:

- fields ending in `_id` -> `keyword`
- fields matching `*.message` -> `text`
- fields under `labels.*` -> `keyword`
- fields under `metrics.*` -> numeric

Templates are one of the best ways to keep dynamic mapping safe without forcing full strict schemas.

## Time-Aware Mapping Hooks

Indexes should be able to declare a primary time field.

Recommended behavior:

- allow explicit configuration during index creation
- validate that the field is mapped as `timestamp`
- use this metadata for pruning, retention, and future rollover logic

Current implementation notes:

- query validation rejects `date_histogram` on non-`timestamp` fields

The time field should be first-class in metadata, not rediscovered on every query.

## Operational Limits

To avoid mapping explosion, `cloudSearch` should have clear safeguards.

Recommended limits:

- maximum fields per index
- maximum object depth
- optional dynamic-field rate warnings
- explicit error when limits are exceeded

Current implementation notes:

- the current implementation enforces a maximum of `1000` mapped fields per index
- depth limits are not yet implemented

This is especially important for SaaS and log-oriented workloads.

## Observability

Mapping decisions should be visible.

Useful signals:

- count of inferred fields
- mapping conflict errors
- field type distribution per index
- rejected writes due to schema limits
- template match statistics later

## V1 Non-Goals

The mapping system should avoid these in the first version:

- automatic remapping of existing fields
- advanced analyzer-per-field matrix explosion
- nested-document semantics
- runtime fields
- dynamic scripts in mapping logic

## Open Design Questions

These should be resolved before implementation:

- exact string-field heuristics and thresholds
- whether decimals default to `double` only or preserve narrower types
- how mapping metadata updates are serialized safely under concurrent writes
- whether object flattening uses dotted paths internally from day one
