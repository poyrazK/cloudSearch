# cloudSearch

cloudSearch is a cloud-native search engine for infra and SaaS teams.

The project goal is not to clone Elasticsearch feature-for-feature. The goal is to deliver the common 80% of Elasticsearch and OpenSearch workflows with a simpler operating model, safer defaults, and a cleaner architecture for multi-tenant SaaS environments.

Core direction:

- index-first in v1
- about 80% Elasticsearch compatibility for common APIs and query DSL flows
- Rust data plane for indexing, query execution, and storage
- Go control plane later for tenancy, orchestration, quotas, and cloud-native operations
- log and event search first, while still supporting general document search
- fast by default, near-real-time behavior

Initial documentation:

- `docs/vision.md` - product thesis, target users, design principles, and non-goals
- `docs/architecture.md` - v1 node design, write path, read path, mappings, and storage model
- `docs/api-v1.md` - initial compatibility surface and API semantics
- `docs/storage-engine.md` - WAL, segment layout, refresh, flush, merge, and recovery design
- `docs/query-engine.md` - internal query AST, execution pipeline, scoring, and aggregations
- `docs/mappings.md` - dynamic mapping rules, field inference, templates, and conflict handling
- `docs/node-runtime.md` - node lifecycle, schedulers, caches, and background workers
- `docs/repo-layout.md` - proposed Rust crates and future Go service boundaries
- `docs/index-lifecycle.md` - index state machine, write visibility, recovery, and deletion behavior
- `docs/wal-format.md` - write-ahead log records, checkpoints, replay, and durability rules
- `docs/query-ast.md` - internal Rust query model, normalization, and execution contracts
- `docs/roadmap.md` - phased delivery plan from single-node engine to SaaS-ready platform

Working principles:

- compatibility helps migration, but does not control engine internals
- defaults beat knobs
- predictable behavior beats clever behavior
- multi-tenancy is designed in, even if exposed later
- time-aware workloads are first-class
- cost and operability are product features
