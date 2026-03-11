# cloudSearch Vision

## Thesis

cloudSearch exists to make search infrastructure easier to run.

Elasticsearch and OpenSearch are powerful, but they are often expensive in both infrastructure cost and human complexity. Teams that only need the common search workflow still end up operating a system designed around many knobs, shard-heavy mental models, and operational patterns that are difficult to reason about.

cloudSearch focuses on a simpler promise:

- Elasticsearch-compatible enough for adoption and migration
- easier to understand and operate
- better prepared for multi-tenant SaaS workloads
- designed with cloud-native operational patterns in mind

## Primary Users

### Infra Teams

Infra and platform teams need a search engine that can ingest logs and events, expose familiar APIs, and stay understandable under load without requiring deep search-specific expertise.

### SaaS Teams

SaaS teams need search infrastructure that can eventually support tenants, quotas, isolation, and service integration without forcing them to build an entire search platform around a complicated engine.

## Product Positioning

cloudSearch is:

- a cloud-native search engine
- index-first in v1
- log and event aware from the beginning
- compatible with the common Elasticsearch workflow
- designed to evolve into a multi-tenant platform

cloudSearch is not:

- a full Elasticsearch replica
- a plugin-compatibility target
- a relevance research project
- a managed hosted service in v1

## Design Principles

### 1. Compatibility Helps Adoption

Compatibility is a migration strategy, not the engine identity.

We support the APIs and query shapes that cover most real usage. We do not copy obscure edge-case behavior when it conflicts with simplicity or architectural clarity.

### 2. Defaults Over Knobs

Every major subsystem should expose safe defaults first.

Users should not need to understand internal merge pressure, analyzer internals, or many index settings just to get good behavior.

### 3. Predictable Beats Clever

Inference is allowed, but not magical.

Mappings, string field behavior, refresh semantics, and time-aware optimizations should all be explainable and persisted clearly.

### 4. Cost Is A Feature

Storage growth, index bloat, write amplification, and memory pressure are first-class design concerns.

We should prefer a design that is slightly less flexible if it is much easier to run predictably.

### 5. Multi-Tenancy Is Designed In

Even though v1 starts with `index` as the public abstraction, internal metadata should be ready for future tenant ownership, quotas, policy attachment, and usage accounting.

### 6. Time-Aware Workloads Are First-Class

The engine should understand append-heavy, time-oriented data from the start.

This includes timestamp-aware pruning, retention hooks in metadata, and a path toward rollover and tiering later.

## V1 Success Criteria

V1 succeeds if it can:

- create and manage indexes cleanly
- ingest documents and bulk events with predictable behavior
- expose a familiar search API for common Elasticsearch workflows
- support full-text basics, filters, sorting, pagination, and small aggregations
- recover cleanly from crashes through WAL-backed durability
- remain understandable as a codebase and operating model

## V1 Non-Goals

V1 does not aim to provide:

- full distributed clustering
- replicas and shard orchestration exposed to users
- scripting
- plugin ecosystems
- exact Elasticsearch edge-case parity
- advanced security and RBAC
- cross-cluster features
- a hosted control plane
