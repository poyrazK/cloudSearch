//! Benchmark search performance across different document counts and query types.
//!
//! # Running
//!
//! ```text
//! cargo bench -p cloudsearch-index
//! ```
//!
//! Results are written to `target/criterion/` and can be compared across runs with:
//!
//! ```text
//! cargo bench -p cloudsearch-index -- --save-baseline <name>
//! cargo bench -p cloudsearch-index -- --baseline <name>
//! ```
//!
//! # Benchmarks
//!
//! Each benchmark builds an index with `n` documents, flushes it, reopens it (so
//! segments are loaded from disk), then times `search()` calls.
//!
//! | Benchmark | What it measures | Expected scaling |
//! |---|---|---|
//! | `MatchAll` | Pure iteration overhead — no scoring | O(n) in doc count |
//! | `term_query` | Posting list lookup + BM25 term scoring | O(hits) |
//! | `multi_match_title_body` | Cross-field tokenization + BM25 per field | Most expensive |
//! | `range_count_gte_500` | Sequential field comparison | O(n) in doc count |
//!
//! # Interpretation
//!
//! - **`MatchAll` at constant time** regardless of doc count → scoring loop is the
//!   dominant cost, not iteration
//! - **`multi_match` slow at 10k** → tokenization + BM25 per field is CPU-bound;
//!   parallel scoring should show significant speedup here
//! - **`range` is cheap** → field comparison is fast; no posting lookup needed
//!
//! To compare parallel vs sequential scoring specifically, benchmark before/after
//! on a multi-core machine and look for speedup on `multi_match` and `term_query`.

use cloudsearch_common::{
    CreateIndexRequest, IndexDocument, IndexSettings, MultiMatchQuery, MultiMatchType, RangeQuery,
    SearchQuery, SearchRequest, TermQuery,
};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use tempfile::TempDir;

fn build_index(n_docs: usize) -> cloudsearch_index::IndexHandle {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(cloudsearch_index::IndexCatalog::new(temp_dir.path()));

    // Use a blocking runtime for async catalog operations
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let _: cloudsearch_common::Result<()> = catalog.initialize().await;
        let _: cloudsearch_common::Result<cloudsearch_common::IndexMetadata> = catalog
            .create_index(
                "test",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await;

        let mut handle: cloudsearch_index::IndexHandle = catalog.open_index("test").await.expect("open index");

        // Index documents with varied content to avoid all docs being identical
        for i in 0..n_docs {
            let doc = IndexDocument {
                id: format!("doc_{i}"),
                source: serde_json::json!({
                    "title": format!("Document {i} title text"),
                    "body": format!("This is the body content of document number {i} with some searchable text"),
                    "category": format!("cat_{}", i % 10),
                    "count": i,
                }),
            };
            let _: cloudsearch_common::Result<u64> = handle.index_document(doc).await;
        }

        let _: cloudsearch_common::Result<usize> = handle.refresh().await;
        let _: cloudsearch_common::Result<cloudsearch_common::FlushResponse> = handle.flush().await;

        // Reopen to ensure segments are loaded fresh
        drop(handle);
        let handle: cloudsearch_index::IndexHandle = catalog.open_index("test").await.expect("open index");
        handle
    })
}

fn bench_search(c: &mut Criterion) {
    // Benchmark with different document counts
    for &n_docs in &[1_000, 10_000] {
        let handle = build_index(n_docs);

        let mut group = c.benchmark_group(format!("search/{n_docs}_docs"));

        // MatchAll — measures pure document iteration overhead
        group.bench_function("MatchAll", |b| {
            b.iter(|| {
                let result = handle.search(&SearchRequest {
                    query: Some(SearchQuery::MatchAll),
                    size: Some(10),
                    ..Default::default()
                });
                black_box(result.hits.total);
            });
        });

        // Term query — measures scoring + posting lookup
        group.bench_function("term_query_cat_5", |b| {
            b.iter(|| {
                let result = handle.search(&SearchRequest {
                    query: Some(SearchQuery::Term(TermQuery {
                        field: "category".to_string(),
                        value: serde_json::json!("cat_5"),
                        fuzziness: None,
                        boost: None,
                    })),
                    size: Some(10),
                    ..Default::default()
                });
                black_box(result.hits.total);
            });
        });

        // Multi-match query — measures cross-field scoring
        group.bench_function("multi_match_title_body", |b| {
            b.iter(|| {
                let mut fields = std::collections::BTreeMap::new();
                fields.insert("title".to_string(), 1.0);
                fields.insert("body".to_string(), 1.0);
                let result = handle.search(&SearchRequest {
                    query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
                        query: "document text".to_string(),
                        fields,
                        multi_match_type: MultiMatchType::BestFields,
                        tie_breaker: 0.3,
                    })),
                    size: Some(10),
                    ..Default::default()
                });
                black_box(result.hits.total);
            });
        });

        // Range query — measures field comparison
        group.bench_function("range_count_gte_500", |b| {
            b.iter(|| {
                let result = handle.search(&SearchRequest {
                    query: Some(SearchQuery::Range(RangeQuery {
                        field: "count".to_string(),
                        gte: Some(serde_json::json!(500)),
                        gt: None,
                        lte: None,
                        lt: None,
                    })),
                    size: Some(10),
                    ..Default::default()
                });
                black_box(result.hits.total);
            });
        });

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(std::time::Duration::from_secs(2))
        .measurement_time(std::time::Duration::from_secs(5));
    targets = bench_search
}
criterion_main!(benches);
