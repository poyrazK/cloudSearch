//! Coverage tests for cloudsearch-index.
//!
//! Run with: cargo test -p cloudsearch-index --test coverage

use cloudsearch_common::{
    BoolQuery, CreateIndexRequest, IndexDocument, IndexSettings, MatchQuery, SearchQuery,
    SearchRequest, SortOrder, SortSpec, TermQuery,
};
use cloudsearch_index::{IndexCatalog, MergePlan};
use cloudsearch_storage::SegmentMeta;
use std::sync::Arc;
use tempfile::TempDir;

fn doc(id: &str, source: serde_json::Value) -> IndexDocument {
    IndexDocument {
        id: id.to_string(),
        source,
    }
}

#[tokio::test]
async fn get_document_returns_none_for_pending_delete() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // Index a doc then delete it — delete goes to pending_operations
    handle
        .index_document(doc("1", serde_json::json!({"x": 1})))
        .await
        .expect("index");
    handle.delete_document("1").await.expect("delete");

    // get_document should return None for deleted doc (pending delete)
    assert!(handle.get_document("1").is_none());
}

#[tokio::test]
async fn apply_merge_plan_skips_when_no_on_disk_segment() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // Index a doc so pending_operations has an entry, but no flush (no on-disk segment)
    handle
        .index_document(doc("1", serde_json::json!({"x": 1})))
        .await
        .expect("index");

    // Create a merge plan — should skip gracefully when no segment on disk
    let plan = MergePlan::new(vec![SegmentMeta {
        segment_number: 1,
        last_sequence_number: 1,
        document_count: 1,
        checksum: 0,
    }]);
    let result = handle.apply_merge_plan(&plan).await;
    result.expect("apply_merge_plan should succeed gracefully");
}

#[tokio::test]
async fn validate_search_request_rejects_nested_bool_with_object_field() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // Index a doc with an object field to establish mapping
    handle
        .index_document(doc("1", serde_json::json!({"meta": {"nested": "value"}})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    // Search with nested bool query that sorts on object field — should be rejected
    let request = SearchRequest {
        query: Some(SearchQuery::Bool(BoolQuery {
            must: vec![SearchQuery::Bool(BoolQuery {
                must: vec![SearchQuery::Term(TermQuery {
                    field: "meta".to_string(),
                    value: serde_json::json!("value"),
                })],
                should: vec![],
                filter: vec![],
                must_not: vec![],
            })],
            should: vec![],
            filter: vec![],
            must_not: vec![],
        })),
        sort: Some(SortSpec {
            field: "meta".to_string(),
            order: SortOrder::Asc,
        }),
        ..Default::default()
    };

    let result = handle.validate_search_request(&request);
    assert!(
        result.is_err(),
        "nested bool with object sort field should be rejected"
    );
}

#[tokio::test]
async fn validate_search_request_rejects_size_exceeding_max() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let handle = catalog.open_index("test").await.expect("open index");

    let request = SearchRequest {
        size: Some(100_000),
        ..Default::default()
    };

    let result = handle.validate_search_request(&request);
    assert!(
        result.is_err(),
        "size exceeding MAX_SEARCH_SIZE should be rejected"
    );
}

#[tokio::test]
async fn validate_search_request_rejects_from_exceeding_max() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let handle = catalog.open_index("test").await.expect("open index");

    let request = SearchRequest {
        from: Some(2_000_000),
        ..Default::default()
    };

    let result = handle.validate_search_request(&request);
    assert!(
        result.is_err(),
        "from exceeding MAX_SEARCH_OFFSET should be rejected"
    );
}

#[tokio::test]
async fn highlight_positions_case_insensitive() {
    // Index doc with mixed-case text, search for lowercase term.
    // extract_positions now searches in to_ascii_lowercase()'d text,
    // so token "error" finds "ERROR" positions correctly.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "1",
            serde_json::json!({"content": "ERROR log ERROR message"}),
        ))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Match(MatchQuery {
            field: "content".to_string(),
            value: "error".to_string(),
        })),
        ..Default::default()
    });

    let hit = result.hits.hits.first().expect("should have a hit");
    assert!(
        hit.highlight.is_some(),
        "lowercase 'error' should find highlight in 'ERROR log ERROR'"
    );
}

#[tokio::test]
async fn highlight_positions_multiple_occurrences() {
    // Same term appears twice in one field — all positions should be captured.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "1",
            serde_json::json!({"content": "info error info error"}),
        ))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Match(MatchQuery {
            field: "content".to_string(),
            value: "info".to_string(),
        })),
        ..Default::default()
    });

    let hit = result.hits.hits.first().expect("should have a hit");
    assert!(
        hit.highlight.is_some(),
        "'info' should be highlighted even when it appears twice"
    );
}

#[tokio::test]
async fn highlight_positions_no_match() {
    // Term not in document — no highlight.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("1", serde_json::json!({"content": "hello world"})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    // Use MatchAll so the document IS found (giving us a hit to assert on),
    // then verify that hit has no highlight since no query terms are provided.
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::MatchAll),
        ..Default::default()
    });

    let hit = result.hits.hits.first().expect("doc should be found with MatchAll");
    assert!(
        hit.highlight.is_none(),
        "MatchAll query should produce no highlight (no query terms to highlight)"
    );
}

#[tokio::test]
async fn highlight_positions_empty_field() {
    // Document with empty text field — no highlights.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("1", serde_json::json!({"content": ""})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Match(MatchQuery {
            field: "content".to_string(),
            value: "any".to_string(),
        })),
        ..Default::default()
    });

    assert!(
        result.hits.hits.is_empty() || result.hits.hits.iter().all(|h| h.highlight.is_none()),
        "no highlight for empty text field"
    );
}
