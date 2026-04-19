//! Coverage tests for cloudsearch-index.
//!
//! Run with: cargo test -p cloudsearch-index --test coverage

use cloudsearch_common::{
    BoolQuery, CreateIndexRequest, IndexDocument, IndexSettings, SearchQuery, SearchRequest,
    SortOrder, SortSpec, TermQuery,
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
