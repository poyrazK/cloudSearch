//! Slow query warning tests for cloudsearch-api.
//!
//! Run with: cargo test -p cloudsearch-api --test slow_query

use axum::{body::Body, http::Request};
use cloudsearch_api::router;
use cloudsearch_common::CreateIndexRequest;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

// Number of docs needed to exceed 50ms threshold on the search path.
// This is heuristic-based — we tune the count until the slow path triggers.
const TRIGGER_DOC_COUNT: usize = 1000;

async fn make_app() -> axum::Router {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(cloudsearch_index::IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    router(catalog)
}

async fn setup_index_with_docs(app: &axum::Router, count: usize) {
    for i in 0..count {
        let doc_id = format!("doc-{}", i);
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/test/_doc/{doc_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&cloudsearch_common::IndexDocumentRequest {
                            id: doc_id,
                            source: serde_json::json!({"msg": "data", "x": i}),
                        })
                        .expect("serialize"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("index response");
    }
}

async fn setup_index(app: &axum::Router) {
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&CreateIndexRequest {
                        settings: Default::default(),
                        ..Default::default()
                    })
                    .expect("serialize create request"),
                ))
                .expect("request"),
        )
        .await
        .expect("create response");
}

#[tokio::test]
async fn search_with_large_result_triggers_slow_query_warning() {
    let app = make_app().await;
    setup_index(&app).await;
    setup_index_with_docs(&app, TRIGGER_DOC_COUNT).await;

    // Force refresh so docs are searchable
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_refresh")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("refresh response");

    // Search that returns many results — should exceed 50ms
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":{"match_all":{}}}"#))
                .expect("request"),
        )
        .await
        .expect("response");

    // Should still succeed (warning doesn't fail the request)
    assert_eq!(response.status(), 200);
}
