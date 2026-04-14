//! Bulk operation edge case tests for cloudsearch-api.
//!
//! Run with: cargo test -p cloudsearch-api --test bulk

use axum::{body::Body, http::Request};
use cloudsearch_api::router;
use cloudsearch_common::CreateIndexRequest;
use http_body_util::BodyExt;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn make_app() -> axum::Router {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(cloudsearch_index::IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    router(catalog)
}

async fn create_index(app: &axum::Router) {
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
                    .expect("serialize"),
                ))
                .expect("request"),
        )
        .await
        .expect("create response");
}

#[tokio::test]
async fn bulk_with_empty_operations_returns_empty_items() {
    let app = make_app().await;
    create_index(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_bulk")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"operations":[]}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 200);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).expect("parse");
    assert_eq!(json["items"], serde_json::json!([]));
}

#[tokio::test]
async fn bulk_rejects_index_operation_with_missing_id() {
    let app = make_app().await;
    create_index(&app).await;

    // Operation with missing id field — serde returns 422
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_bulk")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"operations":[{"index":{"source":{"x":1}}}]}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 422);
}

#[tokio::test]
async fn bulk_rejects_delete_operation_with_missing_id() {
    let app = make_app().await;
    create_index(&app).await;

    // Delete with missing id — serde returns 422
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_bulk")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"operations":[{"delete":{}}]}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 422);
}

#[tokio::test]
async fn bulk_succeeds_with_valid_single_index_operation() {
    let app = make_app().await;
    create_index(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_bulk")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"operations":[{"index":{"id":"doc-1","source":{"msg":"hello"}}}]}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 200);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).expect("parse");
    assert_eq!(json["errors"], false);
}
