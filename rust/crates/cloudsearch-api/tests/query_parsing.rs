//! Query parsing edge case tests for cloudsearch-api.
//!
//! These test the API-layer query validation (not cloudsearch-index internals).
//! Run with: cargo test -p cloudsearch-api --test query_parsing

use axum::{body::Body, http::Request};
use cloudsearch_api::router;
use cloudsearch_common::CreateIndexRequest;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

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
                    })
                    .expect("serialize create request"),
                ))
                .expect("request"),
        )
        .await
        .expect("create response");
}

async fn make_app() -> axum::Router {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(cloudsearch_index::IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    router(catalog)
}

#[tokio::test]
async fn rejects_query_with_multiple_clauses() {
    let app = make_app().await;
    setup_index(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_search")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query":{"term":{"field":"x","value":"y"},"bool":{"must":[{"term":{"field":"a","value":"b"}}]}}}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn rejects_term_query_missing_field() {
    let app = make_app().await;
    setup_index(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":{"term":{"value":"y"}}}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn rejects_term_query_missing_value() {
    let app = make_app().await;
    setup_index(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":{"term":{"field":"x"}}}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn rejects_range_query_missing_field() {
    let app = make_app().await;
    setup_index(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":{"range":{"gte":1}}}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn rejects_bool_clause_non_array() {
    let app = make_app().await;
    setup_index(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":{"bool":{"must":"not-an-array"}}}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn rejects_unsupported_query_type_at_api_layer() {
    let app = make_app().await;
    setup_index(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_search")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query":{"fuzzy":{"field":"x","value":"y"}}}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn rejects_search_with_non_object_query() {
    let app = make_app().await;
    setup_index(&app).await;

    // Query value is a string, not an object
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test/_search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"just a string"}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 400);
}
