//! Metrics output format tests for cloudsearch-api.
//!
//! Run with: cargo test -p cloudsearch-api --test metrics

use axum::{body::Body, http::Request};
use cloudsearch_api::router;
use cloudsearch_common::{CreateIndexRequest, IndexDocumentRequest, IndexSettings};
use http_body_util::BodyExt;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn make_app() -> (TempDir, axum::Router) {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(cloudsearch_index::IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let app = router(catalog);
    (temp_dir, app)
}

async fn create_index(app: &axum::Router, name: &str) {
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{name}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&CreateIndexRequest {
                        settings: IndexSettings::default(),
                        ..Default::default()
                    })
                    .expect("serialize"),
                ))
                .expect("request"),
        )
        .await
        .expect("create response");
}

async fn index_doc(app: &axum::Router, index: &str, id: &str) {
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{index}/_doc"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&IndexDocumentRequest {
                        id: id.to_string(),
                        source: serde_json::json!({"x": 1}),
                    })
                    .expect("serialize"),
                ))
                .expect("request"),
        )
        .await
        .expect("index response");
}

async fn get_metrics(app: &axum::Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics response");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    String::from_utf8(body.to_vec()).expect("utf8")
}

#[tokio::test]
async fn metrics_includes_request_counters() {
    let (_temp_dir, app) = make_app().await;
    create_index(&app, "test").await;

    let metrics = get_metrics(&app).await;
    assert!(
        metrics.contains("cloudsearch_requests_total"),
        "metrics should contain request counter"
    );
}

#[tokio::test]
async fn metrics_includes_index_writes_after_doc_index() {
    let (_temp_dir, app) = make_app().await;
    create_index(&app, "test").await;
    index_doc(&app, "test", "doc-1").await;

    let metrics = get_metrics(&app).await;
    assert!(
        metrics.contains("cloudsearch_index_writes_total"),
        "metrics should contain index writes counter"
    );
}
