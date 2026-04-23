//! Test utilities for cloudsearch crates.
//!
//! Provides reusable helpers for integration testing.

use axum::body::Body;
use axum::http::Request;
use axum::Router;
use cloudsearch_common::{BulkIndexOperation, BulkOperation, BulkRequest, CreateIndexRequest, IndexDocumentRequest, IndexSettings, SearchRequest, SearchResponse};
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::Arc;
use tower::util::ServiceExt;

use cloudsearch_index::IndexCatalog;

/// Creates a test app with in-memory catalog and router.
///
/// # Panics
/// Panics if temp directory creation or catalog initialization fails.
pub async fn test_app() -> (tempfile::TempDir, Arc<IndexCatalog>, Router) {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let app = cloudsearch_api::router(catalog.clone());
    (temp_dir, catalog, app)
}

/// Creates an index via HTTP.
///
/// # Errors
/// Returns `cloudsearch_api::ApiError` if the HTTP request fails.
///
/// # Panics
/// Panics if the request cannot be built or the response cannot be processed.
pub async fn create_index(app: &Router, name: &str) -> Result<(), cloudsearch_api::ApiError> {
    create_index_with_settings(app, name, IndexSettings::default()).await
}

/// Creates an index with custom settings via HTTP.
///
/// # Errors
/// Returns `cloudsearch_api::ApiError` if the HTTP request fails.
///
/// # Panics
/// Panics if the request cannot be built or the response cannot be processed.
pub async fn create_index_with_settings(
    app: &Router,
    name: &str,
    settings: IndexSettings,
) -> Result<(), cloudsearch_api::ApiError> {
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/{name}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&CreateIndexRequest {
                settings,
                ..Default::default()
            })
            .expect("serialize"),
        ))
        .expect("request");

    let _ = app
        .clone()
        .oneshot(request)
        .await
        .expect("create response");

    Ok(())
}

/// Indexes a single document.
///
/// # Errors
/// Returns `cloudsearch_api::ApiError` if the HTTP request fails.
///
/// # Panics
/// Panics if the request cannot be built or the response cannot be processed.
pub async fn index_doc(
    app: &Router,
    index: &str,
    id: &str,
    source: Value,
) -> Result<(), cloudsearch_api::ApiError> {
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/{index}/_doc"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&IndexDocumentRequest {
                id: id.to_string(),
                source,
            })
            .expect("serialize"),
        ))
        .expect("request");

    let _ = app.clone().oneshot(request).await.expect("index response");
    Ok(())
}

/// Bulk indexes documents.
///
/// # Errors
/// Returns `cloudsearch_api::ApiError` if the HTTP request fails.
///
/// # Panics
/// Panics if the request cannot be built or the response cannot be processed.
pub async fn bulk_index(
    app: &Router,
    index: &str,
    docs: &[(impl Into<String> + Clone, Value)],
) -> Result<(), cloudsearch_api::ApiError> {
    let operations = docs
        .iter()
        .map(|(id, source)| BulkOperation::Index(BulkIndexOperation {
            id: id.clone().into(),
            source: source.clone(),
        }))
        .collect();

    let request = Request::builder()
        .method("POST")
        .uri(format!("/{index}/_bulk"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&BulkRequest { operations }).expect("serialize"),
        ))
        .expect("request");

    let _ = app.clone().oneshot(request).await.expect("bulk response");
    Ok(())
}

/// Refreshes an index.
///
/// # Errors
/// Returns `cloudsearch_api::ApiError` if the HTTP request fails.
///
/// # Panics
/// Panics if the request cannot be built or the response cannot be processed.
pub async fn refresh_index(app: &Router, index: &str) -> Result<(), cloudsearch_api::ApiError> {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/{index}/_refresh"))
        .body(Body::empty())
        .expect("request");

    let _ = app.clone().oneshot(request).await.expect("refresh response");
    Ok(())
}

/// Searches an index and returns the parsed response.
///
/// # Errors
/// Returns `cloudsearch_api::ApiError` if the HTTP request fails or the response
/// cannot be deserialized.
///
/// # Panics
/// Panics if the request cannot be built or the response body cannot be collected.
pub async fn search(
    app: &Router,
    index: &str,
    query: Value,
) -> Result<SearchResponse, cloudsearch_api::ApiError> {
    let body = serde_json::json!({ "query": query });

    let request = Request::builder()
        .method("POST")
        .uri(format!("/{index}/_search"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).expect("serialize")))
        .expect("request");

    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("search response");

    let body = response.into_body().collect().await.expect("body").to_bytes();
    let body: SearchResponse = serde_json::from_slice(&body).expect("deserialize");
    Ok(body)
}

/// Searches an index using a `SearchRequest` struct and returns the parsed response.
///
/// # Errors
/// Returns `cloudsearch_api::ApiError` if the HTTP request fails or the response
/// cannot be deserialized.
///
/// # Panics
/// Panics if the request cannot be built or the response body cannot be collected.
pub async fn search_request(
    app: &Router,
    index: &str,
    search_request: SearchRequest,
) -> Result<SearchResponse, cloudsearch_api::ApiError> {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/{index}/_search"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&search_request).expect("serialize")))
        .expect("request");

    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("search response");

    let body = response.into_body().collect().await.expect("body").to_bytes();
    let body: SearchResponse = serde_json::from_slice(&body).expect("deserialize");
    Ok(body)
}

/// Extracts JSON from a response body.
///
/// # Panics
/// Panics if the body cannot be collected or deserialized.
pub async fn body_json<T: DeserializeOwned>(response: axum::response::Response<Body>) -> T {
    let body = response.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&body).expect("deserialize")
}