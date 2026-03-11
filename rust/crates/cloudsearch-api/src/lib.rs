use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, put},
};
use cloudsearch_common::{
    CloudSearchError, CreateIndexRequest, ErrorResponse, GetDocumentResponse, HealthResponse,
    IndexDocument, IndexDocumentRequest, IndexDocumentResponse,
};
use cloudsearch_index::IndexCatalog;
use std::sync::Arc;

#[derive(Clone)]
pub struct ApiState {
    catalog: Arc<IndexCatalog>,
}

impl ApiState {
    pub fn new(catalog: Arc<IndexCatalog>) -> Self {
        Self { catalog }
    }
}

pub fn router(catalog: Arc<IndexCatalog>) -> Router {
    Router::new()
        .route("/_health", get(health))
        .route("/{index}", put(create_index).get(get_index))
        .route("/{index}/_doc", put(index_document))
        .route("/{index}/_doc/{id}", get(get_document))
        .with_state(ApiState::new(catalog))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn create_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
    Json(request): Json<CreateIndexRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let metadata = state.catalog.create_index(&index, request).await?;
    Ok((StatusCode::CREATED, Json(metadata)))
}

async fn get_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let metadata = state.catalog.get_index(&index).await?;
    Ok((StatusCode::OK, Json(metadata)))
}

async fn index_document(
    State(state): State<ApiState>,
    Path(index): Path<String>,
    Json(request): Json<IndexDocumentRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut handle = state.catalog.open_index(&index).await?;
    let sequence_number = handle
        .index_document(IndexDocument {
            id: request.id.clone(),
            source: request.source,
        })
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(IndexDocumentResponse {
            id: request.id,
            result: "created",
            sequence_number,
        }),
    ))
}

async fn get_document(
    State(state): State<ApiState>,
    Path((index, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = state.catalog.open_index(&index).await?;
    let document = handle
        .get_document(&id)
        .ok_or_else(|| ApiError(CloudSearchError::IndexNotFound(format!("document '{id}'"))))?;

    Ok((
        StatusCode::OK,
        Json(GetDocumentResponse {
            id: document.id.clone(),
            found: true,
            source: document.source.clone(),
        }),
    ))
}

#[derive(Debug)]
struct ApiError(CloudSearchError);

impl From<CloudSearchError> for ApiError {
    fn from(value: CloudSearchError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            CloudSearchError::IndexAlreadyExists(_) => StatusCode::CONFLICT,
            CloudSearchError::IndexNotFound(_) => StatusCode::NOT_FOUND,
            CloudSearchError::InvalidIndexName(_) => StatusCode::BAD_REQUEST,
            CloudSearchError::InvalidWalRecord(_)
            | CloudSearchError::WalChecksumMismatch
            | CloudSearchError::Io(_)
            | CloudSearchError::Serde(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };

        (status, Json(ErrorResponse { error: self.0.to_string() })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use cloudsearch_common::{CreateIndexRequest, IndexDocumentRequest, IndexSettings};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn creates_and_fetches_index_over_http() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/logs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&CreateIndexRequest {
                            settings: IndexSettings {
                                mapping_mode: Default::default(),
                                primary_time_field: Some("@timestamp".to_string()),
                            },
                        })
                        .expect("serialize create request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("create response");

        let create_status = create_response.status();
        let create_body = create_response
            .into_body()
            .collect()
            .await
            .expect("create body")
            .to_bytes();

        assert_eq!(
            create_status,
            StatusCode::CREATED,
            "unexpected create body: {}",
            String::from_utf8_lossy(&create_body)
        );

        let get_response = app
            .oneshot(Request::builder().uri("/logs").body(Body::empty()).expect("request"))
            .await
            .expect("get response");

        assert_eq!(get_response.status(), StatusCode::OK);

        let body = get_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body_text = String::from_utf8(body.to_vec()).expect("utf8");

        assert!(body_text.contains("logs"));
        assert!(body_text.contains("@timestamp"));
    }

    #[tokio::test]
    async fn indexes_and_fetches_document_over_http() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/logs")
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

        let index_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/logs/_doc")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&IndexDocumentRequest {
                            id: "doc-1".to_string(),
                            source: serde_json::json!({"message": "hello from api"}),
                        })
                        .expect("serialize index request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("index response");

        assert_eq!(index_response.status(), StatusCode::CREATED);

        let get_response = app
            .oneshot(
                Request::builder()
                    .uri("/logs/_doc/doc-1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("get response");

        assert_eq!(get_response.status(), StatusCode::OK);

        let body = get_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body_text = String::from_utf8(body.to_vec()).expect("utf8");

        assert!(body_text.contains("doc-1"));
        assert!(body_text.contains("hello from api"));
    }
}
