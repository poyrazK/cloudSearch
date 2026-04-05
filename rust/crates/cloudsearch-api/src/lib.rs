use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, put},
};
use cloudsearch_common::{
    AggregationRequest, AggregationResult, BoolQuery, BulkItem, BulkItemResult, BulkRequest,
    BulkResponse, CloudSearchError, CreateIndexRequest, DateHistogramAggregationResult,
    ErrorResponse, FlushResponse, HealthResponse, IndexDocument, IndexDocumentRequest, RangeQuery,
    RefreshResponse, SearchHit, SearchQuery, SearchRequest, SearchResponse, SortSpec,
    StatsAggregationResult, TermQuery, TermsAggregationResult, TermsQuery,
};
use cloudsearch_index::{IndexCatalog, IndexHandle};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(serde::Serialize)]
struct CompatIndexDocumentResponse {
    #[serde(rename = "_id")]
    id: String,
    result: &'static str,
}

#[derive(serde::Serialize)]
struct CompatGetDocumentResponse {
    #[serde(rename = "_id")]
    id: String,
    found: bool,
    #[serde(rename = "_source")]
    source: Value,
}

#[derive(serde::Serialize)]
struct CompatSearchResponse {
    hits: CompatHitsMetadata,
    aggregations: BTreeMap<String, Value>,
}

#[derive(serde::Serialize)]
struct CompatHitsMetadata {
    total: CompatTotalHits,
    hits: Vec<CompatSearchHit>,
}

#[derive(serde::Serialize)]
struct CompatTotalHits {
    value: usize,
    relation: &'static str,
}

#[derive(serde::Serialize)]
struct CompatSearchHit {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_source")]
    source: Value,
}

#[derive(serde::Serialize)]
struct CompatBulkResponse {
    errors: bool,
    items: Vec<CompatBulkItem>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CompatBulkItem {
    Index(CompatBulkItemResult),
    Delete(CompatBulkItemResult),
}

#[derive(serde::Serialize)]
struct CompatBulkItemResult {
    #[serde(rename = "_id")]
    id: String,
    result: String,
}

#[derive(Clone)]
pub struct ApiState {
    catalog: Arc<IndexCatalog>,
    handles: Arc<Mutex<HashMap<String, Arc<Mutex<IndexHandle>>>>>,
}

impl ApiState {
    pub fn new(catalog: Arc<IndexCatalog>) -> Self {
        Self {
            catalog,
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn index_handle(&self, index: &str) -> Result<Arc<Mutex<IndexHandle>>, ApiError> {
        let mut handles = self.handles.lock().await;

        if let Some(handle) = handles.get(index) {
            return Ok(handle.clone());
        }

        let handle = Arc::new(Mutex::new(self.catalog.open_index(index).await?));
        handles.insert(index.to_string(), handle.clone());
        Ok(handle)
    }
}

pub fn router(catalog: Arc<IndexCatalog>) -> Router {
    Router::new()
        .route("/_health", get(health))
        .route("/{index}", put(create_index).get(get_index))
        .route("/{index}/_bulk", put(bulk_index).post(bulk_index))
        .route("/{index}/_doc", put(index_document))
        .route("/{index}/_doc/{id}", get(get_document))
        .route("/{index}/_flush", put(flush_index).post(flush_index))
        .route("/{index}/_refresh", put(refresh_index).post(refresh_index))
        .route("/{index}/_search", put(search_index).post(search_index))
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
    let handle = Arc::new(Mutex::new(state.catalog.open_index(&index).await?));
    state.handles.lock().await.insert(index, handle);
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
    let handle = state.index_handle(&index).await?;
    let mut handle = handle.lock().await;
    let result = if handle.get_document(&request.id).is_some() {
        "updated"
    } else {
        "created"
    };
    handle
        .index_document(IndexDocument {
            id: request.id.clone(),
            source: request.source,
        })
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(CompatIndexDocumentResponse {
            id: request.id,
            result,
        }),
    ))
}

async fn bulk_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
    Json(request): Json<BulkRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = state.index_handle(&index).await?;
    let mut handle = handle.lock().await;
    let response = handle.bulk_apply(request).await?;
    Ok((StatusCode::OK, Json(to_compat_bulk_response(response))))
}

async fn get_document(
    State(state): State<ApiState>,
    Path((index, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = state.index_handle(&index).await?;
    let handle = handle.lock().await;
    let document = handle
        .get_document(&id)
        .ok_or_else(|| ApiError(CloudSearchError::IndexNotFound(format!("document '{id}'"))))?;

    Ok((
        StatusCode::OK,
        Json(CompatGetDocumentResponse {
            id: document.id.clone(),
            found: true,
            source: document.source.clone(),
        }),
    ))
}

async fn search_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
    Json(request): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    let request = parse_search_request(request)?;
    let handle = state.index_handle(&index).await?;
    let handle = handle.lock().await;
    handle.validate_search_request(&request)?;
    Ok((
        StatusCode::OK,
        Json(to_compat_search_response(handle.search(&request))),
    ))
}

fn parse_search_request(value: Value) -> Result<SearchRequest, ApiError> {
    let mut request = SearchRequest::default();
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "search request must be a JSON object".to_string(),
        ))
    })?;

    if let Some(query) = object.get("query").filter(|value| !value.is_null()) {
        request.query = Some(parse_query(query)?);
    }

    if let Some(from) = object.get("from").filter(|value| !value.is_null()) {
        request.from = Some(parse_usize_field(from, "from")?);
    }

    if let Some(size) = object.get("size").filter(|value| !value.is_null()) {
        request.size = Some(parse_usize_field(size, "size")?);
    }

    if let Some(sort) = object.get("sort").filter(|value| !value.is_null()) {
        request.sort = Some(parse_sort(sort)?);
    }

    if let Some(aggs) = object
        .get("aggs")
        .or_else(|| object.get("aggregations"))
        .filter(|value| !value.is_null())
    {
        request.aggs = Some(
            serde_json::from_value::<std::collections::BTreeMap<String, AggregationRequest>>(
                aggs.clone(),
            )
            .map_err(CloudSearchError::from)
            .map_err(ApiError::from)?,
        );
    }

    Ok(request)
}

fn parse_query(value: &Value) -> Result<SearchQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "query must be a JSON object".to_string(),
        ))
    })?;

    if object.len() != 1 {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "query object must contain exactly one clause".to_string(),
        )));
    }

    let (kind, body) = object.iter().next().expect("single query entry");
    match kind.as_str() {
        "match_all" => Ok(SearchQuery::MatchAll),
        "term" => Ok(SearchQuery::Term(parse_term_query(body)?)),
        "terms" => Ok(SearchQuery::Terms(parse_terms_query(body)?)),
        "range" => Ok(SearchQuery::Range(parse_range_query(body)?)),
        "bool" => Ok(SearchQuery::Bool(parse_bool_query(body)?)),
        other => Err(ApiError(CloudSearchError::InvalidSearchRequest(format!(
            "unsupported query clause '{other}'"
        )))),
    }
}

fn parse_term_query(value: &Value) -> Result<TermQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "term query must be a JSON object".to_string(),
        ))
    })?;

    if object.contains_key("field") || object.contains_key("value") {
        let field = object.get("field").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "internal term query shape requires string 'field'".to_string(),
            ))
        })?;
        let value = object.get("value").cloned().ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "internal term query shape requires 'value'".to_string(),
            ))
        })?;

        return Ok(TermQuery {
            field: field.to_string(),
            value,
        });
    }

    if object.len() != 1 {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "term query must contain exactly one field".to_string(),
        )));
    }

    let (field, raw_value) = object.iter().next().expect("single term field");
    Ok(TermQuery {
        field: field.clone(),
        value: raw_value.clone(),
    })
}

fn parse_terms_query(value: &Value) -> Result<TermsQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "terms query must be a JSON object".to_string(),
        ))
    })?;

    if object.contains_key("field") || object.contains_key("values") {
        let field = object.get("field").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "internal terms query shape requires string 'field'".to_string(),
            ))
        })?;
        let values = object
            .get("values")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                ApiError(CloudSearchError::InvalidSearchRequest(
                    "internal terms query shape requires array 'values'".to_string(),
                ))
            })?;

        return Ok(TermsQuery {
            field: field.to_string(),
            values,
        });
    }

    if object.len() != 1 {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "terms query must contain exactly one field".to_string(),
        )));
    }

    let (field, raw_values) = object.iter().next().expect("single terms field");
    let values = raw_values.as_array().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "terms query field must map to an array".to_string(),
        ))
    })?;

    Ok(TermsQuery {
        field: field.clone(),
        values: values.clone(),
    })
}

fn parse_range_query(value: &Value) -> Result<RangeQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "range query must be a JSON object".to_string(),
        ))
    })?;

    if object.contains_key("field") {
        let field = object.get("field").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "internal range query shape requires string 'field'".to_string(),
            ))
        })?;

        return Ok(RangeQuery {
            field: field.to_string(),
            gte: object.get("gte").filter(|value| !value.is_null()).cloned(),
            gt: object.get("gt").filter(|value| !value.is_null()).cloned(),
            lte: object.get("lte").filter(|value| !value.is_null()).cloned(),
            lt: object.get("lt").filter(|value| !value.is_null()).cloned(),
        });
    }

    if object.len() != 1 {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "range query must contain exactly one field".to_string(),
        )));
    }

    let (field, bounds) = object.iter().next().expect("single range field");
    let bounds = bounds.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "range bounds must be a JSON object".to_string(),
        ))
    })?;

    Ok(RangeQuery {
        field: field.clone(),
        gte: bounds.get("gte").filter(|value| !value.is_null()).cloned(),
        gt: bounds.get("gt").filter(|value| !value.is_null()).cloned(),
        lte: bounds.get("lte").filter(|value| !value.is_null()).cloned(),
        lt: bounds.get("lt").filter(|value| !value.is_null()).cloned(),
    })
}

fn parse_bool_query(value: &Value) -> Result<BoolQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "bool query must be a JSON object".to_string(),
        ))
    })?;

    let allowed = ["must", "should", "filter", "must_not"];
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(ApiError(CloudSearchError::InvalidSearchRequest(format!(
                "unsupported bool clause '{key}'"
            ))));
        }
    }

    Ok(BoolQuery {
        must: parse_bool_clause_array(object.get("must"), "bool.must")?,
        should: parse_bool_clause_array(object.get("should"), "bool.should")?,
        filter: parse_bool_clause_array(object.get("filter"), "bool.filter")?,
        must_not: parse_bool_clause_array(object.get("must_not"), "bool.must_not")?,
    })
}

fn parse_bool_clause_array(
    value: Option<&Value>,
    field: &str,
) -> Result<Vec<SearchQuery>, ApiError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };

    if value.is_null() {
        return Ok(Vec::new());
    }

    let values = value.as_array().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(format!(
            "{field} must be an array"
        )))
    })?;

    values
        .iter()
        .filter(|value| !value.is_null())
        .map(parse_query)
        .collect()
}

fn parse_sort(value: &Value) -> Result<SortSpec, ApiError> {
    if let Ok(sort) = serde_json::from_value::<SortSpec>(value.clone()) {
        return Ok(sort);
    }

    let item = match value {
        Value::Array(items) => {
            if items.len() != 1 {
                return Err(ApiError(CloudSearchError::InvalidSearchRequest(
                    "only one sort entry is supported".to_string(),
                )));
            }
            &items[0]
        }
        _ => value,
    };

    let object = item.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "sort must be an object or single-entry array".to_string(),
        ))
    })?;

    if object.len() != 1 {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "sort object must contain exactly one field".to_string(),
        )));
    }

    let (field, body) = object.iter().next().expect("single sort field");
    let order = body
        .as_object()
        .and_then(|body| body.get("order"))
        .cloned()
        .unwrap_or_else(|| Value::String("asc".to_string()));

    Ok(SortSpec {
        field: field.clone(),
        order: serde_json::from_value(order)
            .map_err(CloudSearchError::from)
            .map_err(ApiError::from)?,
    })
}

fn parse_usize_field(value: &Value, field: &str) -> Result<usize, ApiError> {
    let value = value.as_u64().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(format!(
            "{field} must be a non-negative integer"
        )))
    })?;

    usize::try_from(value).map_err(|_| {
        ApiError(CloudSearchError::InvalidSearchRequest(format!(
            "{field} is too large"
        )))
    })
}

fn to_compat_search_response(response: SearchResponse) -> CompatSearchResponse {
    CompatSearchResponse {
        hits: CompatHitsMetadata {
            total: CompatTotalHits {
                value: response.hits.total,
                relation: "eq",
            },
            hits: response
                .hits
                .hits
                .into_iter()
                .map(to_compat_search_hit)
                .collect(),
        },
        aggregations: response
            .aggregations
            .into_iter()
            .map(|(name, aggregation)| (name, aggregation_to_json(aggregation)))
            .collect(),
    }
}

fn to_compat_search_hit(hit: SearchHit) -> CompatSearchHit {
    CompatSearchHit {
        id: hit.id,
        source: hit.source,
    }
}

fn to_compat_bulk_response(response: BulkResponse) -> CompatBulkResponse {
    CompatBulkResponse {
        errors: response.errors,
        items: response
            .items
            .into_iter()
            .map(|item| match item {
                BulkItem::Index(result) => {
                    CompatBulkItem::Index(to_compat_bulk_item_result(result))
                }
                BulkItem::Delete(result) => {
                    CompatBulkItem::Delete(to_compat_bulk_item_result(result))
                }
            })
            .collect(),
    }
}

fn to_compat_bulk_item_result(result: BulkItemResult) -> CompatBulkItemResult {
    CompatBulkItemResult {
        id: result.id,
        result: result.result,
    }
}

fn aggregation_to_json(aggregation: AggregationResult) -> Value {
    match aggregation {
        AggregationResult::Terms(result) => terms_aggregation_to_json(result),
        AggregationResult::Stats(result) => stats_aggregation_to_json(result),
        AggregationResult::DateHistogram(result) => date_histogram_aggregation_to_json(result),
    }
}

fn terms_aggregation_to_json(result: TermsAggregationResult) -> Value {
    serde_json::json!({
        "buckets": result.buckets
    })
}

fn stats_aggregation_to_json(result: StatsAggregationResult) -> Value {
    serde_json::json!({
        "count": result.count,
        "min": result.min,
        "max": result.max,
        "avg": result.avg,
        "sum": result.sum,
    })
}

fn date_histogram_aggregation_to_json(result: DateHistogramAggregationResult) -> Value {
    serde_json::json!({
        "buckets": result.buckets
    })
}

async fn refresh_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = state.index_handle(&index).await?;
    let mut handle = handle.lock().await;
    let refreshed_documents = handle.refresh().await?;

    Ok((
        StatusCode::OK,
        Json(RefreshResponse {
            result: "refreshed",
            refreshed_documents,
        }),
    ))
}

async fn flush_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = state.index_handle(&index).await?;
    let mut handle = handle.lock().await;
    let response = handle.flush().await?;

    Ok((StatusCode::OK, Json::<FlushResponse>(response)))
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
            CloudSearchError::InvalidIndexName(_)
            | CloudSearchError::InvalidSearchRequest(_)
            | CloudSearchError::MappingConflict(_)
            | CloudSearchError::UnknownFieldRejected(_)
            | CloudSearchError::UnsupportedArrayField(_)
            | CloudSearchError::MappingLimitExceeded(_) => StatusCode::BAD_REQUEST,
            CloudSearchError::InvalidWalRecord(_)
            | CloudSearchError::WalChecksumMismatch
            | CloudSearchError::Io(_)
            | CloudSearchError::Serde(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (
            status,
            Json(ErrorResponse {
                error: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use cloudsearch_common::{
        AggregationRequest, BoolQuery, BulkDeleteOperation, BulkIndexOperation, BulkOperation,
        BulkRequest, CreateIndexRequest, DateHistogramAggregationRequest, DateHistogramInterval,
        IndexDocumentRequest, IndexSettings, RangeQuery, SearchQuery, SearchRequest, SortOrder,
        SortSpec, StatsAggregationRequest, TermQuery, TermsAggregationRequest, TermsQuery,
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_returns_exact_response_shape() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/_health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("health response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");

        assert_eq!(value, serde_json::json!({"status": "ok"}));
    }

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
            .oneshot(
                Request::builder()
                    .uri("/logs")
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
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");
        assert_eq!(value["name"], "logs");
        assert_eq!(value["settings"]["primary_time_field"], "@timestamp");
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

    #[tokio::test]
    async fn searches_documents_over_http() {
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

        for request in [
            IndexDocumentRequest {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "level": "info"}),
            },
            IndexDocumentRequest {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "search", "level": "info"}),
            },
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&request).expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("index response");

            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let before_refresh = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            query: Some(SearchQuery::Term(TermQuery {
                                field: "service".to_string(),
                                value: serde_json::json!("billing"),
                            })),
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        let before_refresh_body = before_refresh
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let before_refresh_json: serde_json::Value =
            serde_json::from_slice(&before_refresh_body).expect("deserialize search response");
        assert_eq!(before_refresh_json["hits"]["total"]["value"], 0);

        let refresh_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");
        assert_eq!(refresh_response.status(), StatusCode::OK);

        let term_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            query: Some(SearchQuery::Term(TermQuery {
                                field: "service".to_string(),
                                value: serde_json::json!("billing"),
                            })),
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        assert_eq!(term_response.status(), StatusCode::OK);
        let term_body = term_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let term_text = String::from_utf8(term_body.to_vec()).expect("utf8");
        assert!(term_text.contains("\"value\":1"));
        assert!(term_text.contains("doc-1"));

        let bool_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            query: Some(SearchQuery::Bool(BoolQuery {
                                filter: vec![SearchQuery::Term(TermQuery {
                                    field: "level".to_string(),
                                    value: serde_json::json!("info"),
                                })],
                                ..Default::default()
                            })),
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        assert_eq!(bool_response.status(), StatusCode::OK);
        let bool_body = bool_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let bool_json: serde_json::Value = serde_json::from_slice(&bool_body).expect("json");
        assert_eq!(bool_json["hits"]["total"]["value"], 2);
    }

    #[tokio::test]
    async fn refresh_and_range_queries_work_over_http() {
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

        for request in [
            IndexDocumentRequest {
                id: "doc-1".to_string(),
                source: serde_json::json!({
                    "latency": 42,
                    "timestamp": "2026-03-14T10:00:00Z"
                }),
            },
            IndexDocumentRequest {
                id: "doc-2".to_string(),
                source: serde_json::json!({
                    "latency": 7,
                    "timestamp": "2026-03-14T12:00:00Z"
                }),
            },
        ] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&request).expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("index response");
        }

        let refresh_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");
        assert_eq!(refresh_response.status(), StatusCode::OK);

        let numeric_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            query: Some(SearchQuery::Range(RangeQuery {
                                field: "latency".to_string(),
                                gte: Some(serde_json::json!(10)),
                                gt: None,
                                lte: Some(serde_json::json!(50)),
                                lt: None,
                            })),
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        let numeric_body = numeric_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let numeric_json: serde_json::Value =
            serde_json::from_slice(&numeric_body).expect("deserialize search response");
        assert_eq!(numeric_json["hits"]["total"]["value"], 1);
        assert_eq!(numeric_json["hits"]["hits"][0]["_id"], "doc-1");

        let timestamp_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            query: Some(SearchQuery::Range(RangeQuery {
                                field: "timestamp".to_string(),
                                gte: Some(serde_json::json!("2026-03-14T11:00:00Z")),
                                gt: None,
                                lte: None,
                                lt: Some(serde_json::json!("2026-03-14T13:00:00Z")),
                            })),
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        let timestamp_body = timestamp_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let timestamp_json: serde_json::Value =
            serde_json::from_slice(&timestamp_body).expect("deserialize search response");
        assert_eq!(timestamp_json["hits"]["total"]["value"], 1);
        assert_eq!(timestamp_json["hits"]["hits"][0]["_id"], "doc-2");
    }

    #[tokio::test]
    async fn bulk_ingest_orders_operations_and_requires_refresh() {
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

        let bulk_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_bulk")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&BulkRequest {
                            operations: vec![
                                BulkOperation::Index(BulkIndexOperation {
                                    id: "doc-1".to_string(),
                                    source: serde_json::json!({"service": "billing", "message": "first"}),
                                }),
                                BulkOperation::Index(BulkIndexOperation {
                                    id: "doc-1".to_string(),
                                    source: serde_json::json!({"service": "billing", "message": "second"}),
                                }),
                                BulkOperation::Delete(BulkDeleteOperation {
                                    id: "doc-2".to_string(),
                                }),
                                BulkOperation::Index(BulkIndexOperation {
                                    id: "doc-2".to_string(),
                                    source: serde_json::json!({"service": "search", "message": "survivor"}),
                                }),
                            ],
                        })
                        .expect("serialize bulk request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("bulk response");

        assert_eq!(bulk_response.status(), StatusCode::OK);
        let bulk_body = bulk_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let bulk_json: serde_json::Value =
            serde_json::from_slice(&bulk_body).expect("deserialize bulk");
        assert_eq!(bulk_json["errors"], false);
        assert_eq!(bulk_json["items"].as_array().expect("items array").len(), 4);

        let before_refresh = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest::default())
                            .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        let before_refresh_body = before_refresh
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let before_refresh_json: serde_json::Value =
            serde_json::from_slice(&before_refresh_body).expect("deserialize search response");
        assert_eq!(before_refresh_json["hits"]["total"]["value"], 0);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");

        let after_refresh = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest::default())
                            .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        let after_refresh_body = after_refresh
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let after_refresh_json: serde_json::Value =
            serde_json::from_slice(&after_refresh_body).expect("deserialize search response");
        assert_eq!(after_refresh_json["hits"]["total"]["value"], 2);

        let get_doc = app
            .oneshot(
                Request::builder()
                    .uri("/logs/_doc/doc-1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("get response");

        let get_doc_body = get_doc
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let get_doc_json: serde_json::Value =
            serde_json::from_slice(&get_doc_body).expect("deserialize document");
        assert_eq!(get_doc_json["_source"]["message"], "second");
    }

    #[tokio::test]
    async fn returns_not_found_for_missing_document_and_index() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let missing_index_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/missing/_doc/doc-1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(missing_index_response.status(), StatusCode::NOT_FOUND);

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

        let missing_doc_response = app
            .oneshot(
                Request::builder()
                    .uri("/logs/_doc/missing")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(missing_doc_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn returns_client_error_for_malformed_payloads() {
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

        let malformed_index_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/logs/_doc")
                    .header("content-type", "application/json")
                    .body(Body::from("{not-json"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(malformed_index_response.status().is_client_error());

        let malformed_search_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from("{not-json"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(malformed_search_response.status().is_client_error());
    }

    #[tokio::test]
    async fn returns_not_found_for_search_on_missing_index() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/missing/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest::default())
                            .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn returns_exact_empty_search_response_shape() {
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

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest::default())
                            .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value =
            serde_json::from_slice(&body).expect("deserialize search response");

        assert_eq!(
            value,
            serde_json::json!({
                "hits": {
                    "total": {
                        "value": 0,
                        "relation": "eq"
                    },
                    "hits": []
                },
                "aggregations": {}
            })
        );
    }

    #[tokio::test]
    async fn overwriting_same_document_id_returns_latest_source_over_http() {
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

        for source in [
            serde_json::json!({"message": "first"}),
            serde_json::json!({"message": "second"}),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&IndexDocumentRequest {
                                id: "doc-1".to_string(),
                                source,
                            })
                            .expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("index response");

            assert_eq!(response.status(), StatusCode::CREATED);
        }

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
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");

        assert_eq!(
            value,
            serde_json::json!({
                "_id": "doc-1",
                "found": true,
                "_source": {
                    "message": "second"
                }
            })
        );
    }

    #[tokio::test]
    async fn returns_exact_missing_document_error_shape() {
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

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/logs/_doc/missing")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");

        assert_eq!(
            value,
            serde_json::json!({"error": "index 'document 'missing'' not found"})
        );
    }

    #[tokio::test]
    async fn returns_client_error_for_semantically_invalid_search_payload() {
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

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query":{"term":{"field":"service"}}}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert!(response.status().is_client_error());
    }

    #[tokio::test]
    async fn terms_query_and_sorted_pagination_work_over_http() {
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

        for request in [
            IndexDocumentRequest {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "latency": 30}),
            },
            IndexDocumentRequest {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "search", "latency": 10}),
            },
            IndexDocumentRequest {
                id: "doc-3".to_string(),
                source: serde_json::json!({"service": "auth", "latency": 20}),
            },
        ] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&request).expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("index response");
        }

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");

        let terms_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            query: Some(SearchQuery::Terms(TermsQuery {
                                field: "service".to_string(),
                                values: vec![
                                    serde_json::json!("billing"),
                                    serde_json::json!("auth"),
                                ],
                            })),
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        let terms_body = terms_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let terms_json: serde_json::Value =
            serde_json::from_slice(&terms_body).expect("deserialize search response");
        assert_eq!(terms_json["hits"]["total"]["value"], 2);

        let sorted_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            query: None,
                            from: Some(1),
                            size: Some(1),
                            sort: Some(SortSpec {
                                field: "latency".to_string(),
                                order: SortOrder::Asc,
                            }),
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        let sorted_body = sorted_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let sorted_json: serde_json::Value =
            serde_json::from_slice(&sorted_body).expect("deserialize search response");
        assert_eq!(sorted_json["hits"]["total"]["value"], 3);
        assert_eq!(sorted_json["hits"]["hits"][0]["_id"], "doc-3");
    }

    #[tokio::test]
    async fn invalid_sort_payload_returns_bad_request() {
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

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "sort": {
                                "field": "service",
                                "order": "sideways"
                            }
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert!(response.status().is_client_error());
    }

    #[tokio::test]
    async fn supports_elasticsearch_style_query_shapes_over_http() {
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

        for request in [
            IndexDocumentRequest {
                id: "doc-1".to_string(),
                source: serde_json::json!({
                    "service": "billing",
                    "level": "info",
                    "latency": 10,
                    "timestamp": "2026-03-14T10:10:00Z"
                }),
            },
            IndexDocumentRequest {
                id: "doc-2".to_string(),
                source: serde_json::json!({
                    "service": "search",
                    "level": "info",
                    "latency": 30,
                    "timestamp": "2026-03-14T11:10:00Z"
                }),
            },
            IndexDocumentRequest {
                id: "doc-3".to_string(),
                source: serde_json::json!({
                    "service": "auth",
                    "level": "error",
                    "latency": 20,
                    "timestamp": "2026-03-14T12:10:00Z"
                }),
            },
        ] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&request).expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("index response");
        }

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");

        let term_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "query": {
                                "term": {
                                    "service": "billing"
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");
        let term_body = term_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let term_json: serde_json::Value = serde_json::from_slice(&term_body).expect("json");
        assert_eq!(term_json["hits"]["total"]["value"], 1);
        assert_eq!(term_json["hits"]["hits"][0]["_id"], "doc-1");

        let terms_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "query": {
                                "terms": {
                                    "service": ["billing", "auth"]
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");
        let terms_body = terms_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let terms_json: serde_json::Value = serde_json::from_slice(&terms_body).expect("json");
        assert_eq!(terms_json["hits"]["total"]["value"], 2);

        let range_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "query": {
                                "range": {
                                    "latency": {
                                        "gte": 15,
                                        "lt": 25
                                    }
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");
        let range_body = range_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let range_json: serde_json::Value = serde_json::from_slice(&range_body).expect("json");
        assert_eq!(range_json["hits"]["total"]["value"], 1);
        assert_eq!(range_json["hits"]["hits"][0]["_id"], "doc-3");

        let bool_sort_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"term": {"level": "info"}}
                                    ]
                                }
                            },
                            "sort": [
                                {"latency": {"order": "desc"}}
                            ],
                            "from": 0,
                            "size": 1
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");
        let bool_sort_body = bool_sort_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let bool_sort_json: serde_json::Value =
            serde_json::from_slice(&bool_sort_body).expect("json");
        assert_eq!(bool_sort_json["hits"]["total"]["value"], 2);
        assert_eq!(bool_sort_json["hits"]["hits"][0]["_id"], "doc-2");
    }

    #[tokio::test]
    async fn supports_expanded_bool_query_shapes_over_http() {
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

        for request in [
            IndexDocumentRequest {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "level": "info"}),
            },
            IndexDocumentRequest {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "billing", "level": "error"}),
            },
            IndexDocumentRequest {
                id: "doc-3".to_string(),
                source: serde_json::json!({"service": "search", "level": "info"}),
            },
        ] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&request).expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("index response");
        }

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term": {"service": "billing"}}
                                    ],
                                    "must_not": [
                                        {"term": {"level": "error"}}
                                    ]
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(value["hits"]["total"]["value"], 1);
        assert_eq!(value["hits"]["hits"][0]["_id"], "doc-1");
    }

    #[tokio::test]
    async fn rejects_unsupported_elasticsearch_style_shapes() {
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

        for payload in [
            serde_json::json!({
                "query": {
                    "term": {
                        "service": "billing",
                        "level": "info"
                    }
                }
            }),
            serde_json::json!({
                "query": {
                    "terms": {
                        "service": "billing"
                    }
                }
            }),
            serde_json::json!({
                "query": {
                    "bool": {
                        "must": {"term": {"service": "billing"}}
                    }
                }
            }),
            serde_json::json!({
                "sort": [
                    {"latency": {"order": "asc"}},
                    {"service": {"order": "desc"}}
                ]
            }),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/logs/_search")
                        .header("content-type", "application/json")
                        .body(Body::from(payload.to_string()))
                        .expect("request"),
                )
                .await
                .expect("response");

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn strict_mode_and_mapping_conflicts_return_bad_request() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/strict-logs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&CreateIndexRequest {
                            settings: IndexSettings {
                                mapping_mode: cloudsearch_common::MappingMode::Strict,
                                primary_time_field: None,
                            },
                        })
                        .expect("serialize create request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("create response");

        let strict_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/strict-logs/_doc")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&IndexDocumentRequest {
                            id: "doc-1".to_string(),
                            source: serde_json::json!({"service": "billing"}),
                        })
                        .expect("serialize index request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(strict_response.status(), StatusCode::BAD_REQUEST);

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

        for request in [
            IndexDocumentRequest {
                id: "doc-1".to_string(),
                source: serde_json::json!({"meta": {"host": "a"}}),
            },
            IndexDocumentRequest {
                id: "doc-2".to_string(),
                source: serde_json::json!({"meta": "not-an-object"}),
            },
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&request).expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("response");

            if request.id == "doc-1" {
                assert_eq!(response.status(), StatusCode::CREATED);
            } else {
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            }
        }

        let array_response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/logs/_doc")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&IndexDocumentRequest {
                            id: "doc-3".to_string(),
                            source: serde_json::json!({"services": ["billing", "search"]}),
                        })
                        .expect("serialize index request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(array_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn bulk_returns_exact_success_shape() {
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

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_bulk")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&BulkRequest {
                            operations: vec![BulkOperation::Index(BulkIndexOperation {
                                id: "doc-1".to_string(),
                                source: serde_json::json!({"service": "billing"}),
                            })],
                        })
                        .expect("serialize bulk request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("bulk response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize bulk");

        assert_eq!(
            value,
            serde_json::json!({
                "errors": false,
                "items": [
                    {
                        "index": {
                            "_id": "doc-1",
                            "result": "created"
                        }
                    }
                ]
            })
        );
    }

    #[tokio::test]
    async fn successful_search_response_shape_is_exact_for_paginated_sort() {
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

        for request in [
            IndexDocumentRequest {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "latency": 30}),
            },
            IndexDocumentRequest {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "search", "latency": 10}),
            },
            IndexDocumentRequest {
                id: "doc-3".to_string(),
                source: serde_json::json!({"service": "auth", "latency": 20}),
            },
        ] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&request).expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("index response");
        }

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            from: Some(1),
                            size: Some(1),
                            sort: Some(SortSpec {
                                field: "latency".to_string(),
                                order: SortOrder::Asc,
                            }),
                            aggs: None,
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize search");

        assert_eq!(
            value,
            serde_json::json!({
                "hits": {
                    "total": {
                        "value": 3,
                        "relation": "eq"
                    },
                    "hits": [
                        {
                            "_id": "doc-3",
                            "_source": {
                                "service": "auth",
                                "latency": 20
                            }
                        }
                    ]
                },
                "aggregations": {}
            })
        );
    }

    #[tokio::test]
    async fn terms_and_stats_aggregations_work_over_http() {
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

        for request in [
            IndexDocumentRequest {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "latency": 10, "level": "info"}),
            },
            IndexDocumentRequest {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "billing", "latency": 20, "level": "error"}),
            },
            IndexDocumentRequest {
                id: "doc-3".to_string(),
                source: serde_json::json!({"service": "search", "latency": 30, "level": "info"}),
            },
        ] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&request).expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("index response");
        }

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            query: Some(SearchQuery::Term(TermQuery {
                                field: "level".to_string(),
                                value: serde_json::json!("info"),
                            })),
                            aggs: Some(std::collections::BTreeMap::from([
                                (
                                    "services".to_string(),
                                    AggregationRequest::Terms(TermsAggregationRequest {
                                        field: "service".to_string(),
                                    }),
                                ),
                                (
                                    "latency_stats".to_string(),
                                    AggregationRequest::Stats(StatsAggregationRequest {
                                        field: "latency".to_string(),
                                    }),
                                ),
                            ])),
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize search");

        assert_eq!(value["hits"]["total"]["value"], 2);
        assert_eq!(
            value["aggregations"]["services"]["buckets"][0]["key"],
            "billing"
        );
        assert_eq!(
            value["aggregations"]["services"]["buckets"][0]["doc_count"],
            1
        );
        assert_eq!(value["aggregations"]["latency_stats"]["count"], 2);
        assert_eq!(value["aggregations"]["latency_stats"]["sum"], 40.0);
        assert_eq!(value["aggregations"]["latency_stats"]["avg"], 20.0);
    }

    #[tokio::test]
    async fn date_histogram_aggregation_works_over_http() {
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

        for request in [
            IndexDocumentRequest {
                id: "doc-1".to_string(),
                source: serde_json::json!({"timestamp": "2026-03-14T10:05:00Z", "service": "billing"}),
            },
            IndexDocumentRequest {
                id: "doc-2".to_string(),
                source: serde_json::json!({"timestamp": "2026-03-14T10:45:00Z", "service": "billing"}),
            },
            IndexDocumentRequest {
                id: "doc-3".to_string(),
                source: serde_json::json!({"timestamp": "2026-03-14T11:15:00Z", "service": "search"}),
            },
        ] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs/_doc")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&request).expect("serialize index request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("index response");
        }

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            aggs: Some(std::collections::BTreeMap::from([(
                                "events_over_time".to_string(),
                                AggregationRequest::DateHistogram(
                                    DateHistogramAggregationRequest {
                                        field: "timestamp".to_string(),
                                        interval: DateHistogramInterval::Hour,
                                    },
                                ),
                            )])),
                            ..Default::default()
                        })
                        .expect("serialize search request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("search response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize search");

        assert_eq!(
            value["aggregations"]["events_over_time"]["buckets"][0]["key"],
            "2026-03-14T10:00:00Z"
        );
        assert_eq!(
            value["aggregations"]["events_over_time"]["buckets"][0]["doc_count"],
            2
        );
        assert_eq!(
            value["aggregations"]["events_over_time"]["buckets"][1]["key"],
            "2026-03-14T11:00:00Z"
        );
        assert_eq!(
            value["aggregations"]["events_over_time"]["buckets"][1]["doc_count"],
            1
        );
    }

    #[tokio::test]
    async fn flush_persists_state_and_returns_response_over_http() {
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

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/logs/_doc")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&IndexDocumentRequest {
                            id: "doc-1".to_string(),
                            source: serde_json::json!({"message": "visible"}),
                        })
                        .expect("serialize index request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("index response");

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("refresh response");

        let flush_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_flush")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("flush response");

        assert_eq!(flush_response.status(), StatusCode::OK);
        let flush_body = flush_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let flush_json: serde_json::Value =
            serde_json::from_slice(&flush_body).expect("deserialize flush response");
        assert_eq!(flush_json["result"], "flushed");
        assert_eq!(flush_json["flushed_documents"], 1);
        assert_eq!(flush_json["sequence_number"], 1);
    }

    #[tokio::test]
    async fn flush_missing_index_returns_not_found() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/missing/_flush")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
