use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use cloudsearch_common::{
    AggregationRequest, AggregationResult, BoolQuery, BulkItem, BulkItemResult, BulkRequest,
    BulkResponse, CloudSearchError, CreateIndexRequest, CreateSnapshotResponse,
    DateHistogramAggregationResult, ErrorResponse, FlushResponse, HealthResponse, IndexDocument,
    IndexDocumentRequest, ListSnapshotsResponse, MatchQuery, MergeResponse,
    MultiSearchItemResponse, MultiSearchRequest, MultiSearchResponse, PhraseQuery, PrefixQuery,
    RangeQuery, RefreshResponse, SearchHit, SearchQuery, SearchRequest, SearchResponse, SortSpec,
    StatsAggregationResult, TermQuery, TermsAggregationResult, TermsQuery, UpdateSettingsRequest,
    WildcardQuery,
};
use cloudsearch_index::{IndexCatalog, IndexRegistry};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};
use tower_http::trace::TraceLayer;

#[derive(serde::Serialize)]
struct CompatIndexDocumentResponse {
    #[serde(rename = "_id")]
    id: String,
    result: &'static str,
}

mod query_string;

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
    #[serde(rename = "_score", skip_serializing_if = "Option::is_none")]
    score: Option<f32>,
    #[serde(rename = "_source")]
    source: Value,
    #[serde(rename = "highlight", skip_serializing_if = "Option::is_none")]
    highlight: Option<std::collections::BTreeMap<String, Vec<String>>>,
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

#[derive(serde::Serialize)]
struct CompatDeleteIndexResponse {
    result: &'static str,
}

#[derive(serde::Serialize)]
struct CompatDeleteSnapshotResponse {
    result: &'static str,
}

#[derive(Default)]
struct MetricsState {
    request_counts: BTreeMap<(String, String, u16), u64>,
    request_duration_sum_secs: BTreeMap<(String, String), f64>,
    request_duration_count: BTreeMap<(String, String), u64>,
    index_writes_total: u64,
    bulk_requests_total: u64,
    bulk_operations_total: u64,
    search_requests_total: u64,
    refresh_total: u64,
    flush_total: u64,
    merge_total: u64,
    delete_index_total: u64,
}

impl MetricsState {
    fn record_request(
        &mut self,
        route: &str,
        method: &str,
        status: StatusCode,
        duration_secs: f64,
    ) {
        *self
            .request_counts
            .entry((route.to_string(), method.to_string(), status.as_u16()))
            .or_default() += 1;
        *self
            .request_duration_sum_secs
            .entry((route.to_string(), method.to_string()))
            .or_default() += duration_secs;
        *self
            .request_duration_count
            .entry((route.to_string(), method.to_string()))
            .or_default() += 1;
    }

    fn render(
        &self,
        open_indexes: usize,
        index_metrics: &[(String, cloudsearch_index::IndexMetrics)],
    ) -> String {
        let mut lines = Vec::new();

        for ((route, method, status), value) in &self.request_counts {
            lines.push(format!(
                "cloudsearch_requests_total{{route=\"{route}\",method=\"{method}\",status=\"{status}\"}} {value}"
            ));
        }

        for ((route, method), value) in &self.request_duration_sum_secs {
            lines.push(format!(
                "cloudsearch_request_duration_seconds_sum{{route=\"{route}\",method=\"{method}\"}} {value}"
            ));
        }

        for ((route, method), value) in &self.request_duration_count {
            lines.push(format!(
                "cloudsearch_request_duration_seconds_count{{route=\"{route}\",method=\"{method}\"}} {value}"
            ));
        }

        lines.push(format!(
            "cloudsearch_index_writes_total {}",
            self.index_writes_total
        ));
        lines.push(format!(
            "cloudsearch_bulk_requests_total {}",
            self.bulk_requests_total
        ));
        lines.push(format!(
            "cloudsearch_bulk_operations_total {}",
            self.bulk_operations_total
        ));
        lines.push(format!(
            "cloudsearch_search_requests_total {}",
            self.search_requests_total
        ));
        lines.push(format!("cloudsearch_refresh_total {}", self.refresh_total));
        lines.push(format!("cloudsearch_flush_total {}", self.flush_total));
        lines.push(format!("cloudsearch_merge_total {}", self.merge_total));
        lines.push(format!(
            "cloudsearch_delete_index_total {}",
            self.delete_index_total
        ));
        lines.push(format!("cloudsearch_open_indexes {open_indexes}"));

        for (index_name, metrics) in index_metrics {
            lines.push(format!(
                "cloudsearch_index_documents_total{{index=\"{}\"}} {}",
                index_name, metrics.document_count
            ));
            lines.push(format!(
                "cloudsearch_index_pending_ops{{index=\"{}\"}} {}",
                index_name, metrics.pending_operations
            ));
            lines.push(format!(
                "cloudsearch_index_last_sequence_number{{index=\"{}\"}} {}",
                index_name, metrics.last_sequence_number
            ));
        }

        lines.sort();
        lines.join("\n") + "\n"
    }
}

#[derive(Clone)]
pub struct ApiState {
    registry: Arc<IndexRegistry>,
    metrics: Arc<Mutex<MetricsState>>,
}

impl ApiState {
    #[must_use]
    pub fn new(registry: Arc<IndexRegistry>) -> Self {
        Self {
            registry,
            metrics: Arc::new(Mutex::new(MetricsState::default())),
        }
    }

    fn metrics(&self) -> std::sync::MutexGuard<'_, MetricsState> {
        self.metrics.lock().expect("metrics mutex poisoned")
    }
}

pub fn router(registry: Arc<IndexCatalog>) -> Router {
    router_with_registry(Arc::new(IndexRegistry::new(registry)))
}

pub fn router_with_registry(registry: Arc<IndexRegistry>) -> Router {
    Router::new()
        .route("/_health", get(health))
        .route("/metrics", get(metrics))
        .route("/_msearch", post(multi_search))
        .route(
            "/{index}",
            put(create_index).get(get_index).delete(delete_index),
        )
        .route("/{index}/_bulk", put(bulk_index).post(bulk_index))
        .route("/{index}/_doc", put(index_document).post(index_document))
        .route("/{index}/_doc/{id}", get(get_document))
        .route("/{index}/_flush", put(flush_index).post(flush_index))
        .route("/{index}/_merge", post(merge_index))
        .route("/{index}/_refresh", put(refresh_index).post(refresh_index))
        .route(
            "/{index}/_search",
            get(search_index_get).post(search_index).put(search_index),
        )
        .route("/{index}/_settings", put(update_index_settings))
        .route("/{index}/_snapshot", get(list_snapshots))
        .route(
            "/{index}/_snapshot/{name}",
            post(create_snapshot).get(get_snapshot),
        )
        .route("/{index}/_snapshot/{name}/_restore", post(restore_snapshot))
        .route("/{index}/_snapshot/{name}", delete(delete_snapshot))
        .layer(TraceLayer::new_for_http())
        .with_state(ApiState::new(registry))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn metrics(State(state): State<ApiState>) -> String {
    let open_indexes = state.registry.cached_handle_count().await;
    let index_metrics = state.registry.index_metrics().await;
    state.metrics().render(open_indexes, &index_metrics)
}

async fn create_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
    Json(request): Json<CreateIndexRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let metadata = state.registry.create_index(&index, request).await?;
    state.metrics().record_request(
        "index_create",
        "PUT",
        StatusCode::CREATED,
        started_at.elapsed().as_secs_f64(),
    );
    Ok((StatusCode::CREATED, Json(metadata)))
}

async fn get_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let metadata = state.registry.get_index(&index).await?;
    state.metrics().record_request(
        "index_get",
        "GET",
        StatusCode::OK,
        started_at.elapsed().as_secs_f64(),
    );
    Ok((StatusCode::OK, Json(metadata)))
}

async fn delete_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    state.registry.delete_index(&index).await?;
    {
        let mut metrics = state.metrics();
        metrics.delete_index_total += 1;
        metrics.record_request(
            "index_delete",
            "DELETE",
            StatusCode::OK,
            started_at.elapsed().as_secs_f64(),
        );
    }

    Ok((
        StatusCode::OK,
        Json(CompatDeleteIndexResponse { result: "deleted" }),
    ))
}

async fn update_index_settings(
    State(state): State<ApiState>,
    Path(index): Path<String>,
    Json(request): Json<UpdateSettingsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let metadata = state
        .registry
        .update_index_settings(&index, request)
        .await?;
    state.metrics().record_request(
        "index_settings_update",
        "PUT",
        StatusCode::OK,
        started_at.elapsed().as_secs_f64(),
    );
    Ok((StatusCode::OK, Json(metadata)))
}

async fn index_document(
    State(state): State<ApiState>,
    Path(index): Path<String>,
    Json(request): Json<IndexDocumentRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
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
    {
        let mut metrics = state.metrics();
        metrics.index_writes_total += 1;
        metrics.record_request(
            "document_index",
            "PUT",
            StatusCode::CREATED,
            started_at.elapsed().as_secs_f64(),
        );
    }

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
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let mut handle = handle.lock().await;
    let operation_count = request.operations.len() as u64;
    let response = handle.bulk_apply(request).await?;
    {
        let mut metrics = state.metrics();
        metrics.bulk_requests_total += 1;
        metrics.bulk_operations_total += operation_count;
        metrics.record_request(
            "bulk_index",
            "POST",
            StatusCode::OK,
            started_at.elapsed().as_secs_f64(),
        );
    }
    let elapsed = started_at.elapsed();
    if elapsed.as_millis() > 200 {
        tracing::warn!(index = %index, operation_count, duration_ms = elapsed.as_millis(), "slow bulk");
    }
    Ok((StatusCode::OK, Json(to_compat_bulk_response(response))))
}

async fn get_document(
    State(state): State<ApiState>,
    Path((index, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let handle = handle.lock().await;
    let document = handle
        .get_document(&id)
        .ok_or_else(|| ApiError(CloudSearchError::DocumentNotFound(id.clone())))?;
    state.metrics().record_request(
        "document_get",
        "GET",
        StatusCode::OK,
        started_at.elapsed().as_secs_f64(),
    );

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
    let started_at = Instant::now();
    let request = parse_search_request(&request)?;
    let handle = state.registry.index_handle(&index).await?;
    let handle = handle.lock().await;
    handle.validate_search_request(&request)?;
    {
        let mut metrics = state.metrics();
        metrics.search_requests_total += 1;
        metrics.record_request(
            "search",
            "POST",
            StatusCode::OK,
            started_at.elapsed().as_secs_f64(),
        );
    }
    let elapsed = started_at.elapsed();
    if elapsed.as_millis() > 50 {
        tracing::warn!(index = %index, duration_ms = elapsed.as_millis(), "slow query");
    }
    let result = handle.search(&request);
    Ok((StatusCode::OK, Json(to_compat_search_response(result))))
}

async fn multi_search(
    State(state): State<ApiState>,
    Json(request): Json<MultiSearchRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let mut responses = Vec::with_capacity(request.searches.len());

    for item in request.searches {
        let index_name = item.index.clone();
        let handle = match state.registry.index_handle(&index_name).await {
            Ok(h) => h,
            Err(e) => {
                responses.push(MultiSearchItemResponse {
                    index: index_name,
                    status: StatusCode::NOT_FOUND.as_u16(),
                    response: None,
                    error: Some(e.to_string()),
                });
                continue;
            }
        };
        let handle = handle.lock().await;
        match handle.validate_search_request(&item.request) {
            Ok(()) => {}
            Err(e) => {
                responses.push(MultiSearchItemResponse {
                    index: index_name,
                    status: StatusCode::BAD_REQUEST.as_u16(),
                    response: None,
                    error: Some(e.to_string()),
                });
                continue;
            }
        }
        let result = handle.search(&item.request);
        responses.push(MultiSearchItemResponse {
            index: index_name,
            status: StatusCode::OK.as_u16(),
            response: Some(result),
            error: None,
        });
    }

    let elapsed = started_at.elapsed();
    if elapsed.as_millis() > 50 {
        tracing::warn!(duration_ms = elapsed.as_millis(), "slow multi_search");
    }
    Ok((StatusCode::OK, Json(MultiSearchResponse { responses })))
}

async fn search_index_get(
    State(state): State<ApiState>,
    Path(index): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let mut request = SearchRequest::default();

    if let Some(q) = params.get("q") {
        let parsed = crate::query_string::parse_query_string(q)
            .map_err(|e| ApiError(CloudSearchError::InvalidSearchRequest(e.to_string())))?;
        request.query = Some(parsed);
    }

    let handle = state.registry.index_handle(&index).await?;
    let handle = handle.lock().await;
    handle.validate_search_request(&request)?;
    {
        let mut metrics = state.metrics();
        metrics.search_requests_total += 1;
        metrics.record_request(
            "search",
            "GET",
            StatusCode::OK,
            started_at.elapsed().as_secs_f64(),
        );
    }
    let elapsed = started_at.elapsed();
    if elapsed.as_millis() > 50 {
        tracing::warn!(index = %index, duration_ms = elapsed.as_millis(), "slow query");
    }
    Ok((
        StatusCode::OK,
        Json(to_compat_search_response(handle.search(&request))),
    ))
}

fn parse_search_request(value: &Value) -> Result<SearchRequest, ApiError> {
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
        "prefix" => Ok(SearchQuery::Prefix(parse_prefix_query(body)?)),
        "wildcard" => Ok(SearchQuery::Wildcard(parse_wildcard_query(body)?)),
        "match" => Ok(SearchQuery::Match(parse_match_query(body)?)),
        "phrase" => Ok(SearchQuery::Phrase(parse_phrase_query(body)?)),
        other => Err(ApiError(CloudSearchError::InvalidSearchRequest(format!(
            "unsupported query clause '{other}'"
        )))),
    }
}

fn parse_term_query(value: &Value) -> Result<TermQuery, ApiError> {
    use cloudsearch_common::Fuzziness;

    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "term query must be a JSON object".to_string(),
        ))
    })?;

    // Extract optional fuzziness before consuming the object
    let fuzziness = object
        .get("fuzziness")
        .map(|fv| -> Result<Fuzziness, ApiError> {
            match fv {
                Value::String(s) if s.eq_ignore_ascii_case("auto") => Ok(Fuzziness::Auto),
                Value::Number(n) if n.is_u64() => {
                    let n = usize::try_from(n.as_u64().unwrap()).map_err(|_| {
                        ApiError(CloudSearchError::InvalidSearchRequest(
                            "fuzziness value is too large".to_string(),
                        ))
                    })?;
                    Ok(Fuzziness::Exact(n))
                }
                _ => Err(ApiError(CloudSearchError::InvalidSearchRequest(
                    "fuzziness must be 'auto' or a non-negative integer".to_string(),
                ))),
            }
        })
        .transpose()?;

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
            fuzziness,
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
        fuzziness,
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

fn parse_prefix_query(value: &Value) -> Result<PrefixQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "prefix query must be a JSON object".to_string(),
        ))
    })?;

    if object.contains_key("field") && object.contains_key("value") {
        if object.len() != 2 {
            return Err(ApiError(CloudSearchError::InvalidSearchRequest(
                "prefix query explicit form must contain only 'field' and 'value'".to_string(),
            )));
        }
        let field = object.get("field").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "prefix query requires string 'field'".to_string(),
            ))
        })?;
        let value = object.get("value").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "prefix query requires string 'value'".to_string(),
            ))
        })?;
        return Ok(PrefixQuery {
            field: field.to_string(),
            value: value.to_string(),
        });
    }

    // Malformed explicit form: has field OR value but not both
    if object.contains_key("field") || object.contains_key("value") {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "prefix query explicit form requires both 'field' and 'value'".to_string(),
        )));
    }

    if object.len() != 1 {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "prefix query must contain exactly one field".to_string(),
        )));
    }

    let (field, raw_value) = object.iter().next().expect("single prefix field");
    let value = raw_value.as_str().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "prefix value must be a string".to_string(),
        ))
    })?;
    Ok(PrefixQuery {
        field: field.clone(),
        value: value.to_string(),
    })
}

fn parse_wildcard_query(value: &Value) -> Result<WildcardQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "wildcard query must be a JSON object".to_string(),
        ))
    })?;

    if object.contains_key("field") && object.contains_key("value") {
        if object.len() != 2 {
            return Err(ApiError(CloudSearchError::InvalidSearchRequest(
                "wildcard query explicit form must contain only 'field' and 'value'".to_string(),
            )));
        }
        let field = object.get("field").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "wildcard query requires string 'field'".to_string(),
            ))
        })?;
        let value = object.get("value").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "wildcard query requires string 'value'".to_string(),
            ))
        })?;
        return Ok(WildcardQuery {
            field: field.to_string(),
            value: value.to_string(),
        });
    }

    // Malformed explicit form: has field OR value but not both (XOR check)
    let has_field = object.contains_key("field");
    let has_value = object.contains_key("value");
    if has_field != has_value {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "wildcard query explicit form requires both 'field' and 'value'".to_string(),
        )));
    }

    if object.len() != 1 {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "wildcard query must contain exactly one field".to_string(),
        )));
    }

    let (field, raw_value) = object.iter().next().expect("single wildcard field");
    let value = raw_value.as_str().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "wildcard value must be a string".to_string(),
        ))
    })?;
    Ok(WildcardQuery {
        field: field.clone(),
        value: value.to_string(),
    })
}

fn parse_match_query(value: &Value) -> Result<MatchQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "match query must be a JSON object".to_string(),
        ))
    })?;

    if object.contains_key("field") && object.contains_key("value") {
        if object.len() != 2 {
            return Err(ApiError(CloudSearchError::InvalidSearchRequest(
                "match query explicit form must contain only 'field' and 'value'".to_string(),
            )));
        }
        let field = object.get("field").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "match query requires string 'field'".to_string(),
            ))
        })?;
        let value = object.get("value").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "match query requires string 'value'".to_string(),
            ))
        })?;
        return Ok(MatchQuery {
            field: field.to_string(),
            value: value.to_string(),
        });
    }

    let has_field = object.contains_key("field");
    let has_value = object.contains_key("value");
    if has_field != has_value {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "match query explicit form requires both 'field' and 'value'".to_string(),
        )));
    }

    if object.len() != 1 {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "match query must contain exactly one field".to_string(),
        )));
    }

    let (field, raw_value) = object.iter().next().expect("single match field");
    let value = raw_value.as_str().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "match query value must be a string".to_string(),
        ))
    })?;
    Ok(MatchQuery {
        field: field.clone(),
        value: value.to_string(),
    })
}

fn parse_phrase_query(value: &Value) -> Result<PhraseQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "phrase query must be a JSON object".to_string(),
        ))
    })?;

    if object.contains_key("field") && object.contains_key("value") {
        if object.len() != 2 {
            return Err(ApiError(CloudSearchError::InvalidSearchRequest(
                "phrase query explicit form must contain only 'field' and 'value'".to_string(),
            )));
        }
        let field = object.get("field").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "phrase query requires string 'field'".to_string(),
            ))
        })?;
        let value = object.get("value").and_then(Value::as_str).ok_or_else(|| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "phrase query requires string 'value'".to_string(),
            ))
        })?;
        return Ok(PhraseQuery {
            field: field.to_string(),
            value: value.to_string(),
        });
    }

    let has_field = object.contains_key("field");
    let has_value = object.contains_key("value");
    if has_field != has_value {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "phrase query explicit form requires both 'field' and 'value'".to_string(),
        )));
    }

    if object.len() != 1 {
        return Err(ApiError(CloudSearchError::InvalidSearchRequest(
            "phrase query must contain exactly one field".to_string(),
        )));
    }

    let (field, raw_value) = object.iter().next().expect("single phrase field");
    let value = raw_value.as_str().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "phrase query value must be a string".to_string(),
        ))
    })?;
    Ok(PhraseQuery {
        field: field.clone(),
        value: value.to_string(),
    })
}

fn parse_bool_query(value: &Value) -> Result<BoolQuery, ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError(CloudSearchError::InvalidSearchRequest(
            "bool query must be a JSON object".to_string(),
        ))
    })?;

    let allowed = ["must", "should", "filter", "must_not", "minimum_should_match"];
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(ApiError(CloudSearchError::InvalidSearchRequest(format!(
                "unsupported bool clause '{key}'"
            ))));
        }
    }

    let minimum_should_match = object
        .get("minimum_should_match")
        .and_then(serde_json::Value::as_u64)
        .map(|v| u32::try_from(v).map_err(|_| {
            ApiError(CloudSearchError::InvalidSearchRequest(
                "minimum_should_match must be a non-negative integer".to_string(),
            ))
        }))
        .transpose()?;

    Ok(BoolQuery {
        must: parse_bool_clause_array(object.get("must"), "bool.must")?,
        should: parse_bool_clause_array(object.get("should"), "bool.should")?,
        filter: parse_bool_clause_array(object.get("filter"), "bool.filter")?,
        must_not: parse_bool_clause_array(object.get("must_not"), "bool.must_not")?,
        minimum_should_match,
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
        score: hit.score,
        source: hit.source,
        highlight: hit.highlight,
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
        AggregationResult::Terms(result) => terms_aggregation_to_json(&result),
        AggregationResult::Stats(result) => stats_aggregation_to_json(&result),
        AggregationResult::DateHistogram(result) => date_histogram_aggregation_to_json(&result),
    }
}

fn terms_aggregation_to_json(result: &TermsAggregationResult) -> Value {
    serde_json::json!({
        "buckets": result.buckets
    })
}

fn stats_aggregation_to_json(result: &StatsAggregationResult) -> Value {
    serde_json::json!({
        "count": result.count,
        "min": result.min,
        "max": result.max,
        "avg": result.avg,
        "sum": result.sum,
    })
}

fn date_histogram_aggregation_to_json(result: &DateHistogramAggregationResult) -> Value {
    serde_json::json!({
        "buckets": result.buckets
    })
}

async fn refresh_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let mut handle = handle.lock().await;
    let refreshed_documents = handle.refresh().await?;
    {
        let mut metrics = state.metrics();
        metrics.refresh_total += 1;
        metrics.record_request(
            "refresh",
            "POST",
            StatusCode::OK,
            started_at.elapsed().as_secs_f64(),
        );
    }

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
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let mut handle = handle.lock().await;
    let response = handle.flush().await?;
    {
        let mut metrics = state.metrics();
        metrics.flush_total += 1;
        metrics.record_request(
            "flush",
            "POST",
            StatusCode::OK,
            started_at.elapsed().as_secs_f64(),
        );
    }

    Ok((StatusCode::OK, Json::<FlushResponse>(response)))
}

async fn merge_index(
    State(state): State<ApiState>,
    Path(index): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let mut handle = handle.lock().await;
    let response = handle.merge().await?;
    {
        let mut metrics = state.metrics();
        metrics.merge_total += 1;
        metrics.record_request(
            "merge",
            "POST",
            StatusCode::OK,
            started_at.elapsed().as_secs_f64(),
        );
    }

    Ok((StatusCode::OK, Json::<MergeResponse>(response)))
}

async fn create_snapshot(
    State(state): State<ApiState>,
    Path((index, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let handle = handle.lock().await;
    let response = handle.create_snapshot(&name).await?;
    state.metrics().record_request(
        "snapshot_create",
        "POST",
        StatusCode::CREATED,
        started_at.elapsed().as_secs_f64(),
    );
    Ok((
        StatusCode::CREATED,
        Json::<CreateSnapshotResponse>(response),
    ))
}

async fn list_snapshots(
    State(state): State<ApiState>,
    Path(index): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let handle = handle.lock().await;
    let snapshots = handle.list_snapshots().await?;
    state.metrics().record_request(
        "snapshot_list",
        "GET",
        StatusCode::OK,
        started_at.elapsed().as_secs_f64(),
    );
    Ok((StatusCode::OK, Json(ListSnapshotsResponse { snapshots })))
}

async fn get_snapshot(
    State(state): State<ApiState>,
    Path((index, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let handle = handle.lock().await;
    let snapshot = handle.get_snapshot(&name).await?;
    if let Some(meta) = snapshot {
        state.metrics().record_request(
            "snapshot_get",
            "GET",
            StatusCode::OK,
            started_at.elapsed().as_secs_f64(),
        );
        Ok((StatusCode::OK, Json(meta)))
    } else {
        state.metrics().record_request(
            "snapshot_get",
            "GET",
            StatusCode::NOT_FOUND,
            started_at.elapsed().as_secs_f64(),
        );
        Err(ApiError(CloudSearchError::SnapshotNotFound(name.clone())))
    }
}

async fn delete_snapshot(
    State(state): State<ApiState>,
    Path((index, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let handle = handle.lock().await;
    handle.delete_snapshot(&name).await?;
    state.metrics().record_request(
        "snapshot_delete",
        "DELETE",
        StatusCode::OK,
        started_at.elapsed().as_secs_f64(),
    );
    Ok((
        StatusCode::OK,
        Json(CompatDeleteSnapshotResponse { result: "deleted" }),
    ))
}

async fn restore_snapshot(
    State(state): State<ApiState>,
    Path((index, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let started_at = Instant::now();
    let handle = state.registry.index_handle(&index).await?;
    let mut handle = handle.lock().await;
    let response = handle.restore_snapshot(&name).await?;
    state.metrics().record_request(
        "snapshot_restore",
        "POST",
        StatusCode::OK,
        started_at.elapsed().as_secs_f64(),
    );
    Ok((
        StatusCode::OK,
        Json::<cloudsearch_common::RestoreResponse>(response),
    ))
}

#[derive(Debug)]
pub struct ApiError(CloudSearchError);

impl From<CloudSearchError> for ApiError {
    fn from(value: CloudSearchError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            CloudSearchError::IndexAlreadyExists(_) => StatusCode::CONFLICT,
            CloudSearchError::IndexNotFound(_)
            | CloudSearchError::DocumentNotFound(_)
            | CloudSearchError::SnapshotNotFound(_) => StatusCode::NOT_FOUND,
            CloudSearchError::InvalidIndexName(_)
            | CloudSearchError::InvalidSearchRequest(_)
            | CloudSearchError::MappingConflict(_)
            | CloudSearchError::UnknownFieldRejected(_)
            | CloudSearchError::UnsupportedArrayField(_)
            | CloudSearchError::MappingLimitExceeded(_)
            | CloudSearchError::InvalidNamespace(_)
            | CloudSearchError::ResourceLimitExceeded(_)
            | CloudSearchError::Serde(_) => StatusCode::BAD_REQUEST,
            CloudSearchError::InvalidWalRecord(_)
            | CloudSearchError::WalChecksumMismatch
            | CloudSearchError::Io(_) => StatusCode::SERVICE_UNAVAILABLE,
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
        IndexDocumentRequest, IndexSettings, MappingMode, RangeQuery, SearchQuery, SearchRequest,
        SortOrder, SortSpec, StatsAggregationRequest, TermQuery, TermsAggregationRequest,
        TermsQuery,
    };
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tower::ServiceExt;

    #[allow(dead_code)]
    async fn test_api() -> (TempDir, Arc<IndexCatalog>, axum::Router) {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog.clone());
        (temp_dir, catalog, app)
    }

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

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn metrics_endpoint_exposes_request_and_operation_counters() {
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            source: serde_json::json!({"service": "billing"}),
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
                    .uri("/logs/_bulk")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&BulkRequest {
                            operations: vec![BulkOperation::Index(BulkIndexOperation {
                                id: "doc-2".to_string(),
                                source: serde_json::json!({"service": "search"}),
                            })],
                        })
                        .expect("serialize bulk request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("bulk response");

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

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::json!({}).to_string()))
                    .expect("request"),
            )
            .await
            .expect("search response");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("metrics response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).expect("utf8");

        assert!(text.contains("cloudsearch_index_writes_total 1"));
        assert!(text.contains("cloudsearch_bulk_requests_total 1"));
        assert!(text.contains("cloudsearch_bulk_operations_total 1"));
        assert!(text.contains("cloudsearch_search_requests_total 1"));
        assert!(text.contains("cloudsearch_refresh_total 1"));
        assert!(text.contains("cloudsearch_open_indexes 1"));
        assert!(text.contains(
            "cloudsearch_requests_total{route=\"index_create\",method=\"PUT\",status=\"201\"} 1"
        ));
        assert!(text.contains(
            "cloudsearch_requests_total{route=\"search\",method=\"POST\",status=\"200\"} 1"
        ));
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
                                mapping_mode: MappingMode::default(),
                                primary_time_field: Some("@timestamp".to_string()),
                                namespace: None,
                                retention_secs: None,
                                merge_threshold_docs: None,
                            },
                            ..Default::default()
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
    async fn rejects_invalid_namespace_over_http() {
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
                                mapping_mode: MappingMode::default(),
                                primary_time_field: None,
                                namespace: Some("tenant@invalid".to_string()),
                                retention_secs: None,
                                merge_threshold_docs: None,
                            },
                            ..Default::default()
                        })
                        .expect("serialize create request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("create response");

        assert_eq!(create_response.status(), StatusCode::BAD_REQUEST);

        let body = create_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");
        let error_msg = value["error"].as_str().expect("error message string");
        assert!(
            error_msg.contains("invalid namespace"),
            "expected error about invalid namespace, got: {error_msg}"
        );
    }

    #[tokio::test]
    async fn rejects_empty_namespace_over_http() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/logs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&CreateIndexRequest {
                            settings: IndexSettings {
                                mapping_mode: MappingMode::default(),
                                primary_time_field: None,
                                namespace: Some(String::new()),
                                retention_secs: None,
                                merge_threshold_docs: None,
                            },
                            ..Default::default()
                        })
                        .expect("serialize"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert!(response.status().is_client_error());
    }

    #[tokio::test]
    async fn rejects_too_long_namespace_over_http() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/logs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&CreateIndexRequest {
                            settings: IndexSettings {
                                mapping_mode: MappingMode::default(),
                                primary_time_field: None,
                                namespace: Some("a".repeat(65)),
                                retention_secs: None,
                                merge_threshold_docs: None,
                            },
                            ..Default::default()
                        })
                        .expect("serialize"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert!(response.status().is_client_error());
    }

    #[tokio::test]
    async fn accepts_valid_namespace_over_http() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/logs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&CreateIndexRequest {
                            settings: IndexSettings {
                                mapping_mode: MappingMode::default(),
                                primary_time_field: None,
                                namespace: Some("tenant-abc".to_string()),
                                retention_secs: None,
                                merge_threshold_docs: None,
                            },
                            ..Default::default()
                        })
                        .expect("serialize"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn deletes_index_over_http_and_allows_recreate() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        for _ in 0..2 {
            let create_response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/logs")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&CreateIndexRequest {
                                settings: IndexSettings::default(),
                                ..Default::default()
                            })
                            .expect("serialize create request"),
                        ))
                        .expect("request"),
                )
                .await
                .expect("create response");
            assert_eq!(create_response.status(), StatusCode::CREATED);

            let delete_response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("DELETE")
                        .uri("/logs")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("delete response");
            assert_eq!(delete_response.status(), StatusCode::OK);

            let delete_body = delete_response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes();
            let delete_json: serde_json::Value =
                serde_json::from_slice(&delete_body).expect("deserialize delete response");
            assert_eq!(delete_json, serde_json::json!({"result": "deleted"}));

            let get_response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/logs")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("get response");
            assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
        }
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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

    #[allow(clippy::too_many_lines)]
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                                fuzziness: None,
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
                                fuzziness: None,
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
                                    fuzziness: None,
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

    #[allow(clippy::too_many_lines)]
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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

    #[allow(clippy::too_many_lines)]
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
                        })
                        .expect("serialize create request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("create response");

        let missing_doc_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/logs/_doc/missing")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(missing_doc_response.status(), StatusCode::NOT_FOUND);

        let missing_delete_response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/missing")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(missing_delete_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn bulk_on_missing_index_returns_not_found() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/missing/_bulk")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"operations":[{"index":{"id":"doc-1","source":{"a":1}}}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("bulk response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");
        assert_eq!(value["error"], "index 'missing' not found");
    }

    #[tokio::test]
    async fn bulk_with_empty_operations_returns_empty_items() {
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
                            settings: IndexSettings::default(),
                            ..Default::default()
                        })
                        .expect("serialize"),
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
                    .body(Body::from(r#"{"operations":[]}"#))
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
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");
        assert_eq!(value["errors"], false);
        assert!(value["items"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bulk_on_deleted_index_returns_not_found() {
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
                            settings: IndexSettings::default(),
                            ..Default::default()
                        })
                        .expect("serialize"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("create response");

        app.clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/logs")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("delete response");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_bulk")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"operations":[{"index":{"id":"doc-1","source":{"a":1}}}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("bulk response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn bulk_delete_of_nonexistent_document_succeeds() {
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
                            settings: IndexSettings::default(),
                            ..Default::default()
                        })
                        .expect("serialize"),
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
                        r#"{"operations":[{"delete":{"id":"doc-nonexistent"}}]}"#,
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
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");
        assert_eq!(value["errors"], false);
        let items = value["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["delete"]["result"], "deleted");
    }

    #[tokio::test]
    async fn put_doc_on_missing_index_returns_not_found() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/missing/_doc")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":"doc-1","source":{"a":1}}"#))
                    .expect("request"),
            )
            .await
            .expect("index response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");
        assert_eq!(value["error"], "index 'missing' not found");
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
            serde_json::json!({"error": "document 'missing' not found"})
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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

    #[allow(clippy::too_many_lines)]
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
    async fn sort_on_object_field_returns_bad_request() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&CreateIndexRequest {
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                    .uri("/test/_doc")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"id":"doc-1","source":{"metadata":{"key":"value"}}}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("index doc response");

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

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/test/_search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "sort": {"field": "metadata", "order": "asc"}
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .contains("cannot be used for sorting")
        );
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::similar_names)]
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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

    #[allow(clippy::too_many_lines)]
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
                                namespace: None,
                                retention_secs: None,
                                merge_threshold_docs: None,
                            },
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
    async fn exceeding_max_fields_returns_bad_request() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let app = router(catalog);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&CreateIndexRequest {
                            settings: IndexSettings::default(),
                            ..Default::default()
                        })
                        .expect("serialize create request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("create response");

        // Build bulk request with 1001 distinct fields to exceed MAX_FIELDS_PER_INDEX (1000)
        let mut operations = Vec::with_capacity(1001);
        for i in 0..=1000 {
            operations.push(serde_json::json!({"index": {"id": format!("doc-{}", i), "source": {format!("field_{}", i): i}}}));
        }

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/test/_bulk")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({"operations": operations}))
                            .unwrap(),
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
        let value: serde_json::Value = serde_json::from_slice(&body).expect("deserialize body");
        // Bulk returns 200 with errors: true when individual operations fail
        assert!(value["errors"].as_bool().unwrap());
    }

    #[allow(clippy::too_many_lines)]
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            "_score": 1.0,
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

    #[allow(clippy::too_many_lines)]
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                                fuzziness: None,
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

    #[allow(clippy::too_many_lines)]
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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

    #[tokio::test]
    async fn merge_endpoint_returns_merge_response_and_increments_metric() {
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
                            settings: IndexSettings::default(),
                            ..Default::default()
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
                            source: serde_json::json!({"message": "hello"}),
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

        let merge_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logs/_merge")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("merge response");

        assert_eq!(merge_response.status(), StatusCode::OK);
        let merge_body = merge_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let merge_json: serde_json::Value =
            serde_json::from_slice(&merge_body).expect("deserialize merge response");
        assert_eq!(merge_json["result"], "merged");
        assert_eq!(merge_json["merged_documents"], 1);

        let metrics_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("metrics response");
        let metrics_body = metrics_response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let metrics_str = String::from_utf8(metrics_body.to_vec()).expect("metrics to string");
        assert!(metrics_str.contains("cloudsearch_merge_total"));
    }

    #[test]
    fn parse_term_query_with_fuzziness_auto() {
        use cloudsearch_common::Fuzziness;
        let json = serde_json::json!({
            "field": "name",
            "value": "admin",
            "fuzziness": "auto"
        });
        let result = parse_term_query(&json).expect("should parse");
        assert_eq!(result.fuzziness, Some(Fuzziness::Auto));
    }

    #[test]
    fn parse_term_query_with_fuzziness_auto_uppercase() {
        use cloudsearch_common::Fuzziness;
        let json = serde_json::json!({
            "field": "name",
            "value": "admin",
            "fuzziness": "AUTO"
        });
        let result = parse_term_query(&json).expect("should parse");
        assert_eq!(result.fuzziness, Some(Fuzziness::Auto));
    }

    #[test]
    fn parse_term_query_with_fuzziness_exact_integer() {
        use cloudsearch_common::Fuzziness;
        let json = serde_json::json!({
            "field": "name",
            "value": "admin",
            "fuzziness": 2
        });
        let result = parse_term_query(&json).expect("should parse");
        assert_eq!(result.fuzziness, Some(Fuzziness::Exact(2)));
    }

    #[test]
    fn parse_term_query_with_fuzziness_zero() {
        use cloudsearch_common::Fuzziness;
        let json = serde_json::json!({
            "field": "name",
            "value": "admin",
            "fuzziness": 0
        });
        let result = parse_term_query(&json).expect("should parse");
        assert_eq!(result.fuzziness, Some(Fuzziness::Exact(0)));
    }

    #[test]
    fn parse_term_query_with_fuzziness_missing() {
        let json = serde_json::json!({
            "field": "name",
            "value": "admin"
        });
        let result = parse_term_query(&json).expect("should parse");
        assert_eq!(result.fuzziness, None);
    }

    #[test]
    fn parse_term_query_with_fuzziness_wrong_type_rejected() {
        let json = serde_json::json!({
            "field": "name",
            "value": "admin",
            "fuzziness": true
        });
        let result = parse_term_query(&json);
        assert!(result.is_err(), "fuzziness: true should be rejected");
    }

    #[test]
    fn parse_term_query_with_fuzziness_unknown_string_rejected() {
        let json = serde_json::json!({
            "field": "name",
            "value": "admin",
            "fuzziness": "unknown"
        });
        let result = parse_term_query(&json);
        assert!(result.is_err(), "fuzziness: unknown should be rejected");
    }
}
