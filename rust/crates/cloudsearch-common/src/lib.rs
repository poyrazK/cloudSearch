use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, CloudSearchError>;

#[derive(Debug, Error)]
pub enum CloudSearchError {
    #[error("index '{0}' already exists")]
    IndexAlreadyExists(String),
    #[error("index '{0}' not found")]
    IndexNotFound(String),
    #[error("invalid index name '{0}'")]
    InvalidIndexName(String),
    #[error("invalid search request: {0}")]
    InvalidSearchRequest(String),
    #[error("mapping conflict: {0}")]
    MappingConflict(String),
    #[error("unknown field rejected: {0}")]
    UnknownFieldRejected(String),
    #[error("array fields are unsupported: {0}")]
    UnsupportedArrayField(String),
    #[error("mapping limit exceeded: {0}")]
    MappingLimitExceeded(String),
    #[error("invalid WAL record: {0}")]
    InvalidWalRecord(String),
    #[error("WAL checksum mismatch")]
    WalChecksumMismatch,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MappingMode {
    Strict,
    #[default]
    ControlledDynamic,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct IndexSettings {
    pub mapping_mode: MappingMode,
    pub primary_time_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexMetadata {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub settings: IndexSettings,
    pub mappings: BTreeMap<String, FieldMapping>,
}

impl IndexMetadata {
    pub fn new(name: impl Into<String>, settings: IndexSettings) -> Self {
        let now = Utc::now();

        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            created_at: now,
            updated_at: now,
            settings,
            mappings: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldMapping {
    pub field_type: FieldType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Keyword,
    Boolean,
    Integer,
    Long,
    Double,
    Timestamp,
    Object,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CreateIndexRequest {
    #[serde(default)]
    pub settings: IndexSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDocument {
    pub id: String,
    pub source: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDocumentRequest {
    pub id: String,
    pub source: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDocumentResponse {
    pub id: String,
    pub result: &'static str,
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetDocumentResponse {
    pub id: String,
    pub found: bool,
    pub source: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BulkRequest {
    pub operations: Vec<BulkOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BulkOperation {
    Index(BulkIndexOperation),
    Delete(BulkDeleteOperation),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BulkIndexOperation {
    pub id: String,
    pub source: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BulkDeleteOperation {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BulkResponse {
    pub errors: bool,
    pub items: Vec<BulkItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BulkItem {
    Index(BulkItemResult),
    Delete(BulkItemResult),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BulkItemResult {
    pub id: String,
    pub result: String,
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RefreshResponse {
    pub result: &'static str,
    pub refreshed_documents: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlushResponse {
    pub result: &'static str,
    pub flushed_documents: usize,
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergeResponse {
    pub result: &'static str,
    pub merged_documents: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SearchRequest {
    pub query: Option<SearchQuery>,
    pub from: Option<usize>,
    pub size: Option<usize>,
    pub sort: Option<SortSpec>,
    pub aggs: Option<BTreeMap<String, AggregationRequest>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SearchQuery {
    MatchAll,
    Term(TermQuery),
    Terms(TermsQuery),
    Range(RangeQuery),
    Bool(BoolQuery),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TermQuery {
    pub field: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TermsQuery {
    pub field: String,
    pub values: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RangeQuery {
    pub field: String,
    pub gte: Option<serde_json::Value>,
    pub gt: Option<serde_json::Value>,
    pub lte: Option<serde_json::Value>,
    pub lt: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BoolQuery {
    #[serde(default)]
    pub must: Vec<SearchQuery>,
    #[serde(default)]
    pub should: Vec<SearchQuery>,
    #[serde(default)]
    pub filter: Vec<SearchQuery>,
    #[serde(default)]
    pub must_not: Vec<SearchQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SortSpec {
    pub field: String,
    pub order: SortOrder,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchResponse {
    pub hits: HitsMetadata,
    pub aggregations: BTreeMap<String, AggregationResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HitsMetadata {
    pub total: usize,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchHit {
    pub id: String,
    pub source: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AggregationRequest {
    Terms(TermsAggregationRequest),
    Stats(StatsAggregationRequest),
    DateHistogram(DateHistogramAggregationRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TermsAggregationRequest {
    pub field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatsAggregationRequest {
    pub field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DateHistogramAggregationRequest {
    pub field: String,
    pub interval: DateHistogramInterval,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DateHistogramInterval {
    Minute,
    Hour,
    Day,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AggregationResult {
    Terms(TermsAggregationResult),
    Stats(StatsAggregationResult),
    DateHistogram(DateHistogramAggregationResult),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TermsAggregationResult {
    pub buckets: Vec<TermsBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TermsBucket {
    pub key: serde_json::Value,
    pub doc_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatsAggregationResult {
    pub count: usize,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub avg: Option<f64>,
    pub sum: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DateHistogramAggregationResult {
    pub buckets: Vec<DateHistogramBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DateHistogramBucket {
    pub key: String,
    pub doc_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthResponse {
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    pub bind_addr: String,
    pub data_dir: PathBuf,
    pub refresh_interval_secs: u64,
    pub flush_interval_secs: u64,
    pub merge_interval_secs: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:4000".to_string(),
            data_dir: PathBuf::from("./data"),
            refresh_interval_secs: 1,
            flush_interval_secs: 30,
            merge_interval_secs: 60,
        }
    }
}

impl AppConfig {
    pub fn normalize_intervals(&mut self) {
        if self.refresh_interval_secs == 0 {
            self.refresh_interval_secs = 1;
        }

        if self.flush_interval_secs == 0 {
            self.flush_interval_secs = 30;
        }

        if self.merge_interval_secs == 0 {
            self.merge_interval_secs = 60;
        }
    }
}
