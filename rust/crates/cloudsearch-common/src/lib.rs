use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
    #[error("invalid WAL record: {0}")]
    InvalidWalRecord(String),
    #[error("WAL checksum mismatch")]
    WalChecksumMismatch,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MappingMode {
    Strict,
    ControlledDynamic,
}

impl Default for MappingMode {
    fn default() -> Self {
        Self::ControlledDynamic
    }
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
        }
    }
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
pub struct SearchRequest {
    pub query: Option<SearchQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SearchQuery {
    MatchAll,
    Term(TermQuery),
    Bool(BoolQuery),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TermQuery {
    pub field: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BoolQuery {
    #[serde(default)]
    pub filter: Vec<SearchQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchResponse {
    pub hits: HitsMetadata,
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
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:4000".to_string(),
            data_dir: PathBuf::from("./data"),
        }
    }
}
