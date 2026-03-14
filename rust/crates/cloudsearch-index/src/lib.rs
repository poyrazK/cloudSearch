use chrono::{DateTime, Utc};
use cloudsearch_common::{
    CloudSearchError, CreateIndexRequest, HitsMetadata, IndexDocument, IndexMetadata, RangeQuery,
    Result, SearchHit, SearchQuery, SearchRequest, SearchResponse,
};
use cloudsearch_storage::{WalManager, WalRecord};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use tokio::fs;

#[derive(Debug, Clone)]
pub struct IndexCatalog {
    root_dir: PathBuf,
}

impl IndexCatalog {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        fs::create_dir_all(self.indexes_dir()).await?;
        Ok(())
    }

    pub async fn create_index(
        &self,
        name: &str,
        request: CreateIndexRequest,
    ) -> Result<IndexMetadata> {
        validate_index_name(name)?;

        let index_dir = self.index_dir(name);
        let metadata_path = self.metadata_path(name);

        if fs::try_exists(&index_dir).await? {
            return Err(CloudSearchError::IndexAlreadyExists(name.to_string()));
        }

        fs::create_dir_all(index_dir.join("wal")).await?;
        fs::create_dir_all(index_dir.join("segments")).await?;

        let metadata = IndexMetadata::new(name, request.settings);
        let json = serde_json::to_vec_pretty(&metadata)?;
        fs::write(metadata_path, json).await?;

        Ok(metadata)
    }

    pub async fn get_index(&self, name: &str) -> Result<IndexMetadata> {
        validate_index_name(name)?;

        let metadata_path = self.metadata_path(name);
        if !fs::try_exists(&metadata_path).await? {
            return Err(CloudSearchError::IndexNotFound(name.to_string()));
        }

        let bytes = fs::read(metadata_path).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn open_index(&self, name: &str) -> Result<IndexHandle> {
        let metadata = self.get_index(name).await?;
        let wal = WalManager::open(self.index_dir(name).join("wal")).await?;
        let entries = wal.replay().await?;

        let mut searchable_documents = BTreeMap::new();
        let mut last_sequence_number = 0;

        for entry in entries {
            last_sequence_number = entry.sequence_number;

            match entry.record {
                WalRecord::IndexDocument { document } => {
                    searchable_documents.insert(document.id.clone(), document);
                }
                WalRecord::DeleteDocument { document_id } => {
                    searchable_documents.remove(&document_id);
                }
                WalRecord::MappingUpdate { .. } => {}
            }
        }

        Ok(IndexHandle {
            metadata,
            wal,
            searchable_documents,
            pending_operations: BTreeMap::new(),
            last_sequence_number,
        })
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    fn indexes_dir(&self) -> PathBuf {
        self.root_dir.join("indexes")
    }

    fn index_dir(&self, name: &str) -> PathBuf {
        self.indexes_dir().join(name)
    }

    fn metadata_path(&self, name: &str) -> PathBuf {
        self.index_dir(name).join("metadata.json")
    }
}

#[derive(Debug)]
pub struct IndexHandle {
    metadata: IndexMetadata,
    wal: WalManager,
    searchable_documents: BTreeMap<String, IndexDocument>,
    pending_operations: BTreeMap<String, PendingOperation>,
    last_sequence_number: u64,
}

#[derive(Debug, Clone)]
enum PendingOperation {
    Upsert(IndexDocument),
    Delete,
}

impl IndexHandle {
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    pub fn documents(&self) -> &BTreeMap<String, IndexDocument> {
        &self.searchable_documents
    }

    pub fn get_document(&self, document_id: &str) -> Option<&IndexDocument> {
        match self.pending_operations.get(document_id) {
            Some(PendingOperation::Upsert(document)) => Some(document),
            Some(PendingOperation::Delete) => None,
            None => self.searchable_documents.get(document_id),
        }
    }

    pub fn search(&self, request: &SearchRequest) -> SearchResponse {
        let query = request.query.as_ref().unwrap_or(&SearchQuery::MatchAll);
        let hits = self
            .searchable_documents
            .values()
            .filter(|document| matches_query(document, query))
            .map(|document| SearchHit {
                id: document.id.clone(),
                source: document.source.clone(),
            })
            .collect::<Vec<_>>();

        SearchResponse {
            hits: HitsMetadata {
                total: hits.len(),
                hits,
            },
        }
    }

    pub async fn index_document(&mut self, document: IndexDocument) -> Result<u64> {
        let sequence_number = self.last_sequence_number + 1;
        self.wal
            .append(
                sequence_number,
                WalRecord::IndexDocument {
                    document: document.clone(),
                },
            )
            .await?;
        self.pending_operations
            .insert(document.id.clone(), PendingOperation::Upsert(document));
        self.last_sequence_number = sequence_number;
        Ok(sequence_number)
    }

    pub async fn delete_document(&mut self, document_id: &str) -> Result<u64> {
        let sequence_number = self.last_sequence_number + 1;
        self.wal
            .append(
                sequence_number,
                WalRecord::DeleteDocument {
                    document_id: document_id.to_string(),
                },
            )
            .await?;
        self.pending_operations
            .insert(document_id.to_string(), PendingOperation::Delete);
        self.last_sequence_number = sequence_number;
        Ok(sequence_number)
    }

    pub async fn refresh(&mut self) -> Result<usize> {
        let refreshed_documents = self.pending_operations.len();

        for (document_id, operation) in std::mem::take(&mut self.pending_operations) {
            match operation {
                PendingOperation::Upsert(document) => {
                    self.searchable_documents.insert(document_id, document);
                }
                PendingOperation::Delete => {
                    self.searchable_documents.remove(&document_id);
                }
            }
        }

        self.metadata.updated_at = Utc::now();

        Ok(refreshed_documents)
    }
}

fn matches_query(document: &IndexDocument, query: &SearchQuery) -> bool {
    match query {
        SearchQuery::MatchAll => true,
        SearchQuery::Term(term) => document
            .source
            .get(&term.field)
            .is_some_and(|value| value == &term.value),
        SearchQuery::Range(range) => matches_range_query(document, range),
        SearchQuery::Bool(bool_query) => bool_query
            .filter
            .iter()
            .all(|filter_query| matches_query(document, filter_query)),
    }
}

fn matches_range_query(document: &IndexDocument, range: &RangeQuery) -> bool {
    let Some(value) = document.source.get(&range.field) else {
        return false;
    };

    match comparable_value(value) {
        Some(ComparableValue::Number(number)) => matches_numeric_range(number, range),
        Some(ComparableValue::Timestamp(timestamp)) => matches_timestamp_range(timestamp, range),
        None => false,
    }
}

enum ComparableValue {
    Number(f64),
    Timestamp(DateTime<Utc>),
}

fn comparable_value(value: &serde_json::Value) -> Option<ComparableValue> {
    if let Some(number) = value.as_f64() {
        return Some(ComparableValue::Number(number));
    }

    value
        .as_str()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|timestamp| ComparableValue::Timestamp(timestamp.with_timezone(&Utc)))
}

fn matches_numeric_range(number: f64, range: &RangeQuery) -> bool {
    compare_numeric_bound(number, range.gte.as_ref(), |lhs, rhs| lhs >= rhs)
        && compare_numeric_bound(number, range.gt.as_ref(), |lhs, rhs| lhs > rhs)
        && compare_numeric_bound(number, range.lte.as_ref(), |lhs, rhs| lhs <= rhs)
        && compare_numeric_bound(number, range.lt.as_ref(), |lhs, rhs| lhs < rhs)
}

fn compare_numeric_bound(
    number: f64,
    bound: Option<&serde_json::Value>,
    predicate: impl Fn(f64, f64) -> bool,
) -> bool {
    match bound {
        Some(value) => value.as_f64().is_some_and(|bound| predicate(number, bound)),
        None => true,
    }
}

fn matches_timestamp_range(timestamp: DateTime<Utc>, range: &RangeQuery) -> bool {
    compare_timestamp_bound(timestamp, range.gte.as_ref(), |lhs, rhs| lhs >= rhs)
        && compare_timestamp_bound(timestamp, range.gt.as_ref(), |lhs, rhs| lhs > rhs)
        && compare_timestamp_bound(timestamp, range.lte.as_ref(), |lhs, rhs| lhs <= rhs)
        && compare_timestamp_bound(timestamp, range.lt.as_ref(), |lhs, rhs| lhs < rhs)
}

fn compare_timestamp_bound(
    timestamp: DateTime<Utc>,
    bound: Option<&serde_json::Value>,
    predicate: impl Fn(DateTime<Utc>, DateTime<Utc>) -> bool,
) -> bool {
    match bound {
        Some(value) => value
            .as_str()
            .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
            .map(|parsed| parsed.with_timezone(&Utc))
            .is_some_and(|bound| predicate(timestamp, bound)),
        None => true,
    }
}

fn validate_index_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 255
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');

    if valid {
        Ok(())
    } else {
        Err(CloudSearchError::InvalidIndexName(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudsearch_common::{
        BoolQuery, CreateIndexRequest, IndexSettings, MappingMode, RangeQuery, SearchQuery,
        SearchRequest, TermQuery,
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn creates_and_loads_index_metadata() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let metadata = catalog
            .create_index(
                "logs_v1",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::ControlledDynamic,
                        primary_time_field: Some("@timestamp".to_string()),
                    },
                },
            )
            .await
            .expect("create index");

        let loaded = catalog.get_index("logs_v1").await.expect("load index");

        assert_eq!(loaded.name, metadata.name);
        assert_eq!(
            loaded.settings.primary_time_field.as_deref(),
            Some("@timestamp")
        );
    }

    #[tokio::test]
    async fn rejects_duplicate_index_creation() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let error = catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect_err("duplicate should fail");

        assert!(matches!(error, CloudSearchError::IndexAlreadyExists(_)));
    }

    #[tokio::test]
    async fn replays_documents_from_wal() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "world"}),
            })
            .await
            .expect("index doc");
        handle.delete_document("doc-1").await.expect("delete doc");

        let recovered = catalog.open_index("logs").await.expect("recover index");

        assert_eq!(recovered.documents().len(), 1);
        assert!(recovered.documents().contains_key("doc-2"));
        assert!(!recovered.documents().contains_key("doc-1"));
    }

    #[tokio::test]
    async fn gets_document_by_id_after_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-42".to_string(),
                source: serde_json::json!({"message": "persist me"}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        let document = reopened.get_document("doc-42").expect("document exists");

        assert_eq!(document.id, "doc-42");
        assert_eq!(document.source["message"], "persist me");
        assert!(reopened.get_document("missing").is_none());
    }

    #[tokio::test]
    async fn writes_are_searchable_only_after_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing"}),
            })
            .await
            .expect("index doc");

        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 0);
        assert!(handle.get_document("doc-1").is_some());

        let refreshed = handle.refresh().await.expect("refresh");
        assert_eq!(refreshed, 1);
        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 1);
    }

    #[tokio::test]
    async fn delete_becomes_search_visible_after_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");
        handle.delete_document("doc-1").await.expect("delete doc");

        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 1);

        handle.refresh().await.expect("refresh");
        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 0);
    }

    #[tokio::test]
    async fn searches_match_all_and_term_queries_after_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "level": "info"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "search", "level": "info"}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");

        let match_all = reopened.search(&SearchRequest::default());
        assert_eq!(match_all.hits.total, 2);

        let term = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "service".to_string(),
                value: serde_json::json!("billing"),
            })),
        });
        assert_eq!(term.hits.total, 1);
        assert_eq!(term.hits.hits[0].id, "doc-1");

        let filtered = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![SearchQuery::Term(TermQuery {
                    field: "level".to_string(),
                    value: serde_json::json!("info"),
                })],
            })),
        });
        assert_eq!(filtered.hits.total, 2);
    }

    #[tokio::test]
    async fn latest_document_version_wins_after_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "first"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "second"}),
            })
            .await
            .expect("overwrite doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        let document = reopened.get_document("doc-1").expect("document exists");

        assert_eq!(document.source["message"], "second");
    }

    #[tokio::test]
    async fn search_handles_empty_indexes_missing_fields_and_multiple_filters() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let empty = catalog.open_index("logs").await.expect("open empty index");
        assert_eq!(empty.search(&SearchRequest::default()).hits.total, 0);

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "level": "info", "active": true}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "billing", "level": "error", "active": false}),
            })
            .await
            .expect("index doc");
        handle.delete_document("doc-2").await.expect("delete doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");

        let missing_field = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "missing".to_string(),
                value: serde_json::json!("nope"),
            })),
        });
        assert_eq!(missing_field.hits.total, 0);

        let multiple_filters = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![
                    SearchQuery::Term(TermQuery {
                        field: "service".to_string(),
                        value: serde_json::json!("billing"),
                    }),
                    SearchQuery::Term(TermQuery {
                        field: "active".to_string(),
                        value: serde_json::json!(true),
                    }),
                ],
            })),
        });
        assert_eq!(multiple_filters.hits.total, 1);
        assert_eq!(multiple_filters.hits.hits[0].id, "doc-1");
    }

    #[tokio::test]
    async fn repeated_reopen_preserves_overwrite_and_delete_sequence() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut first = catalog.open_index("logs").await.expect("open index");
        first
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "first"}),
            })
            .await
            .expect("index doc");

        let mut second = catalog.open_index("logs").await.expect("reopen index");
        second
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "second"}),
            })
            .await
            .expect("overwrite doc");
        second.delete_document("doc-1").await.expect("delete doc");
        second
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "survivor"}),
            })
            .await
            .expect("index doc");

        let third = catalog.open_index("logs").await.expect("reopen index");
        assert!(third.get_document("doc-1").is_none());
        assert_eq!(third.search(&SearchRequest::default()).hits.total, 1);
        assert_eq!(
            third.get_document("doc-2").unwrap().source["message"],
            "survivor"
        );
    }

    #[tokio::test]
    async fn deleting_missing_document_is_safe_across_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        let sequence = handle
            .delete_document("missing-doc")
            .await
            .expect("delete missing doc");
        assert_eq!(sequence, 1);

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(reopened.search(&SearchRequest::default()).hits.total, 0);
        assert!(reopened.get_document("missing-doc").is_none());
    }

    #[tokio::test]
    async fn boolean_and_numeric_term_queries_match_exact_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"active": true, "latency": 42}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");

        let bool_query = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "active".to_string(),
                value: serde_json::json!(true),
            })),
        });
        assert_eq!(bool_query.hits.total, 1);

        let numeric_query = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "latency".to_string(),
                value: serde_json::json!(42),
            })),
        });
        assert_eq!(numeric_query.hits.total, 1);

        let wrong_type = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "latency".to_string(),
                value: serde_json::json!("42"),
            })),
        });
        assert_eq!(wrong_type.hits.total, 0);
    }

    #[tokio::test]
    async fn range_queries_match_numeric_and_timestamp_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({
                    "latency": 42,
                    "timestamp": "2026-03-14T10:00:00Z"
                }),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({
                    "latency": 7,
                    "timestamp": "2026-03-14T12:00:00Z"
                }),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        let numeric = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "latency".to_string(),
                gte: Some(serde_json::json!(10)),
                gt: None,
                lte: Some(serde_json::json!(50)),
                lt: None,
            })),
        });
        assert_eq!(numeric.hits.total, 1);
        assert_eq!(numeric.hits.hits[0].id, "doc-1");

        let timestamp = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "timestamp".to_string(),
                gte: Some(serde_json::json!("2026-03-14T11:00:00Z")),
                gt: None,
                lte: None,
                lt: Some(serde_json::json!("2026-03-14T13:00:00Z")),
            })),
        });
        assert_eq!(timestamp.hits.total, 1);
        assert_eq!(timestamp.hits.hits[0].id, "doc-2");

        let missing = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "missing".to_string(),
                gte: Some(serde_json::json!(1)),
                gt: None,
                lte: None,
                lt: None,
            })),
        });
        assert_eq!(missing.hits.total, 0);
    }

    #[tokio::test]
    async fn deleted_documents_do_not_appear_in_match_all_after_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "remove me"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "keep me"}),
            })
            .await
            .expect("index doc");
        handle.delete_document("doc-1").await.expect("delete doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        let all = reopened.search(&SearchRequest::default());

        assert_eq!(all.hits.total, 1);
        assert_eq!(all.hits.hits[0].id, "doc-2");
    }

    #[tokio::test]
    async fn bool_filter_with_no_clauses_matches_all_documents() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "a"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "b"}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        let result = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery { filter: vec![] })),
        });

        assert_eq!(result.hits.total, 2);
    }
}
