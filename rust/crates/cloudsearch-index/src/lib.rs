use cloudsearch_common::{
    CloudSearchError, CreateIndexRequest, IndexDocument, IndexMetadata, Result,
};
use cloudsearch_storage::{WalManager, WalRecord};
use std::{collections::BTreeMap, path::{Path, PathBuf}};
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

        let mut documents = BTreeMap::new();
        let mut last_sequence_number = 0;

        for entry in entries {
            last_sequence_number = entry.sequence_number;
            match entry.record {
                WalRecord::IndexDocument { document } => {
                    documents.insert(document.id.clone(), document);
                }
                WalRecord::DeleteDocument { document_id } => {
                    documents.remove(&document_id);
                }
                WalRecord::MappingUpdate { .. } => {}
            }
        }

        Ok(IndexHandle {
            metadata,
            wal,
            documents,
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
    documents: BTreeMap<String, IndexDocument>,
    last_sequence_number: u64,
}

impl IndexHandle {
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    pub fn documents(&self) -> &BTreeMap<String, IndexDocument> {
        &self.documents
    }

    pub fn get_document(&self, document_id: &str) -> Option<&IndexDocument> {
        self.documents.get(document_id)
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
        self.documents.insert(document.id.clone(), document);
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
        self.documents.remove(document_id);
        self.last_sequence_number = sequence_number;
        Ok(sequence_number)
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
    use cloudsearch_common::{CreateIndexRequest, IndexSettings, MappingMode};
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
        assert_eq!(loaded.settings.primary_time_field.as_deref(), Some("@timestamp"));
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
}
