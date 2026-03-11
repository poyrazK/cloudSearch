use cloudsearch_common::{CloudSearchError, CreateIndexRequest, IndexMetadata, Result};
use std::path::{Path, PathBuf};
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
}
