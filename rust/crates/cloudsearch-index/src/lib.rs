use chrono::{DateTime, Timelike, Utc};
use cloudsearch_common::{
    AggregationRequest, AggregationResult, BoolQuery, BulkItem, BulkItemResult, BulkOperation,
    BulkRequest, BulkResponse, CloudSearchError, CreateIndexRequest,
    DateHistogramAggregationResult, DateHistogramBucket, DateHistogramInterval, FieldMapping,
    FieldType, FlushResponse, Fuzziness, HitsMetadata, IndexDocument, IndexMetadata, MappingMode,
    MatchQuery, MergeResponse, PhraseQuery, PrefixQuery, RangeQuery, Result, SearchHit,
    SearchQuery, SearchRequest, SearchResponse, SortOrder, SortSpec, StatsAggregationResult,
    TermQuery, TermsAggregationResult, TermsBucket, TermsQuery, WildcardQuery,
};
use cloudsearch_storage::{
    IndexManifest, SegmentMeta, SegmentSnapshot, SnapshotMetadata, WalManager, WalRecord,
    delete_snapshot as storage_delete_snapshot, legacy_snapshot_exists, list_snapshots,
    read_doc_values, read_index_manifest, read_named_snapshot, read_segment_file,
    read_segment_snapshot, read_snapshot_metadata, segment_file_path,
    write_doc_values as storage_write_doc_values, write_index_manifest, write_named_snapshot,
    write_positions as storage_write_positions, write_segment_snapshot,
};
mod doc_values;
mod doc_values_reader;
use crate::doc_values::DocValuesWriter;
use crate::doc_values_reader::DocValuesReader;
use regex::Regex;
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs,
    sync::{Mutex, RwLock},
};

const MAX_FIELDS_PER_INDEX: usize = 1000;
const MERGE_TRIGGER_DOCUMENT_COUNT: usize = 8;
const MAX_SEARCH_SIZE: usize = 10_000;
const MAX_SEARCH_OFFSET: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergePlan {
    segments: Vec<SegmentMeta>,
}

impl MergePlan {
    #[must_use]
    pub fn new(segments: Vec<SegmentMeta>) -> Self {
        Self { segments }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct IndexCatalog {
    root_dir: PathBuf,
    lifecycle_lock: Arc<RwLock<()>>,
}

#[derive(Debug, Clone)]
pub struct IndexRegistry {
    catalog: Arc<IndexCatalog>,
    handles: Arc<Mutex<HashMap<String, Arc<Mutex<IndexHandle>>>>>,
    lifecycle_lock: Arc<RwLock<()>>,
}

impl IndexCatalog {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            lifecycle_lock: Arc::new(RwLock::new(())),
        }
    }

    /// Initializes the catalog by creating the indexes directory.
    ///
    /// # Errors
    /// Returns an error if directory creation fails.
    pub async fn initialize(&self) -> Result<()> {
        fs::create_dir_all(self.indexes_dir()).await?;
        Ok(())
    }

    /// Creates a new index with the given name and settings.
    ///
    /// # Errors
    /// Returns an error if validation fails or index creation fails.
    pub async fn create_index(
        &self,
        name: &str,
        request: CreateIndexRequest,
    ) -> Result<IndexMetadata> {
        let _guard = self.lifecycle_lock.write().await;
        validate_index_name(name)?;

        if let Some(ref ns) = request.settings.namespace {
            validate_namespace(ns)?;
        }

        let index_dir = self.index_dir(name);
        let metadata_path = self.metadata_path(name);

        if fs::try_exists(&index_dir).await? {
            return Err(CloudSearchError::IndexAlreadyExists(name.to_string()));
        }

        fs::create_dir_all(index_dir.join("wal")).await?;
        fs::create_dir_all(index_dir.join("segments")).await?;

        // Initialize empty manifest for new indexes
        let segments_dir = index_dir.join("segments");
        write_index_manifest(&segments_dir, &IndexManifest::new()).await?;

        let mut metadata = IndexMetadata::new(name, request.settings);
        if let Some(mappings) = request.mappings {
            for (field, mapping) in mappings {
                metadata.mappings.insert(field, mapping);
            }
        }
        let json = serde_json::to_vec_pretty(&metadata)?;
        fs::write(metadata_path, json).await?;

        Ok(metadata)
    }

    /// Gets metadata for an existing index.
    ///
    /// # Errors
    /// Returns an error if validation fails or file operations fail.
    pub async fn get_index(&self, name: &str) -> Result<IndexMetadata> {
        validate_index_name(name)?;

        let metadata_path = self.metadata_path(name);
        if !fs::try_exists(&metadata_path).await? {
            return Err(CloudSearchError::IndexNotFound(name.to_string()));
        }

        let bytes = fs::read(metadata_path).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Deletes an index and its storage.
    ///
    /// # Errors
    /// Returns an error if validation fails or file operations fail.
    pub async fn delete_index(&self, name: &str) -> Result<()> {
        let _guard = self.lifecycle_lock.write().await;
        validate_index_name(name)?;

        let index_dir = self.index_dir(name);
        if !fs::try_exists(&index_dir).await? {
            return Err(CloudSearchError::IndexNotFound(name.to_string()));
        }

        fs::remove_dir_all(index_dir).await?;
        Ok(())
    }

    /// Updates index settings.
    ///
    /// # Errors
    /// Returns an error if validation fails or file operations fail.
    pub async fn update_index_settings(
        &self,
        name: &str,
        retention_secs: Option<u64>,
    ) -> Result<IndexMetadata> {
        let _guard = self.lifecycle_lock.write().await;
        validate_index_name(name)?;

        let metadata_path = self.metadata_path(name);
        if !fs::try_exists(&metadata_path).await? {
            return Err(CloudSearchError::IndexNotFound(name.to_string()));
        }

        let bytes = fs::read(&metadata_path).await?;
        let mut metadata: IndexMetadata = serde_json::from_slice(&bytes)?;
        metadata.settings.retention_secs = retention_secs;
        metadata.updated_at = chrono::Utc::now();

        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        fs::write(&metadata_path, metadata_json.as_bytes()).await?;

        Ok(metadata)
    }

    /// Load all positions sidecars from a manifest's segments.
    async fn load_all_positions_readers(
        segments_dir: &Path,
        manifest: &IndexManifest,
    ) -> Vec<cloudsearch_storage::inverted_index::PositionsReader> {
        let mut readers = Vec::new();
        for seg in &manifest.segments {
            if let Some(reader) = Self::load_single_positions_reader(segments_dir, seg).await {
                readers.push(reader);
            }
        }
        readers
    }

    /// Load positions sidecar for a single segment, if it exists.
    async fn load_single_positions_reader(
        segments_dir: &Path,
        seg: &SegmentMeta,
    ) -> Option<cloudsearch_storage::inverted_index::PositionsReader> {
        let path = segments_dir.join(format!("positions_{:020}.bin", seg.segment_number));
        let reader = cloudsearch_storage::inverted_index::PositionsReader::read(&path)
            .await
            .ok()?;
        if reader.term_count() > 0 {
            tracing::info!(terms = reader.term_count(), "loaded positions sidecar");
            Some(reader)
        } else {
            None
        }
    }

    /// Opens an existing index for reading and writing.
    ///
    /// # Errors
    /// Returns an error if the index does not exist or if file operations fail.
    pub async fn open_index(&self, name: &str) -> Result<IndexHandle> {
        let _guard = self.lifecycle_lock.write().await;
        let metadata = self.get_index(name).await?;
        let metadata_path = self.metadata_path(name);
        let segments_dir = self.index_dir(name).join("segments");
        let wal = WalManager::open(self.index_dir(name).join("wal")).await?;

        // Try new manifest-based recovery first, fall back to legacy current.json migration
        let manifest = match read_index_manifest(&segments_dir).await? {
            Some(m) => m,
            None => {
                // Migration: check if old current.json exists
                if legacy_snapshot_exists(&segments_dir).await? {
                    tracing::info!(index = %name, "migrating legacy current.json to manifest");
                    let snapshot = read_segment_snapshot(&segments_dir).await?;
                    let segment_number = 1;
                    let seg_path = segment_file_path(&segments_dir, segment_number);
                    if let Some(ref _snap) = snapshot {
                        // Rename current.json → seg_000001.json
                        let current_path = segments_dir.join("current.json");
                        fs::rename(&current_path, &seg_path).await?;
                    }
                    let meta = SegmentMeta {
                        segment_number,
                        last_sequence_number: snapshot
                            .as_ref()
                            .map_or(0, |s| s.last_sequence_number),
                        document_count: snapshot.as_ref().map_or(0, |s| s.documents.len() as u64),
                        checksum: 0, // checksum not computed for legacy
                    };
                    IndexManifest::new().with_segment(meta)
                } else {
                    IndexManifest::new()
                }
            }
        };

        // Load all segments into searchable_documents (last-write-wins)
        let mut searchable_documents: BTreeMap<String, IndexDocument> = BTreeMap::new();
        let mut max_seq = 0u64;
        for seg_meta in &manifest.segments {
            let seg_path = segment_file_path(&segments_dir, seg_meta.segment_number);
            if let Ok(Some(snap)) = read_segment_file(&seg_path).await {
                for doc in snap.documents {
                    searchable_documents.insert(doc.id.clone(), doc);
                }
                max_seq = max_seq.max(seg_meta.last_sequence_number);
            }
        }

        let mut last_sequence_number = max_seq;

        tracing::info!(index = %name, "recovering WAL");
        let entries = wal.replay_from(last_sequence_number).await?;
        let recovered_count = entries.len();

        let mut document_timestamps: BTreeMap<String, DateTime<Utc>> = BTreeMap::new();

        for entry in entries {
            last_sequence_number = entry.sequence_number;

            match entry.record {
                WalRecord::IndexDocument { document } => {
                    searchable_documents.insert(document.id.clone(), document.clone());
                    if let Some(ts) = extract_document_timestamp_from_doc(&metadata, &document)
                        && let Some(retention) = metadata.settings.retention_secs
                    {
                        let expiry = ts + chrono::Duration::seconds(retention.cast_signed());
                        document_timestamps.insert(document.id.clone(), expiry);
                    }
                }
                WalRecord::DeleteDocument { document_id } => {
                    searchable_documents.remove(&document_id);
                    document_timestamps.remove(&document_id);
                }
                WalRecord::MappingUpdate { .. } => {}
            }
        }
        tracing::info!(index = %name, recovered_docs = recovered_count, "WAL recovery complete");

        // Load doc values sidecar if available
        let doc_values_reader = match read_doc_values(&segments_dir).await {
            Ok(fields) if !fields.is_empty() => {
                tracing::info!(index = %name, num_fields = fields.len(), "loaded doc values");
                Some(DocValuesReader::new(fields))
            }
            _ => None,
        };

        // Load positions sidecar from all segments for multi-segment highlight support
        let positions_readers = Self::load_all_positions_readers(&segments_dir, &manifest).await;
        if !positions_readers.is_empty() {
            tracing::info!(
                count = positions_readers.len(),
                "loaded positions sidecars for all segments"
            );
        }

        Ok(IndexHandle {
            metadata,
            metadata_path,
            wal,
            segments_dir,
            manifest,
            searchable_documents,
            pending_operations: BTreeMap::new(),
            last_sequence_number,
            document_timestamps,
            doc_values_reader,
            per_doc_inverted_index: BTreeMap::new(),
            positions_readers,
        })
    }

    #[must_use]
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

impl IndexRegistry {
    #[must_use]
    pub fn new(catalog: Arc<IndexCatalog>) -> Self {
        Self {
            catalog,
            handles: Arc::new(Mutex::new(HashMap::new())),
            lifecycle_lock: Arc::new(RwLock::new(())),
        }
    }

    /// Creates a new index and opens it for writing.
    ///
    /// # Errors
    /// Returns an error if index creation fails or the index already exists.
    pub async fn create_index(
        &self,
        name: &str,
        request: CreateIndexRequest,
    ) -> Result<IndexMetadata> {
        let _guard = self.lifecycle_lock.write().await;
        let metadata = self.catalog.create_index(name, request).await?;

        let handle = Arc::new(Mutex::new(self.catalog.open_index(name).await?));
        self.handles.lock().await.insert(name.to_string(), handle);

        Ok(metadata)
    }

    /// Gets metadata for an existing index.
    ///
    /// # Errors
    /// Returns an error if the index does not exist or file operations fail.
    pub async fn get_index(&self, name: &str) -> Result<IndexMetadata> {
        self.catalog.get_index(name).await
    }

    /// Deletes an index and removes it from the registry.
    ///
    /// # Errors
    /// Returns an error if the index does not exist or deletion fails.
    pub async fn delete_index(&self, name: &str) -> Result<()> {
        let _guard = self.lifecycle_lock.write().await;
        self.catalog.delete_index(name).await?;
        self.handles.lock().await.remove(name);
        Ok(())
    }

    /// Updates index settings.
    ///
    /// # Errors
    /// Returns an error if the index does not exist or settings update fails.
    pub async fn update_index_settings(
        &self,
        name: &str,
        request: cloudsearch_common::UpdateSettingsRequest,
    ) -> Result<IndexMetadata> {
        self.catalog
            .update_index_settings(name, request.retention_secs)
            .await
    }

    /// Gets or opens an index handle for direct operations.
    ///
    /// # Errors
    /// Returns an error if the index does not exist or cannot be opened.
    pub async fn index_handle(&self, name: &str) -> Result<Arc<Mutex<IndexHandle>>> {
        {
            let handles = self.handles.lock().await;
            if let Some(handle) = handles.get(name) {
                return Ok(handle.clone());
            }
        }

        let _guard = self.lifecycle_lock.read().await;

        {
            let handles = self.handles.lock().await;
            if let Some(handle) = handles.get(name) {
                return Ok(handle.clone());
            }
        }

        let opened = Arc::new(Mutex::new(self.catalog.open_index(name).await?));

        let mut handles = self.handles.lock().await;
        if let Some(handle) = handles.get(name) {
            return Ok(handle.clone());
        }

        handles.insert(name.to_string(), opened.clone());
        Ok(opened)
    }

    pub async fn cached_handle_count(&self) -> usize {
        self.handles.lock().await.len()
    }

    pub async fn cached_handles(&self) -> Vec<Arc<Mutex<IndexHandle>>> {
        self.handles.lock().await.values().cloned().collect()
    }

    pub async fn cached_handles_with_names(&self) -> Vec<(String, Arc<Mutex<IndexHandle>>)> {
        let handles = self.handles.lock().await;
        handles
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Returns per-index metrics for all cached indexes.
    pub async fn index_metrics(&self) -> Vec<(String, IndexMetrics)> {
        let handles = self.handles.lock().await;
        let mut result = Vec::new();
        for (name, handle) in handles.iter() {
            let metrics = handle.lock().await.metrics();
            result.push((name.clone(), metrics));
        }
        result
    }
}

/// Per-index resource metrics.
#[derive(Debug, Clone)]
pub struct IndexMetrics {
    pub document_count: usize,
    pub pending_operations: usize,
    pub last_sequence_number: u64,
}

#[derive(Debug)]
pub struct IndexHandle {
    metadata: IndexMetadata,
    metadata_path: PathBuf,
    wal: WalManager,
    segments_dir: PathBuf,
    /// Tracks active immutable segments — replaces single mutable snapshot.
    manifest: IndexManifest,
    searchable_documents: BTreeMap<String, IndexDocument>,
    pending_operations: BTreeMap<String, PendingOperation>,
    last_sequence_number: u64,
    /// Stores `document_id` -> expiration `DateTime` for retention policy.
    document_timestamps: BTreeMap<String, DateTime<Utc>>,
    /// Pre-extracted columnar doc values for aggregations, if available.
    doc_values_reader: Option<DocValuesReader>,
    /// Per-document inverted indices (term -> byte offsets) for pending documents.
    /// Cleared after each flush when documents are persisted.
    per_doc_inverted_index: BTreeMap<String, BTreeMap<String, Vec<u32>>>,
    /// Positions readers loaded from all segments, for multi-segment highlight extraction.
    positions_readers: Vec<cloudsearch_storage::inverted_index::PositionsReader>,
}

#[derive(Debug, Clone)]
enum PendingOperation {
    Upsert(IndexDocument),
    Delete,
}

/// Extracts timestamp from a document using the index's primary time field.
/// Supports RFC3339 strings and Unix integer timestamps.
fn extract_document_timestamp_from_doc(
    metadata: &IndexMetadata,
    document: &IndexDocument,
) -> Option<DateTime<Utc>> {
    let field_name = metadata.settings.primary_time_field.as_deref()?;
    let value = document.source.get(field_name)?;

    // Try RFC3339 parsing first
    if let Some(raw) = value.as_str()
        && let Ok(parsed) = DateTime::parse_from_rfc3339(raw)
    {
        return Some(parsed.with_timezone(&Utc));
    }

    // Fallback: Unix integer timestamp (seconds)
    if let Some(secs) = value.as_i64() {
        return DateTime::from_timestamp(secs, 0);
    }

    None
}

impl IndexHandle {
    /// Returns the retention duration in seconds, if configured.
    #[must_use]
    pub fn retention_secs(&self) -> Option<u64> {
        self.metadata.settings.retention_secs
    }

    /// Returns the primary time field name, if configured.
    #[must_use]
    pub fn primary_time_field(&self) -> Option<&str> {
        self.metadata.settings.primary_time_field.as_deref()
    }

    /// Returns true if this index has a retention policy configured.
    #[must_use]
    pub fn has_retention_policy(&self) -> bool {
        self.retention_secs().is_some() && self.primary_time_field().is_some()
    }

    /// Returns per-index resource metrics.
    #[must_use]
    pub fn metrics(&self) -> IndexMetrics {
        IndexMetrics {
            document_count: self.searchable_documents.len(),
            pending_operations: self.pending_operations.len(),
            last_sequence_number: self.last_sequence_number,
        }
    }

    /// Extracts the timestamp from a document's primary time field.
    /// Supports RFC3339 strings and Unix integer timestamps.
    fn extract_document_timestamp(&self, document: &IndexDocument) -> Option<DateTime<Utc>> {
        let field_name = self.primary_time_field()?;
        let value = document.source.get(field_name)?;

        // Try RFC3339 parsing first
        if let Some(raw) = value.as_str()
            && let Ok(parsed) = DateTime::parse_from_rfc3339(raw)
        {
            return Some(parsed.with_timezone(&Utc));
        }

        // Fallback: Unix integer timestamp (seconds)
        if let Some(secs) = value.as_i64() {
            return DateTime::from_timestamp(secs, 0);
        }

        None
    }

    /// Returns true if the document has expired based on its timestamp.
    fn is_expired(&self, document_id: &str, now: DateTime<Utc>) -> bool {
        if !self.has_retention_policy() {
            return false;
        }

        if let Some(expiry) = self.document_timestamps.get(document_id) {
            return expiry <= &now;
        }

        // No timestamp means it cannot be expired
        false
    }

    pub(crate) fn plan_merge(&self) -> Option<MergePlan> {
        // Policy: merge if 2+ small segments exist (worth compacting together),
        // OR if 1 segment exceeds the threshold (large enough to warrant compaction).
        let threshold = self
            .metadata
            .settings
            .merge_threshold_docs
            .unwrap_or(MERGE_TRIGGER_DOCUMENT_COUNT) as u64;

        let below: Vec<_> = self
            .manifest
            .segments
            .iter()
            .filter(|s| s.document_count > 0 && s.document_count < threshold)
            .cloned()
            .collect();

        if below.len() >= 2 {
            // Multiple small segments — compact them together
            return Some(MergePlan::new(below));
        }

        // Check if single large segment warrants immediate merge
        if let Some(large) = self.manifest.segments.last()
            && large.document_count >= threshold
        {
            return Some(MergePlan::new(vec![large.clone()]));
        }

        None
    }

    /// Applies a merge plan to consolidate segments.
    ///
    /// # Errors
    /// Returns an error if segment file operations fail.
    pub async fn apply_merge_plan(&mut self, plan: &MergePlan) -> Result<()> {
        if plan.is_empty() {
            return Ok(());
        }

        // Collect segment IDs to merge
        let segment_ids: Vec<u64> = plan.segments.iter().map(|s| s.segment_number).collect();

        // Read all segment files being merged
        let mut merged: BTreeMap<String, IndexDocument> = BTreeMap::new();
        for seg_meta in &plan.segments {
            let seg_path = segment_file_path(&self.segments_dir, seg_meta.segment_number);
            if let Ok(Some(snap)) = read_segment_file(&seg_path).await {
                for doc in snap.documents {
                    merged.insert(doc.id.clone(), doc);
                }
            }
        }

        // Apply pending operations
        for (id, op) in &self.pending_operations {
            match op {
                PendingOperation::Upsert(doc) => {
                    merged.insert(id.clone(), doc.clone());
                }
                PendingOperation::Delete => {
                    merged.remove(id);
                }
            }
        }

        let merged_documents = merged.len();

        // Determine next segment number
        let new_segment_number = self.manifest.next_segment_number();

        // Write new compacted segment
        let new_snapshot = SegmentSnapshot {
            last_sequence_number: self.last_sequence_number,
            documents: merged.clone().into_values().collect(),
        };

        // Write to segment file path (NOT current.json)
        let new_seg_path = segment_file_path(&self.segments_dir, new_segment_number);
        let temp_path = self
            .segments_dir
            .join(format!("seg_{new_segment_number:06}.tmp"));
        let bytes = serde_json::to_vec_pretty(&new_snapshot)?;
        fs::write(&temp_path, bytes).await?;
        fs::rename(&temp_path, &new_seg_path).await?;

        // Compute checksum
        let data_bytes = serde_json::to_vec(&new_snapshot)?;
        let checksum = crc32c::crc32c(&data_bytes);

        // Build new manifest: remove merged segments, add new one
        let mut new_manifest = IndexManifest {
            version: self.manifest.version + 1,
            last_updated: chrono::Utc::now(),
            segments: self
                .manifest
                .segments
                .iter()
                .filter(|s| !segment_ids.contains(&s.segment_number))
                .cloned()
                .collect(),
        };
        new_manifest.segments.push(SegmentMeta {
            segment_number: new_segment_number,
            last_sequence_number: new_snapshot.last_sequence_number,
            document_count: merged_documents as u64,
            checksum,
        });

        // Atomic manifest swap
        write_index_manifest(&self.segments_dir, &new_manifest).await?;

        // Build and write positions sidecar for the merged segment by extracting
        // positions from each merged document (rebuilds from primary data).
        // This must happen before we move `merged` into searchable_documents.
        let merged_docs: Vec<IndexDocument> = merged.values().cloned().collect();
        let mut merged_positions = cloudsearch_storage::inverted_index::InvertedIndex::new();
        for doc in &merged_docs {
            let doc_positions = Self::extract_positions(doc);
            for (term, positions) in doc_positions {
                let doc_id_hash = hash_doc_id(&doc.id);
                for pos in positions {
                    merged_positions.insert(term.clone(), doc_id_hash, vec![pos]);
                }
            }
        }
        if merged_positions.term_count() > 0 {
            storage_write_positions(&self.segments_dir, new_segment_number, &merged_positions)
                .await
                .ok();
        }

        // Update in-memory state
        self.manifest = new_manifest;
        self.searchable_documents = merged;

        // Reload positions reader for the new merged segment only, since
        // the old segment sidecars were invalidated by the merge.
        self.positions_readers.clear();
        let positions_path = self
            .segments_dir
            .join(format!("positions_{new_segment_number:020}.bin"));
        if let Ok(reader) =
            cloudsearch_storage::inverted_index::PositionsReader::read(&positions_path).await
        {
            tracing::info!(
                terms = reader.term_count(),
                "loaded positions for merged segment"
            );
            if reader.term_count() > 0 {
                self.positions_readers.push(reader);
            }
        } else {
            tracing::warn!(path = %positions_path.display(), "no positions file found for merged segment");
        }

        tracing::info!(index = %self.metadata.name, docs = merged_documents, "apply_merge_plan complete");
        Ok(())
    }

    #[must_use]
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    #[must_use]
    pub fn documents(&self) -> &BTreeMap<String, IndexDocument> {
        &self.searchable_documents
    }

    #[must_use]
    pub fn get_document(&self, document_id: &str) -> Option<&IndexDocument> {
        match self.pending_operations.get(document_id) {
            Some(PendingOperation::Upsert(document)) => Some(document),
            Some(PendingOperation::Delete) => None,
            None => self.searchable_documents.get(document_id),
        }
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn search(&self, request: &SearchRequest) -> SearchResponse {
        let query = request.query.as_ref().unwrap_or(&SearchQuery::MatchAll);
        let now = Utc::now();

        // BM25 parameters
        let k1 = 1.2f32;
        let b = 0.75f32;
        let n_docs = self.searchable_documents.len().max(1);

        // Build IDF map: for each query term, compute IDF = log((N-df+0.5)/(df+0.5))
        // We sum DF across all segment readers to get total document frequency.
        // Deduplicate terms via BTreeSet to avoid redundant IDF lookups.
        let mut idf_map: std::collections::BTreeMap<String, f32> =
            std::collections::BTreeMap::new();
        let query_terms: std::collections::BTreeSet<String> =
            extract_query_terms(query).into_iter().collect();
        for term in &query_terms {
            let mut total_df = 0usize;
            for reader in &self.positions_readers {
                if let Some(pl) = reader.get(term) {
                    total_df += pl.docs.len();
                }
            }
            let idf = bm25_idf(total_df, n_docs);
            idf_map.insert(term.clone(), idf);
        }
        // For prefix queries, also compute IDF for the full stored field value
        // since prefix matching checks if stored.starts_with(prefix) first.
        if let SearchQuery::Prefix(pq) = query {
            let full_value = pq.value.to_lowercase();
            let mut total_df = 0usize;
            for reader in &self.positions_readers {
                if let Some(pl) = reader.get(&full_value) {
                    total_df += pl.docs.len();
                }
            }
            idf_map
                .entry(full_value)
                .or_insert(bm25_idf(total_df, n_docs));
        }
        let mut all_fields: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for doc in self.searchable_documents.values() {
            if let Some(obj) = doc.source.as_object() {
                for k in obj.keys() {
                    all_fields.insert(k.clone());
                }
            }
        }
        let mut avg_field_lens: std::collections::BTreeMap<String, f32> =
            std::collections::BTreeMap::new();
        for field in &all_fields {
            avg_field_lens.insert(
                field.clone(),
                compute_avg_field_length(&self.searchable_documents, field).max(1.0),
            );
        }
        let bm25_ctx = Bm25Context::new(idf_map, avg_field_lens, k1, b);

        let mut scored: Vec<(f32, &IndexDocument)> = self
            .searchable_documents
            .iter()
            .filter(|(_, doc)| !self.is_expired(&doc.id, now))
            .filter_map(|(_, doc)| {
                let doc_id_hash = hash_doc_id(&doc.id);
                score_query(doc, query, doc_id_hash, &self.positions_readers, &bm25_ctx)
                    .map(|s| (s, doc))
            })
            .collect();

        let total = scored.len();

        let matching_documents: Vec<IndexDocument> =
            scored.iter().map(|(_, d)| (*d).clone()).collect();
        let aggregations = compute_aggregations(
            &matching_documents,
            request.aggs.as_ref(),
            self.doc_values_reader.as_ref(),
        );

        if let Some(sort) = &request.sort {
            scored.sort_by(|(_, l), (_, r)| {
                let lh = SearchHit {
                    id: l.id.clone(),
                    source: l.source.clone(),
                    score: None,
                    highlight: None,
                    sort_values: None,
                };
                let rh = SearchHit {
                    id: r.id.clone(),
                    source: r.source.clone(),
                    score: None,
                    highlight: None,
                    sort_values: None,
                };
                compare_hits(&lh, &rh, sort)
            });
        } else {
            scored.sort_by(|(s1, d1), (s2, d2)| {
                s2.partial_cmp(s1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| d1.id.cmp(&d2.id))
            });
        }

        let from = if let Some(cursor) = &request.search_after {
            // For search_after, find position where doc > cursor
            scored
                .iter()
                .position(|(score, doc)| {
                    let doc_sort_values = compute_sort_values(doc, request.sort.as_ref(), *score);
                    compare_sort_values_list(&doc_sort_values, cursor, request.sort.as_ref())
                        == std::cmp::Ordering::Greater
                })
                .unwrap_or(scored.len())
        } else {
            request.from.unwrap_or(0).min(MAX_SEARCH_OFFSET)
        };
        let size = request.size.unwrap_or(total).min(MAX_SEARCH_SIZE);

        let hits = scored
            .into_iter()
            .skip(from)
            .take(size)
            .map(|(score, doc)| {
                let doc_id_hash = hash_doc_id(&doc.id);
                let highlight = match &self.positions_readers {
                    r if !r.is_empty() => extract_highlight(doc, doc_id_hash, r, query),
                    _ => None,
                };
                let sort_values = compute_sort_values(doc, request.sort.as_ref(), score);
                SearchHit {
                    id: doc.id.clone(),
                    source: doc.source.clone(),
                    score: Some(score),
                    highlight,
                    sort_values: Some(sort_values),
                }
            })
            .collect::<Vec<_>>();

        SearchResponse {
            hits: HitsMetadata { total, hits },
            aggregations,
        }
    }

    /// Evicts all expired documents by soft-deleting them via WAL.
    /// Returns the number of documents evicted.
    ///
    /// # Errors
    /// Returns an error if WAL operations fail.
    pub async fn evict_expired_documents(&mut self) -> Result<usize> {
        if !self.has_retention_policy() {
            return Ok(0);
        }

        let now = Utc::now();
        let expired_ids: Vec<String> = self
            .document_timestamps
            .iter()
            .filter(|(_, expiry)| *expiry <= &now)
            .map(|(id, _)| id.clone())
            .collect();

        let mut evicted = 0;
        for id in expired_ids {
            let sequence_number = self.last_sequence_number + 1;
            self.wal
                .append(
                    sequence_number,
                    WalRecord::DeleteDocument {
                        document_id: id.clone(),
                    },
                )
                .await?;
            self.pending_operations
                .insert(id.clone(), PendingOperation::Delete);
            self.document_timestamps.remove(&id);
            self.last_sequence_number = sequence_number;
            evicted += 1;
        }

        if evicted > 0 {
            tracing::info!(index = %self.metadata.name, evicted, "retention eviction complete");
        }

        Ok(evicted)
    }

    /// Validates a search request.
    ///
    /// # Errors
    /// Returns an error if the query is invalid or fields are not aggregatable.
    pub fn validate_search_request(&self, request: &SearchRequest) -> Result<()> {
        if let Some(query) = &request.query {
            self.validate_query(query)?;
            // search_after + fuzzy query is invalid because fuzzy matching can change
            // which documents match, affecting sort order and making cursors unreliable.
            if request.search_after.is_some() && self.query_has_fuzzy_term(query) {
                return Err(CloudSearchError::InvalidSearchRequest(
                    "search_after is not supported with fuzzy queries because match behavior affects sort order"
                        .to_string(),
                ));
            }
        }

        if let Some(sort) = &request.sort
            && let Some(mapping) = self.metadata.mappings.get(&sort.field)
            && matches!(mapping.field_type, FieldType::Object)
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{}' cannot be used for sorting",
                sort.field
            )));
        }

        if let Some(size) = request.size
            && size > MAX_SEARCH_SIZE
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "size ({size}) exceeds maximum allowed value ({MAX_SEARCH_SIZE})"
            )));
        }

        if let Some(from) = request.from
            && from > MAX_SEARCH_OFFSET
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "from ({from}) exceeds maximum allowed value ({MAX_SEARCH_OFFSET})"
            )));
        }

        if request.search_after.is_some() && request.from.is_some() {
            return Err(CloudSearchError::InvalidSearchRequest(
                "search_after and from cannot be used together".to_string(),
            ));
        }

        if request.search_after.is_some() && request.sort.is_none() {
            return Err(CloudSearchError::InvalidSearchRequest(
                "search_after requires sort field to be specified".to_string(),
            ));
        }

        if let Some(aggs) = &request.aggs {
            for (name, agg) in aggs {
                match agg {
                    AggregationRequest::Terms(terms) => {
                        self.ensure_scalar_field(&terms.field, name)?;
                    }
                    AggregationRequest::Stats(stats) => {
                        self.ensure_numeric_field(&stats.field, name)?;
                    }
                    AggregationRequest::DateHistogram(histogram) => {
                        self.ensure_timestamp_field(&histogram.field, name)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Indexes a document and returns its sequence number.
    ///
    /// # Errors
    /// Returns an error if mapping validation fails or WAL operations fail.
    pub async fn index_document(&mut self, document: IndexDocument) -> Result<u64> {
        self.validate_and_update_mappings(&document.source).await?;

        // Extract and store timestamp for retention policy
        if let Some(ts) = self.extract_document_timestamp(&document) {
            let expiry =
                ts + chrono::Duration::seconds(self.retention_secs().unwrap_or(0).cast_signed());
            self.document_timestamps.insert(document.id.clone(), expiry);
        }

        let sequence_number = self.last_sequence_number + 1;
        self.wal
            .append(
                sequence_number,
                WalRecord::IndexDocument {
                    document: document.clone(),
                },
            )
            .await?;
        self.pending_operations.insert(
            document.id.clone(),
            PendingOperation::Upsert(document.clone()),
        );
        self.last_sequence_number = sequence_number;

        // Build per-document inverted index (term -> byte offsets) for this document.
        let doc_index = Self::extract_positions(&document);
        if !doc_index.is_empty() {
            self.per_doc_inverted_index
                .insert(document.id.clone(), doc_index);
        }

        Ok(sequence_number)
    }

    /// Extract term byte-offsets from a document's text fields for highlight support.
    fn extract_positions(document: &IndexDocument) -> BTreeMap<String, Vec<u32>> {
        let mut doc_index = BTreeMap::new();
        if let Some(obj) = document.source.as_object() {
            for (_, field_value) in obj {
                if let Some(text) = field_value.as_str() {
                    let tokens = tokenize(text);
                    let lower_text = text.to_ascii_lowercase();
                    let mut seen_offsets: BTreeMap<String, Vec<u32>> = BTreeMap::new();
                    for token in &tokens {
                        let mut search_from = 0usize;
                        while let Some(pos) = lower_text[search_from..].find(token) {
                            let Ok(byte_offset) = u32::try_from(search_from + pos) else {
                                tracing::warn!(
                                    offset = search_from + pos,
                                    "byte offset exceeds u32::MAX, skipping position for term '{}' in doc '{}'",
                                    token,
                                    document.id
                                );
                                search_from += pos + 1;
                                continue;
                            };
                            seen_offsets
                                .entry(token.clone())
                                .or_default()
                                .push(byte_offset);
                            search_from += pos + token.len();
                        }
                    }
                    for (term, positions) in seen_offsets {
                        doc_index.insert(term, positions);
                    }
                }
            }
        }
        doc_index
    }

    /// Soft-deletes a document by writing a delete record to the WAL.
    ///
    /// # Errors
    /// Returns an error if WAL operations fail.
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

    /// Applies multiple index or delete operations in batch.
    ///
    /// # Errors
    /// Returns an error if any individual operation fails.
    pub async fn bulk_apply(&mut self, request: BulkRequest) -> Result<BulkResponse> {
        let mut items = Vec::with_capacity(request.operations.len());
        let mut has_errors = false;

        for operation in request.operations {
            match operation {
                BulkOperation::Index(index) => {
                    match self
                        .index_document(IndexDocument {
                            id: index.id.clone(),
                            source: index.source,
                        })
                        .await
                    {
                        Ok(sequence_number) => {
                            items.push(BulkItem::Index(BulkItemResult {
                                id: index.id,
                                result: "created".to_string(),
                                sequence_number,
                            }));
                        }
                        Err(err) => {
                            has_errors = true;
                            items.push(BulkItem::Index(BulkItemResult {
                                id: index.id,
                                result: format!("error: {err}"),
                                sequence_number: 0,
                            }));
                        }
                    }
                }
                BulkOperation::Delete(delete) => match self.delete_document(&delete.id).await {
                    Ok(sequence_number) => {
                        items.push(BulkItem::Delete(BulkItemResult {
                            id: delete.id,
                            result: "deleted".to_string(),
                            sequence_number,
                        }));
                    }
                    Err(err) => {
                        has_errors = true;
                        items.push(BulkItem::Delete(BulkItemResult {
                            id: delete.id,
                            result: format!("error: {err}"),
                            sequence_number: 0,
                        }));
                    }
                },
            }
        }

        Ok(BulkResponse {
            errors: has_errors,
            items,
        })
    }

    /// Makes recently indexed documents searchable.
    ///
    /// # Errors
    /// Returns an error if file operations fail.
    #[allow(clippy::unused_async)]
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

        if refreshed_documents > 0 {
            tracing::debug!(index = %self.metadata.name, docs = refreshed_documents, "refresh complete");
        }
        Ok(refreshed_documents)
    }

    /// Persists all in-memory data to disk and rolls over the WAL.
    ///
    /// # Errors
    /// Returns an error if file or WAL operations fail.
    pub async fn flush(&mut self) -> Result<FlushResponse> {
        // Rollover WAL first so the snapshot captures a consistent post-rollover state.
        // This ensures WAL replay on restart starts from the new generation.
        self.wal.rollover().await?;

        let snapshot = SegmentSnapshot {
            last_sequence_number: self.last_sequence_number,
            documents: self.searchable_documents.values().cloned().collect(),
        };

        // Determine next segment number
        let segment_number = self.manifest.next_segment_number();

        // Write new immutable segment file
        let seg_path = segment_file_path(&self.segments_dir, segment_number);
        let temp_path = self
            .segments_dir
            .join(format!("seg_{segment_number:06}.tmp"));
        let bytes = serde_json::to_vec_pretty(&snapshot)?;
        fs::write(&temp_path, bytes).await?;
        fs::rename(temp_path, &seg_path).await?;
        // Sync directory for durability
        let dir_file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&self.segments_dir)
            .await?;
        dir_file.sync_all().await?;

        // Compute checksum of segment data
        let data_bytes = serde_json::to_vec(&snapshot)?;
        let checksum = crc32c::crc32c(&data_bytes);

        // Build updated manifest
        let new_segment_meta = SegmentMeta {
            segment_number,
            last_sequence_number: snapshot.last_sequence_number,
            document_count: snapshot.documents.len() as u64,
            checksum,
        };

        let mut new_manifest = IndexManifest {
            version: self.manifest.version + 1,
            last_updated: chrono::Utc::now(),
            segments: self.manifest.segments.clone(),
        };
        new_manifest.segments.push(new_segment_meta);

        // Atomic manifest swap
        write_index_manifest(&self.segments_dir, &new_manifest).await?;

        // Update in-memory manifest
        self.manifest = new_manifest;

        tracing::info!(index = %self.metadata.name, seq = self.last_sequence_number, seg = segment_number, "flushed segment to disk");

        // Build and write doc values sidecar for aggregations
        let doc_values =
            DocValuesWriter::build_from_documents(&snapshot.documents, &self.metadata.mappings);
        storage_write_doc_values(&self.segments_dir, &doc_values).await?;
        tracing::info!(index = %self.metadata.name, num_fields = doc_values.len(), "wrote doc values sidecar");

        // Build and write positions sidecar for highlight support
        let mut inverted_index = cloudsearch_storage::inverted_index::InvertedIndex::new();
        for doc in &snapshot.documents {
            if let Some(doc_index) = self.per_doc_inverted_index.get(&doc.id) {
                for (term, positions) in doc_index {
                    let doc_id_hash = hash_doc_id(&doc.id);
                    for pos in positions {
                        inverted_index.insert(term.clone(), doc_id_hash, vec![*pos]);
                    }
                }
            }
        }

        if inverted_index.term_count() > 0 {
            storage_write_positions(&self.segments_dir, segment_number, &inverted_index).await?;
            tracing::info!(index = %self.metadata.name, terms = inverted_index.term_count(), "wrote positions sidecar");
            // Update positions_readers to include the newly written segment's positions
            let positions_path = self
                .segments_dir
                .join(format!("positions_{segment_number:020}.bin"));
            match cloudsearch_storage::inverted_index::PositionsReader::read(&positions_path).await
            {
                Ok(reader) if reader.term_count() > 0 => {
                    self.positions_readers.push(reader);
                    tracing::info!(
                        count = self.positions_readers.len(),
                        "added new positions reader after flush"
                    );
                }
                Err(e) => {
                    tracing::warn!(path = %positions_path.as_path().display(), error = %e, "failed to read positions file after flush");
                }
                _ => {}
            }
        }

        // Clear per-doc indices now that they're persisted
        self.per_doc_inverted_index.clear();

        self.wal.trim_through(snapshot.last_sequence_number).await?;

        if let Some(plan) = self.plan_merge() {
            self.apply_merge_plan(&plan).await?;
        }

        Ok(FlushResponse {
            result: "flushed",
            flushed_documents: snapshot.documents.len(),
            sequence_number: snapshot.last_sequence_number,
        })
    }

    /// Merges small segments into larger ones to reduce segment count.
    ///
    /// # Errors
    /// Returns an error if merge operations fail.
    pub async fn merge(&mut self) -> Result<MergeResponse> {
        let mut merged = self.searchable_documents.clone();

        for (id, op) in &self.pending_operations {
            match op {
                PendingOperation::Upsert(doc) => {
                    merged.insert(id.clone(), doc.clone());
                }
                PendingOperation::Delete => {
                    merged.remove(id);
                }
            }
        }

        let merged_documents = merged.len();

        // Rebuild positions sidecar for the merged segment by extracting
        // positions from each merged document.
        let merged_docs: Vec<IndexDocument> = merged.values().cloned().collect();
        let mut merged_positions = cloudsearch_storage::inverted_index::InvertedIndex::new();
        for doc in &merged_docs {
            let doc_positions = Self::extract_positions(doc);
            for (term, positions) in doc_positions {
                let doc_id_hash = hash_doc_id(&doc.id);
                for pos in positions {
                    merged_positions.insert(term.clone(), doc_id_hash, vec![pos]);
                }
            }
        }
        let new_segment_number = self.manifest.next_segment_number();
        if merged_positions.term_count() > 0 {
            storage_write_positions(&self.segments_dir, new_segment_number, &merged_positions)
                .await
                .ok();
        }

        let snapshot = SegmentSnapshot {
            last_sequence_number: self.last_sequence_number,
            documents: merged_docs,
        };

        write_segment_snapshot(&self.segments_dir, &snapshot).await?;

        // Update searchable_documents so subsequent searches see the merged state.
        // Also update positions_readers to point to the new merged segment.
        self.searchable_documents = merged;
        let positions_path = self
            .segments_dir
            .join(format!("positions_{new_segment_number:020}.bin"));
        self.positions_readers.clear();
        if let Ok(reader) =
            cloudsearch_storage::inverted_index::PositionsReader::read(&positions_path).await
            && reader.term_count() > 0
        {
            self.positions_readers.push(reader);
        }

        tracing::info!(index = %self.metadata.name, docs = merged_documents, seq = self.last_sequence_number, "merge complete");

        Ok(MergeResponse {
            result: "merged",
            merged_documents,
        })
    }

    /// Creates a named snapshot of the current index state.
    ///
    /// # Errors
    /// Returns an error if snapshot creation fails.
    pub async fn create_snapshot(
        &self,
        name: &str,
    ) -> Result<cloudsearch_common::CreateSnapshotResponse> {
        // Apply pending operations to get the true current state
        let mut snapshot_docs: BTreeMap<String, IndexDocument> = self.searchable_documents.clone();
        for (id, op) in &self.pending_operations {
            match op {
                PendingOperation::Upsert(doc) => {
                    snapshot_docs.insert(id.clone(), doc.clone());
                }
                PendingOperation::Delete => {
                    snapshot_docs.remove(id);
                }
            }
        }

        let snapshot = SegmentSnapshot {
            last_sequence_number: self.last_sequence_number,
            documents: snapshot_docs.into_values().collect(),
        };

        let data_bytes = serde_json::to_vec(&snapshot)?;
        let checksum = crc32c::crc32c(&data_bytes);

        let metadata = SnapshotMetadata {
            name: name.to_string(),
            created_at: chrono::Utc::now(),
            last_sequence_number: self.last_sequence_number,
            document_count: snapshot.documents.len(),
            checksum,
        };

        write_named_snapshot(&self.segments_dir, name, &snapshot, &metadata).await?;
        tracing::info!(index = %self.metadata.name, snapshot = %name, docs = snapshot.documents.len(), "snapshot created");

        Ok(cloudsearch_common::CreateSnapshotResponse {
            name: metadata.name,
            created_at: metadata.created_at,
            last_sequence_number: metadata.last_sequence_number,
            document_count: metadata.document_count,
            checksum: metadata.checksum,
        })
    }

    fn map_to_common_snapshot_meta(
        s: cloudsearch_storage::SnapshotMetadata,
    ) -> cloudsearch_common::SnapshotMetadata {
        cloudsearch_common::SnapshotMetadata {
            name: s.name,
            created_at: s.created_at,
            last_sequence_number: s.last_sequence_number,
            document_count: s.document_count,
            checksum: s.checksum,
        }
    }

    /// List all named snapshots for this index.
    ///
    /// # Errors
    /// Returns an error if snapshot listing fails.
    pub async fn list_snapshots(&self) -> Result<Vec<cloudsearch_common::SnapshotMetadata>> {
        let snapshots = list_snapshots(&self.segments_dir).await?;
        Ok(snapshots
            .into_iter()
            .map(Self::map_to_common_snapshot_meta)
            .collect())
    }

    /// Get metadata for a specific named snapshot.
    ///
    /// # Errors
    /// Returns an error if snapshot metadata read fails.
    pub async fn get_snapshot(
        &self,
        name: &str,
    ) -> Result<Option<cloudsearch_common::SnapshotMetadata>> {
        let meta = read_snapshot_metadata(&self.segments_dir, name).await?;
        Ok(meta.map(Self::map_to_common_snapshot_meta))
    }

    /// Delete a named snapshot.
    ///
    /// # Errors
    /// Returns an error if snapshot deletion fails.
    pub async fn delete_snapshot(&self, name: &str) -> Result<()> {
        storage_delete_snapshot(&self.segments_dir, name).await?;
        tracing::info!(index = %self.metadata.name, snapshot = %name, "snapshot deleted");
        Ok(())
    }

    /// Restore the index from a named snapshot.
    ///
    /// # Errors
    /// Returns an error if snapshot read or restoration fails.
    pub async fn restore_snapshot(
        &mut self,
        name: &str,
    ) -> Result<cloudsearch_common::RestoreResponse> {
        let snapshot = read_named_snapshot(&self.segments_dir, name)
            .await?
            .ok_or_else(|| CloudSearchError::SnapshotNotFound(name.to_string()))?;

        let metadata = read_snapshot_metadata(&self.segments_dir, name)
            .await?
            .ok_or_else(|| CloudSearchError::SnapshotNotFound(name.to_string()))?;

        // Validate checksum
        let data_bytes = serde_json::to_vec(&snapshot)?;
        let computed_checksum = crc32c::crc32c(&data_bytes);
        if computed_checksum != metadata.checksum {
            return Err(CloudSearchError::WalChecksumMismatch);
        }

        // Restore searchable documents and clear pending operations
        self.searchable_documents = snapshot
            .documents
            .iter()
            .map(|doc| (doc.id.clone(), doc.clone()))
            .collect();
        self.pending_operations.clear();
        self.last_sequence_number = metadata.last_sequence_number;

        // Rebuild document_timestamps from restored documents
        self.document_timestamps.clear();
        if let Some(retention) = self.retention_secs() {
            for doc in &snapshot.documents {
                if let Some(ts) = self.extract_document_timestamp(doc) {
                    let expiry = ts + chrono::Duration::seconds(retention.cast_signed());
                    self.document_timestamps.insert(doc.id.clone(), expiry);
                }
            }
        }

        // Write as the new current segment
        write_segment_snapshot(&self.segments_dir, &snapshot).await?;
        self.wal.rollover().await?;
        self.wal.trim_through(snapshot.last_sequence_number).await?;

        tracing::info!(
            index = %self.metadata.name,
            snapshot = %name,
            docs = snapshot.documents.len(),
            seq = snapshot.last_sequence_number,
            "snapshot restored"
        );

        Ok(cloudsearch_common::RestoreResponse {
            result: "restored".to_string(),
            restored_documents: snapshot.documents.len(),
            sequence_number: snapshot.last_sequence_number,
        })
    }

    async fn validate_and_update_mappings(&mut self, source: &serde_json::Value) -> Result<()> {
        let object = source.as_object().ok_or_else(|| {
            CloudSearchError::MappingConflict("document source must be a JSON object".to_string())
        })?;

        let mut new_mappings = Vec::new();

        for (field, value) in object {
            let Some(inferred_type) = infer_field_type(field, value)? else {
                continue;
            };

            match self.metadata.mappings.get(field) {
                Some(existing) if existing.field_type != inferred_type => {
                    return Err(CloudSearchError::MappingConflict(format!(
                        "field '{}' expected {:?} but received {:?}",
                        field, existing.field_type, inferred_type
                    )));
                }
                Some(_) => {}
                None => match self.metadata.settings.mapping_mode {
                    MappingMode::Strict => {
                        return Err(CloudSearchError::UnknownFieldRejected(field.clone()));
                    }
                    MappingMode::ControlledDynamic => {
                        new_mappings.push((
                            field.clone(),
                            FieldMapping {
                                field_type: inferred_type,
                            },
                        ));
                    }
                },
            }
        }

        if self.metadata.mappings.len() + new_mappings.len() > MAX_FIELDS_PER_INDEX {
            return Err(CloudSearchError::MappingLimitExceeded(format!(
                "index '{}' exceeded maximum field count of {}",
                self.metadata.name, MAX_FIELDS_PER_INDEX
            )));
        }

        if !new_mappings.is_empty() {
            for (field, mapping) in new_mappings {
                self.metadata.mappings.insert(field, mapping);
            }
            self.metadata.updated_at = Utc::now();
            self.persist_metadata().await?;
        }

        Ok(())
    }

    async fn persist_metadata(&self) -> Result<()> {
        let json = serde_json::to_vec_pretty(&self.metadata)?;
        fs::write(&self.metadata_path, json).await?;
        Ok(())
    }

    fn validate_query(&self, query: &SearchQuery) -> Result<()> {
        match query {
            SearchQuery::MatchAll => Ok(()),
            SearchQuery::Term(term) => self.ensure_scalar_field(&term.field, &term.field),
            SearchQuery::Terms(terms) => self.ensure_scalar_field(&terms.field, &terms.field),
            SearchQuery::Range(range) => self.ensure_range_field(&range.field),
            SearchQuery::Bool(boolean) => boolean
                .must
                .iter()
                .chain(boolean.should.iter())
                .chain(boolean.filter.iter())
                .chain(boolean.must_not.iter())
                .try_for_each(|query| self.validate_query(query)),
            SearchQuery::Prefix(prefix) => self.ensure_scalar_field(&prefix.field, &prefix.field),
            SearchQuery::Wildcard(wc) => self.ensure_scalar_field(&wc.field, &wc.field),
            SearchQuery::Match(mq) => self.ensure_text_field(&mq.field, &mq.field),
            SearchQuery::Phrase(phrase) => self.ensure_text_field(&phrase.field, &phrase.field),
        }
    }

    #[allow(clippy::self_only_used_in_recursion)]
    fn query_has_fuzzy_term(&self, query: &SearchQuery) -> bool {
        match query {
            SearchQuery::Term(term) => term.fuzziness.is_some(),
            SearchQuery::Bool(boolean) => boolean
                .must
                .iter()
                .chain(boolean.should.iter())
                .chain(boolean.filter.iter())
                .chain(boolean.must_not.iter())
                .any(|q| self.query_has_fuzzy_term(q)),
            _ => false,
        }
    }

    fn ensure_scalar_field(&self, field: &str, context: &str) -> Result<()> {
        if let Some(mapping) = self.metadata.mappings.get(field)
            && matches!(mapping.field_type, FieldType::Object)
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{field}' cannot be used as a scalar in '{context}'"
            )));
        }

        Ok(())
    }

    fn ensure_text_field(&self, field: &str, context: &str) -> Result<()> {
        if let Some(mapping) = self.metadata.mappings.get(field) {
            match mapping.field_type {
                FieldType::Keyword => Ok(()),
                _ => Err(CloudSearchError::InvalidSearchRequest(format!(
                    "field '{field}' does not support match queries in '{context}'"
                ))),
            }
        } else {
            Ok(())
        }
    }

    fn ensure_numeric_field(&self, field: &str, context: &str) -> Result<()> {
        if let Some(mapping) = self.metadata.mappings.get(field)
            && !matches!(
                mapping.field_type,
                FieldType::Integer | FieldType::Long | FieldType::Double
            )
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{field}' is not numeric for '{context}'"
            )));
        }

        Ok(())
    }

    fn ensure_timestamp_field(&self, field: &str, context: &str) -> Result<()> {
        if let Some(mapping) = self.metadata.mappings.get(field)
            && mapping.field_type != FieldType::Timestamp
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{field}' is not a timestamp for '{context}'"
            )));
        }

        Ok(())
    }

    fn ensure_range_field(&self, field: &str) -> Result<()> {
        if let Some(mapping) = self.metadata.mappings.get(field)
            && !matches!(
                mapping.field_type,
                FieldType::Integer | FieldType::Long | FieldType::Double | FieldType::Timestamp
            )
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{field}' does not support range queries"
            )));
        }

        Ok(())
    }
}

fn infer_field_type(field: &str, value: &serde_json::Value) -> Result<Option<FieldType>> {
    Ok(match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(_) => Some(FieldType::Boolean),
        serde_json::Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                if i32::try_from(integer).is_ok() {
                    Some(FieldType::Integer)
                } else {
                    Some(FieldType::Long)
                }
            } else if let Some(integer) = number.as_u64() {
                if i32::try_from(integer).is_ok() {
                    Some(FieldType::Integer)
                } else if i64::try_from(integer).is_ok() {
                    Some(FieldType::Long)
                } else {
                    Some(FieldType::Double)
                }
            } else {
                Some(FieldType::Double)
            }
        }
        serde_json::Value::String(raw) => {
            if DateTime::parse_from_rfc3339(raw).is_ok() {
                Some(FieldType::Timestamp)
            } else {
                Some(FieldType::Keyword)
            }
        }
        serde_json::Value::Array(_) => {
            return Err(CloudSearchError::UnsupportedArrayField(field.to_string()));
        }
        serde_json::Value::Object(_) => Some(FieldType::Object),
    })
}

/// BM25 context: precomputed values for BM25 scoring, passed through the scoring chain.
#[derive(Clone, Debug)]
struct Bm25Context {
    /// Precomputed IDF per term: term → IDF score
    idf_map: std::collections::BTreeMap<String, f32>,
    /// Precomputed average field length per field: field name → avg length
    avg_field_lens: std::collections::BTreeMap<String, f32>,
    /// BM25 term frequency saturation parameter (default 1.2)
    k1: f32,
    /// BM25 field length normalization parameter (default 0.75)
    b: f32,
}

impl Bm25Context {
    fn new(
        idf_map: std::collections::BTreeMap<String, f32>,
        avg_field_lens: std::collections::BTreeMap<String, f32>,
        k1: f32,
        b: f32,
    ) -> Self {
        Self {
            idf_map,
            avg_field_lens,
            k1,
            b,
        }
    }

    /// Get the average field length for a specific field, defaulting to 1.0.
    fn get_avg_field_len(&self, field: &str) -> f32 {
        self.avg_field_lens.get(field).copied().unwrap_or(1.0)
    }

    /// Score a single term using the BM25 formula for a specific field.
    #[allow(clippy::cast_precision_loss)]
    fn bm25_term_score(&self, tf: u32, doc_len: usize, idf: f32, field: &str) -> f32 {
        let tf = tf as f32;
        let doc_len = doc_len as f32;
        let avg_len = self.get_avg_field_len(field);
        let numerator = tf * (self.k1 + 1.0);
        let denominator = tf + self.k1 * (1.0 - self.b + self.b * doc_len / avg_len.max(1.0));
        idf * numerator / denominator
    }
}

/// BM25 IDF formula: log((N - df + 0.5) / (df + 0.5)), clamped to [0, ∞).
/// - df == 0 (term not in corpus): returns 1.0 (rare-term default)
/// - otherwise: computes the log formula; if result is negative (term in >50% of docs),
///   returns 0.0 since high-frequency terms should not boost scoring.
#[allow(clippy::cast_precision_loss)]
fn bm25_idf(df: usize, n_docs: usize) -> f32 {
    if df == 0 {
        return 1.0;
    }
    let n = n_docs as f32;
    let df = df as f32;
    ((n - df + 0.5) / (df + 0.5)).ln().max(0.0)
}

/// Collect all unique query terms from a `SearchQuery` (for match/phrase/term queries).
fn extract_query_terms(query: &SearchQuery) -> Vec<String> {
    match query {
        SearchQuery::Match(mq) => tokenize(&mq.value),
        SearchQuery::Phrase(pq) => tokenize(&pq.value),
        SearchQuery::Term(tq) if tq.fuzziness.is_none() => {
            // For exact term queries, use the term value as-is (already lowercase normalization)
            if let serde_json::Value::String(s) = &tq.value {
                vec![s.to_lowercase()]
            } else {
                vec![]
            }
        }
        SearchQuery::Bool(bq) => {
            let mut terms = Vec::new();
            for q in bq
                .must
                .iter()
                .chain(bq.should.iter())
                .chain(bq.filter.iter())
            {
                terms.extend(extract_query_terms(q));
            }
            terms
        }
        SearchQuery::Terms(tq) => tq
            .values
            .iter()
            .filter_map(|v| v.as_str().map(str::to_lowercase))
            .collect(),
        // Prefix and Wildcard are handled in scoring (need positions_readers for term enumeration)
        _ => vec![],
    }
}

/// Compute average field length across all documents in the index.
#[allow(clippy::cast_precision_loss)]
fn compute_avg_field_length(
    documents: &std::collections::BTreeMap<String, IndexDocument>,
    field: &str,
) -> f32 {
    let mut total_len = 0usize;
    let mut count = 0usize;
    for doc in documents.values() {
        if let Some(val) = doc.source.get(field)
            && let Some(s) = val.as_str()
        {
            total_len += tokenize(s).len();
            count += 1;
        }
    }
    if count == 0 {
        1.0
    } else {
        total_len as f32 / count as f32
    }
}

fn score_query(
    document: &IndexDocument,
    query: &SearchQuery,
    doc_id: u64,
    positions_readers: &[cloudsearch_storage::inverted_index::PositionsReader],
    bm25_ctx: &Bm25Context,
) -> Option<f32> {
    match query {
        SearchQuery::MatchAll => Some(1.0),
        SearchQuery::Term(term) => {
            if term.fuzziness.is_some() {
                match fuzzy_term_match(document, term) {
                    Some(true) => Some(1.0),
                    _ => None,
                }
            } else {
                score_term_query(document, term, doc_id, positions_readers, bm25_ctx)
            }
        }
        SearchQuery::Terms(terms) => {
            score_terms_query(document, terms, doc_id, positions_readers, bm25_ctx)
        }
        SearchQuery::Range(range) => matches_range_query(document, range).then_some(1.0),
        SearchQuery::Bool(bool_query) => {
            score_bool_query(document, bool_query, doc_id, positions_readers, bm25_ctx)
        }
        SearchQuery::Prefix(prefix) => {
            if matches_prefix_query(document, prefix) {
                score_prefix_query_bm25(document, prefix, doc_id, positions_readers, bm25_ctx)
            } else {
                None
            }
        }
        SearchQuery::Wildcard(wc) => {
            if matches_wildcard_query(document, wc) {
                score_wildcard_query_bm25(document, wc, doc_id, positions_readers, bm25_ctx)
            } else {
                None
            }
        }
        SearchQuery::Match(mq) => {
            score_match_query(document, mq, doc_id, positions_readers, bm25_ctx)
        }
        SearchQuery::Phrase(phrase) => {
            score_phrase_query(document, phrase, doc_id, positions_readers, bm25_ctx)
        }
    }
}

#[allow(clippy::cast_precision_loss)]
fn score_match_query(
    document: &IndexDocument,
    query: &MatchQuery,
    doc_id: u64,
    positions_readers: &[cloudsearch_storage::inverted_index::PositionsReader],
    bm25_ctx: &Bm25Context,
) -> Option<f32> {
    let field_str = document.source.get(&query.field)?.as_str()?;
    let field_tokens = tokenize(field_str);
    let query_tokens = tokenize(&query.value);
    if query_tokens.is_empty() {
        return None;
    }
    let doc_len = field_tokens.len();

    // For each query token, look up DF from positions readers and TF from the posting
    // for this specific document, then compute BM25 and sum
    let mut total_score = 0.0f32;
    let mut matched = 0;

    for token in &query_tokens {
        // Find TF for this document across all segment readers
        let mut tf = 0u32;
        for reader in positions_readers {
            if let Some(pl) = reader.get(token) {
                // Binary search for this doc_id
                if let Ok(idx) = pl.docs.binary_search_by(|p| p.doc_id.cmp(&doc_id)) {
                    tf += pl.docs[idx].term_freq;
                }
            }
        }

        if tf == 0 {
            // Token not in inverted index for this doc — check source field directly
            // (doc may not have been flushed yet, only in WAL)
            if field_tokens.contains(token) {
                // Count occurrences in field_tokens
                tf =
                    u32::try_from(field_tokens.iter().filter(|t| *t == token).count()).unwrap_or(0);
            } else {
                continue;
            }
        }

        // Use precomputed IDF if available; missing terms use default of 1.0
        let idf = bm25_ctx.idf_map.get(token).copied().unwrap_or(1.0);
        let term_score = bm25_ctx.bm25_term_score(tf, doc_len, idf, &query.field);
        total_score += term_score;
        matched += 1;
    }

    if matched == 0 {
        None
    } else {
        Some(total_score)
    }
}

/// Score a non-fuzzy term query using BM25.
/// Falls back to binary scoring for non-string fields (bool, int, etc.)
/// or when no postings are available.
fn score_term_query(
    document: &IndexDocument,
    term: &TermQuery,
    doc_id: u64,
    positions_readers: &[cloudsearch_storage::inverted_index::PositionsReader],
    bm25_ctx: &Bm25Context,
) -> Option<f32> {
    // Get the stored field value
    let stored = document.source.get(&term.field)?;

    // For non-string fields, use direct value equality (binary scoring)
    let Some(stored_str) = stored.as_str() else {
        return (stored == &term.value).then_some(1.0);
    };

    // Look up term value (lowercased) in positions readers
    let term_value = term.value.as_str()?;
    let term_key = term_value.to_lowercase();

    // Find TF for this document across all segment readers
    let mut tf = 0u32;
    for reader in positions_readers {
        if let Some(pl) = reader.get(&term_key)
            && let Ok(idx) = pl.docs.binary_search_by(|p| p.doc_id.cmp(&doc_id))
        {
            tf += pl.docs[idx].term_freq;
        }
    }

    // Fallback: if no postings (e.g., not flushed), approximate from field value
    if tf == 0 {
        let field_tokens = tokenize(stored_str);
        if field_tokens.contains(&term_key) {
            tf =
                u32::try_from(field_tokens.iter().filter(|t| *t == &term_key).count()).unwrap_or(0);
        }
    }

    if tf == 0 {
        return None;
    }

    // Look up IDF; missing terms use a default of 1.0 (moderately rare term)
    let idf = bm25_ctx.idf_map.get(&term_key).copied().unwrap_or(1.0);

    // Compute doc_len
    let field_tokens = tokenize(stored_str);
    let doc_len = field_tokens.len();

    Some(bm25_ctx.bm25_term_score(tf, doc_len, idf, &term.field))
}

/// Score a terms query (OR over multiple values) using BM25.
/// Returns the max BM25 score across all matching term values.
fn score_terms_query(
    document: &IndexDocument,
    terms: &TermsQuery,
    doc_id: u64,
    positions_readers: &[cloudsearch_storage::inverted_index::PositionsReader],
    bm25_ctx: &Bm25Context,
) -> Option<f32> {
    let mut max_score = 0.0f32;

    // Look up stored field value once
    let stored = document.source.get(&terms.field)?;

    for term_value in &terms.values {
        // Handle non-string term values (numbers, booleans): use value equality
        let Some(term_value_str) = term_value.as_str() else {
            if stored == term_value {
                max_score = max_score.max(1.0);
            }
            continue;
        };

        // For string term values, both term_value_str and stored_str must be strings
        let Some(stored_str) = stored.as_str() else {
            continue;
        };

        let term_key = term_value_str.to_lowercase();

        // Find TF for this document
        let mut tf = 0u32;
        for reader in positions_readers {
            if let Some(pl) = reader.get(&term_key)
                && let Ok(idx) = pl.docs.binary_search_by(|p| p.doc_id.cmp(&doc_id))
            {
                tf += pl.docs[idx].term_freq;
            }
        }
        if tf == 0 {
            let field_tokens = tokenize(stored_str);
            if field_tokens.contains(&term_key) {
                tf = u32::try_from(field_tokens.iter().filter(|t| *t == &term_key).count())
                    .unwrap_or(0);
            }
        }
        if tf == 0 {
            continue;
        }

        let idf = bm25_ctx.idf_map.get(&term_key).copied().unwrap_or(1.0);
        let field_tokens = tokenize(stored_str);
        let doc_len = field_tokens.len();
        let score = bm25_ctx.bm25_term_score(tf, doc_len, idf, &terms.field);
        max_score = max_score.max(score);
    }
    (max_score > 0.0).then_some(max_score)
}

/// Score a phrase query by checking if query terms appear consecutively in document text.
/// Uses positions data to verify proximity and scores using BM25 per term.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn score_phrase_query(
    document: &IndexDocument,
    phrase: &PhraseQuery,
    doc_id: u64,
    positions_readers: &[cloudsearch_storage::inverted_index::PositionsReader],
    bm25_ctx: &Bm25Context,
) -> Option<f32> {
    let field_str = document.source.get(&phrase.field)?.as_str()?;
    let field_tokens = tokenize(field_str);
    let doc_len = field_tokens.len();
    let query_tokens = tokenize(&phrase.value);
    if query_tokens.len() < 2 {
        return None;
    }

    // For each segment's positions reader, find matching phrase occurrences
    let mut best_score: Option<f32> = None;

    for reader in positions_readers {
        // Get positions of first term
        let first_term = &query_tokens[0];
        let Some(posting_list) = reader.get(first_term) else {
            continue;
        };

        for posting in &posting_list.docs {
            // Only consider postings for THIS document (matched by doc_id)
            if posting.doc_id != doc_id {
                continue;
            }
            let first_positions = &posting.positions;
            for &first_pos in first_positions {
                let first_pos = first_pos as usize;
                if first_pos >= field_str.len() {
                    continue;
                }

                // Check if remaining terms appear consecutively after first_pos
                let mut all_match = true;
                let mut max_gap: u32 = 0;
                let mut previous_pos = first_pos as u32;

                for term in query_tokens.iter().skip(1) {
                    let Some(next_list) = reader.get(term) else {
                        all_match = false;
                        break;
                    };

                    // Find a position of this term that is after previous_pos
                    // Must check ALL documents in the posting list, filtering by doc_id
                    let mut found_pos: Option<u32> = None;
                    for posting in &next_list.docs {
                        // Only consider postings for THIS document
                        if posting.doc_id != doc_id {
                            continue;
                        }
                        for p in &posting.positions {
                            if *p > previous_pos {
                                found_pos = Some(*p);
                                break;
                            }
                        }
                        if found_pos.is_some() {
                            break;
                        }
                    }

                    let Some(found_pos) = found_pos else {
                        all_match = false;
                        break;
                    };

                    // Calculate gap from previous term's position
                    let gap = found_pos.saturating_sub(previous_pos);
                    max_gap = max_gap.max(gap);
                    previous_pos = found_pos;
                }

                if all_match {
                    // Compute BM25 score per term and sum
                    let mut term_score_sum = 0.0f32;
                    for term in &query_tokens {
                        let mut tf = 0u32;
                        for reader in positions_readers {
                            if let Some(pl) = reader.get(term)
                                && let Ok(idx) = pl.docs.binary_search_by(|p| p.doc_id.cmp(&doc_id))
                            {
                                tf += pl.docs[idx].term_freq;
                            }
                        }
                        if tf == 0 {
                            // Fallback: count from field tokens
                            tf = field_tokens.iter().filter(|t| *t == term).count() as u32;
                        }
                        if tf == 0 {
                            continue;
                        }
                        let idf = bm25_ctx.idf_map.get(term).copied().unwrap_or(1.0);
                        term_score_sum += bm25_ctx.bm25_term_score(tf, doc_len, idf, &phrase.field);
                    }
                    // Apply gap penalty as divisor (not multiplier) to avoid amplifying
                    // already-large BM25 term scores. Exact consecutive phrases (gap=0) keep full score.
                    let score = if max_gap <= 10 {
                        term_score_sum
                    } else {
                        term_score_sum / (1.0 + max_gap as f32 * 0.1)
                    };
                    best_score = Some(best_score.map_or(score, |s| s.max(score)));
                }
            }
        }
    }

    best_score
}

fn tokenize(text: &str) -> Vec<String> {
    text.split_whitespace()
        .map(str::to_ascii_lowercase)
        .collect()
}

/// Returns whether the stored value fuzzy-matches the query term.
///
/// # Return Value Semantics
/// - `None` — field is absent from the document, or stored value is not a string.
///   Cannot participate in fuzzy matching.
/// - `Some(true)` — field is a string and matches within the fuzziness threshold.
/// - `Some(false)` — field is a string but edit distance exceeds the threshold.
///   The document does not match.
///
/// When `fuzziness` is `None`, performs exact comparison (handles bool, number,
/// string uniformly).
fn fuzzy_term_match(document: &IndexDocument, term: &TermQuery) -> Option<bool> {
    let stored = document.source.get(&term.field)?;

    // When no fuzziness, do exact comparison (handles bool, number, string)
    if term.fuzziness.is_none() {
        return (stored == &term.value).then_some(true);
    }

    // Fuzzy matching requires string values
    let stored_str = stored.as_str()?;
    let query_value = term.value.as_str()?;

    match &term.fuzziness {
        // The None branch is unreachable because we already checked is_none() above.
        // If this guard logic ever changes, a compilation failure is preferable to a
        // silent wrong-answer at runtime — so we use unreachable!() rather than None.
        None => unreachable!(),
        Some(Fuzziness::Auto) => {
            let threshold = match query_value.len() {
                0..=2 => 0,
                3..=5 => 1,
                _ => 2,
            };
            if threshold == 0 {
                (stored_str == query_value).then_some(true)
            } else {
                Some(levenshtein_distance(stored_str, query_value) <= threshold)
            }
        }
        Some(Fuzziness::Exact(max_dist)) => {
            if *max_dist == 0 {
                (stored_str == query_value).then_some(true)
            } else {
                Some(levenshtein_distance(stored_str, query_value) <= *max_dist)
            }
        }
    }
}

/// Compute the Levenshtein edit distance between two strings.
fn levenshtein_distance(a: &str, b: &str) -> usize {
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }

    let mut matrix = vec![vec![0usize; b.len() + 1]; a.len() + 1];

    for (i, row) in matrix.iter_mut().enumerate().take(a.len() + 1) {
        row[0] = i;
    }
    for (j, cell) in matrix[0].iter_mut().enumerate().take(b.len() + 1) {
        *cell = j;
    }

    for (i, ca) in a.char_indices() {
        for (j, cb) in b.char_indices() {
            let cost = usize::from(ca != cb);
            matrix[i + 1][j + 1] = (matrix[i][j + 1] + 1) // deletion
                .min(matrix[i + 1][j] + 1) // insertion
                .min(matrix[i][j] + cost); // substitution
        }
    }

    matrix[a.len()][b.len()]
}

/// Stable hash of a document ID string for use as a persistent `doc_id` in postings.
/// Using the string directly ensures the same ID always produces the same hash,
/// independent of enumeration order or segment boundaries.
fn hash_doc_id(id: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    hasher.finish()
}

/// Number of characters to include before the matched term in a highlight fragment.
const HIGHLIGHT_PRE_CONTEXT_CHARS: usize = 50;
/// Number of characters to include after the matched term in a highlight fragment.
const HIGHLIGHT_POST_CONTEXT_CHARS: usize = 30;

/// Extract highlight fragments from a document's text fields using position data.
#[allow(clippy::cast_possible_truncation)]
fn extract_highlight(
    doc: &IndexDocument,
    doc_id: u64,
    positions_readers: &[cloudsearch_storage::inverted_index::PositionsReader],
    query: &SearchQuery,
) -> Option<std::collections::BTreeMap<String, Vec<String>>> {
    let query_terms = get_query_terms(query);
    if query_terms.is_empty() {
        return None;
    }

    let mut result: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();

    // For each text field in the document, look up positions of matched terms
    if let Some(obj) = doc.source.as_object() {
        for (field_name, field_value) in obj {
            if let Some(text) = field_value.as_str() {
                let mut fragments: Vec<(usize, String)> = Vec::new(); // (start_byte, fragment)

                for term in &query_terms {
                    // Search all segment positions readers
                    for positions_reader in positions_readers {
                        if let Some(posting_list) = positions_reader.get(term) {
                            // Find the posting for this document (by doc_id)
                            for posting in &posting_list.docs {
                                if posting.doc_id != doc_id {
                                    continue;
                                }
                                for &byte_pos in &posting.positions {
                                    let pos = byte_pos as usize;
                                    if pos >= text.len() {
                                        continue;
                                    }
                                    // Extract window around match: pre chars before, matched term, post chars after
                                    let pre_start = pos.saturating_sub(HIGHLIGHT_PRE_CONTEXT_CHARS);
                                    let pre = &text[pre_start..pos];
                                    let term_match =
                                        &text[pos..pos.saturating_add(term.len()).min(text.len())];
                                    let post_end = pos
                                        .saturating_add(term.len() + HIGHLIGHT_POST_CONTEXT_CHARS)
                                        .min(text.len());
                                    let post = if pos.saturating_add(term.len()) < post_end {
                                        &text[pos.saturating_add(term.len())..post_end]
                                    } else {
                                        &text[post_end..post_end]
                                    };

                                    let fragment = format!("{pre}<em>{term_match}</em>{post}");
                                    fragments.push((pre_start, fragment));
                                }
                            }
                        }
                    }
                }

                // Deduplicate and limit to 3 fragments per field
                fragments.sort_by_key(|(start, _)| *start);
                let unique: Vec<String> = fragments.into_iter().map(|(_, f)| f).take(3).collect();

                if !unique.is_empty() {
                    result.insert(field_name.clone(), unique);
                }
            }
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Recursively extract all terms from a `SearchQuery` for highlighting.
fn get_query_terms(query: &SearchQuery) -> Vec<String> {
    match query {
        SearchQuery::MatchAll | SearchQuery::Range(_) => Vec::new(),
        SearchQuery::Term(term) => term
            .value
            .as_str()
            .map(str::to_lowercase)
            .into_iter()
            .collect(),
        SearchQuery::Terms(terms) => terms
            .values
            .iter()
            .filter_map(|v| v.as_str().map(str::to_lowercase))
            .collect(),
        SearchQuery::Bool(bool_query) => bool_query
            .must
            .iter()
            .chain(bool_query.should.iter())
            .chain(bool_query.filter.iter())
            .flat_map(get_query_terms)
            .collect(),
        SearchQuery::Prefix(prefix) => tokenize(&prefix.value),
        SearchQuery::Wildcard(wc) => tokenize(&wc.value),
        SearchQuery::Match(mq) => tokenize(&mq.value),
        SearchQuery::Phrase(phrase) => tokenize(&phrase.value),
    }
}

fn matches_prefix_query(document: &IndexDocument, prefix: &PrefixQuery) -> bool {
    document
        .source
        .get(&prefix.field)
        .is_some_and(|value| value.as_str().is_some_and(|s| s.starts_with(&prefix.value)))
}

fn matches_wildcard_query(document: &IndexDocument, wildcard: &WildcardQuery) -> bool {
    let Some(re) = build_wildcard_regex(&wildcard.value) else {
        return false;
    };
    document
        .source
        .get(&wildcard.field)
        .is_some_and(|value| value.as_str().is_some_and(|text| re.is_match(text)))
}

fn build_wildcard_regex(pattern: &str) -> Option<Regex> {
    let regex_pattern: String = pattern
        .chars()
        .map(|c| match c {
            '*' => ".*".to_string(),
            '?' => ".".to_string(),
            other => regex::escape(&other.to_string()),
        })
        .collect();
    Regex::new(&format!("^{regex_pattern}$")).ok()
}

/// Score a prefix query using BM25 by summing BM25 scores for all tokens
/// that start with the prefix. Uses binary matching to filter candidates first.
fn score_prefix_query_bm25(
    document: &IndexDocument,
    prefix: &PrefixQuery,
    doc_id: u64,
    positions_readers: &[cloudsearch_storage::inverted_index::PositionsReader],
    bm25_ctx: &Bm25Context,
) -> Option<f32> {
    let stored = document.source.get(&prefix.field)?.as_str()?;
    let stored_lowercase = stored.to_lowercase();
    let field_tokens = tokenize(stored);
    let doc_len = field_tokens.len();
    let prefix_lower = prefix.value.to_lowercase();

    // Check each field token: if it starts with the prefix, score it using BM25.
    // This handles both keyword-style fields (full value match) and text fields (token match).
    let mut total_score = 0.0f32;
    let mut matched_any = false;

    for token in &field_tokens {
        if !token.starts_with(&prefix_lower) {
            continue;
        }
        matched_any = true;
        let mut tf = 0u32;
        for reader in positions_readers {
            if let Some(pl) = reader.get(token)
                && let Ok(idx) = pl.docs.binary_search_by(|p| p.doc_id.cmp(&doc_id))
            {
                tf += pl.docs[idx].term_freq;
            }
        }
        if tf == 0 {
            tf = u32::try_from(field_tokens.iter().filter(|t| *t == token).count()).unwrap_or(0);
        }
        if tf == 0 {
            continue;
        }
        let idf = bm25_ctx
            .idf_map
            .get(token)
            .copied()
            .or_else(|| bm25_ctx.idf_map.get(&stored_lowercase).copied())
            .unwrap_or(1.0);
        total_score += bm25_ctx.bm25_term_score(tf, doc_len, idf, &prefix.field);
    }

    if matched_any { Some(total_score) } else { None }
}

/// Score a wildcard query using BM25 by enumerating all terms matching the pattern
/// and summing their BM25 scores. Uses binary matching to filter candidates first.
fn score_wildcard_query_bm25(
    document: &IndexDocument,
    wildcard: &WildcardQuery,
    doc_id: u64,
    positions_readers: &[cloudsearch_storage::inverted_index::PositionsReader],
    bm25_ctx: &Bm25Context,
) -> Option<f32> {
    let re = build_wildcard_regex(&wildcard.value)?;
    let stored = document.source.get(&wildcard.field)?.as_str()?;
    let field_tokens = tokenize(stored);
    let doc_len = field_tokens.len();

    // Score each field token that matches the wildcard pattern
    let mut total_score = 0.0f32;
    for token in &field_tokens {
        if !re.is_match(token) {
            continue;
        }
        let mut tf = 0u32;
        for reader in positions_readers {
            if let Some(pl) = reader.get(token)
                && let Ok(idx) = pl.docs.binary_search_by(|p| p.doc_id.cmp(&doc_id))
            {
                tf += pl.docs[idx].term_freq;
            }
        }
        if tf == 0 {
            tf = u32::try_from(field_tokens.iter().filter(|t| *t == token).count()).unwrap_or(0);
        }
        if tf == 0 {
            continue;
        }
        let idf = bm25_ctx.idf_map.get(token).copied().unwrap_or(1.0);
        total_score += bm25_ctx.bm25_term_score(tf, doc_len, idf, &wildcard.field);
    }
    (total_score > 0.0).then_some(total_score)
}

#[allow(clippy::cast_precision_loss)]
fn score_bool_query(
    document: &IndexDocument,
    bool_query: &BoolQuery,
    doc_id: u64,
    positions_readers: &[cloudsearch_storage::inverted_index::PositionsReader],
    bm25_ctx: &Bm25Context,
) -> Option<f32> {
    // Evaluate each clause group once and store the scores.
    let must_scores: Vec<Option<f32>> = bool_query
        .must
        .iter()
        .map(|q| score_query(document, q, doc_id, positions_readers, bm25_ctx))
        .collect();
    let filter_scores: Vec<Option<f32>> = bool_query
        .filter
        .iter()
        .map(|q| score_query(document, q, doc_id, positions_readers, bm25_ctx))
        .collect();
    let must_not_scores: Vec<Option<f32>> = bool_query
        .must_not
        .iter()
        .map(|q| score_query(document, q, doc_id, positions_readers, bm25_ctx))
        .collect();
    let should_scores: Vec<Option<f32>> = bool_query
        .should
        .iter()
        .map(|q| score_query(document, q, doc_id, positions_readers, bm25_ctx))
        .collect();

    // All must clauses must match.
    if must_scores.iter().any(std::option::Option::is_none) {
        return None;
    }
    // All filter clauses must match (not scored).
    if filter_scores.iter().any(std::option::Option::is_none) {
        return None;
    }
    // No must_not clause may match.
    if must_not_scores.iter().any(std::option::Option::is_some) {
        return None;
    }
    // When there are no must/filter clauses, at least one should must match.
    let should_required =
        bool_query.must.is_empty() && bool_query.filter.is_empty() && !bool_query.should.is_empty();
    if should_required && !should_scores.iter().any(std::option::Option::is_some) {
        return None;
    }

    // Score = average of must + matching should scores.
    let (sum, count) =
        must_scores
            .iter()
            .chain(should_scores.iter())
            .fold((0.0f32, 0usize), |(sum, count), s| match s {
                Some(s) => (sum + s, count + 1),
                None => (sum, count),
            });

    Some(if count > 0 { sum / count as f32 } else { 1.0 })
}

#[allow(clippy::cast_precision_loss)]
fn compute_aggregations(
    documents: &[IndexDocument],
    requests: Option<&BTreeMap<String, AggregationRequest>>,
    doc_values: Option<&DocValuesReader>,
) -> BTreeMap<String, AggregationResult> {
    let mut aggregations = BTreeMap::new();

    let Some(requests) = requests else {
        return aggregations;
    };

    for (name, request) in requests {
        let result = match request {
            AggregationRequest::Terms(terms) => AggregationResult::Terms(
                compute_terms_aggregation(documents, &terms.field, doc_values),
            ),
            AggregationRequest::Stats(stats) => AggregationResult::Stats(
                compute_stats_aggregation(documents, &stats.field, doc_values),
            ),
            AggregationRequest::DateHistogram(histogram) => {
                AggregationResult::DateHistogram(compute_date_histogram_aggregation(
                    documents,
                    &histogram.field,
                    &histogram.interval,
                    doc_values,
                ))
            }
        };

        aggregations.insert(name.clone(), result);
    }

    aggregations
}

fn compute_terms_aggregation(
    documents: &[IndexDocument],
    field: &str,
    doc_values: Option<&DocValuesReader>,
) -> TermsAggregationResult {
    // Use doc values if available
    if let Some(reader) = doc_values
        && let Some(keywords) = reader.keywords(field)
    {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for k in keywords {
            *counts.entry(k.to_string()).or_insert(0) += 1;
        }
        let mut buckets: Vec<TermsBucket> = counts
            .into_iter()
            .map(|(key, doc_count)| TermsBucket {
                key: serde_json::Value::String(key),
                doc_count,
            })
            .collect();
        buckets.sort_by_key(|b| std::cmp::Reverse(b.doc_count));
        return TermsAggregationResult { buckets };
    }

    // Fall back to JSON extraction
    let mut counts: HashMap<String, (serde_json::Value, usize)> = HashMap::new();
    for document in documents {
        let Some(value) = document.source.get(field) else {
            continue;
        };
        if !matches!(
            value,
            serde_json::Value::String(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Bool(_)
        ) {
            continue;
        }
        let key = value.to_string();
        let entry = counts
            .entry(key.clone())
            .or_insert_with(|| (value.clone(), 0));
        entry.1 += 1;
    }

    let mut buckets: Vec<TermsBucket> = counts
        .into_values()
        .map(|(key, doc_count)| TermsBucket { key, doc_count })
        .collect();

    buckets.sort_by(|left, right| {
        right
            .doc_count
            .cmp(&left.doc_count)
            .then_with(|| left.key.to_string().cmp(&right.key.to_string()))
    });

    TermsAggregationResult { buckets }
}

#[allow(clippy::cast_precision_loss)]
fn compute_stats_aggregation(
    documents: &[IndexDocument],
    field: &str,
    doc_values: Option<&DocValuesReader>,
) -> StatsAggregationResult {
    // Use doc values if available
    if let Some(reader) = doc_values {
        if let Some(values) = reader.f64_values(field) {
            let count = values.len();
            let sum = values.iter().sum::<f64>();
            let min = values.iter().copied().reduce(f64::min);
            let max = values.iter().copied().reduce(f64::max);
            let avg = if count > 0 {
                Some(sum / count as f64)
            } else {
                None
            };
            return StatsAggregationResult {
                count,
                min,
                max,
                avg,
                sum,
            };
        }
        if let Some(values) = reader.i64_values(field) {
            let count = values.len();
            let sum: f64 = values.iter().copied().map(|v| v as f64).sum();
            let min = values.iter().copied().map(|v| v as f64).reduce(f64::min);
            let max = values.iter().copied().map(|v| v as f64).reduce(f64::max);
            let avg = if count > 0 {
                Some(sum / count as f64)
            } else {
                None
            };
            return StatsAggregationResult {
                count,
                min,
                max,
                avg,
                sum,
            };
        }
    }

    // Fall back to JSON extraction
    let values = documents
        .iter()
        .filter_map(|document| document.source.get(field))
        .filter_map(serde_json::Value::as_f64)
        .collect::<Vec<_>>();

    let count = values.len();
    let sum = values.iter().sum::<f64>();
    let min = values.iter().copied().reduce(f64::min);
    let max = values.iter().copied().reduce(f64::max);
    let avg = (count > 0).then(|| sum / count as f64);
    StatsAggregationResult {
        count,
        min,
        max,
        avg,
        sum,
    }
}

fn compute_date_histogram_aggregation(
    documents: &[IndexDocument],
    field: &str,
    interval: &DateHistogramInterval,
    doc_values: Option<&DocValuesReader>,
) -> DateHistogramAggregationResult {
    // Try doc values first (timestamps stored as i64 millis)
    if let Some(reader) = doc_values
        && let Some(values) = reader.i64_values(field)
    {
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for ts_millis in values {
            if let Some(ts) = DateTime::from_timestamp_millis(ts_millis) {
                let bucket = truncate_timestamp(ts, interval)
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                *counts.entry(bucket).or_default() += 1;
            }
        }
        let buckets: Vec<DateHistogramBucket> = counts
            .into_iter()
            .map(|(key, doc_count)| DateHistogramBucket { key, doc_count })
            .collect();
        return DateHistogramAggregationResult { buckets };
    }

    // Fall back to JSON parsing
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for document in documents {
        let Some(raw) = document
            .source
            .get(field)
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let Ok(timestamp) = DateTime::parse_from_rfc3339(raw) else {
            continue;
        };
        let bucket_key = truncate_timestamp(timestamp.with_timezone(&Utc), interval)
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        *counts.entry(bucket_key).or_default() += 1;
    }

    let buckets: Vec<DateHistogramBucket> = counts
        .into_iter()
        .map(|(key, doc_count)| DateHistogramBucket { key, doc_count })
        .collect();
    DateHistogramAggregationResult { buckets }
}

fn truncate_timestamp(timestamp: DateTime<Utc>, interval: &DateHistogramInterval) -> DateTime<Utc> {
    match interval {
        DateHistogramInterval::Minute => timestamp
            .with_second(0)
            .and_then(|ts| ts.with_nanosecond(0))
            .expect("valid minute truncation"),
        DateHistogramInterval::Hour => timestamp
            .with_minute(0)
            .and_then(|ts| ts.with_second(0))
            .and_then(|ts| ts.with_nanosecond(0))
            .expect("valid hour truncation"),
        DateHistogramInterval::Day => timestamp
            .with_hour(0)
            .and_then(|ts| ts.with_minute(0))
            .and_then(|ts| ts.with_second(0))
            .and_then(|ts| ts.with_nanosecond(0))
            .expect("valid day truncation"),
    }
}

fn matches_range_query(document: &IndexDocument, range: &RangeQuery) -> bool {
    let Some(value) = document.source.get(&range.field) else {
        return false;
    };

    match comparable_value(value) {
        Some(ComparableValue::Number(number)) => matches_numeric_range(number, range),
        Some(ComparableValue::Timestamp(timestamp)) => matches_timestamp_range(timestamp, range),
        Some(ComparableValue::String(_) | ComparableValue::Boolean(_)) | None => false,
    }
}

enum ComparableValue {
    Number(f64),
    Timestamp(DateTime<Utc>),
    String(String),
    Boolean(bool),
}

fn comparable_value(value: &serde_json::Value) -> Option<ComparableValue> {
    if let Some(number) = value.as_f64() {
        return Some(ComparableValue::Number(number));
    }

    if let Some(boolean) = value.as_bool() {
        return Some(ComparableValue::Boolean(boolean));
    }

    if let Some(raw) = value.as_str() {
        if let Ok(timestamp) = DateTime::parse_from_rfc3339(raw) {
            return Some(ComparableValue::Timestamp(timestamp.with_timezone(&Utc)));
        }

        return Some(ComparableValue::String(raw.to_string()));
    }

    None
}

/// Compute sort values for a document.
/// Returns a vector: [`sort_field_value`, `tie_breaker`].
/// Tie-breaker is [`score`, `doc_id`].
/// This is used for both `search_after` cursor positioning and response `sort_values`.
fn compute_sort_values(
    doc: &IndexDocument,
    sort: Option<&SortSpec>,
    score: f32,
) -> Vec<serde_json::Value> {
    let mut values = Vec::with_capacity(2);
    if let Some(sort_spec) = sort {
        if let Some(field_value) = doc.source.get(&sort_spec.field) {
            values.push(field_value.clone());
        } else {
            values.push(serde_json::Value::Null);
        }
    }
    // Tie-breaker: include score and doc_id to ensure uniqueness
    values.push(serde_json::Value::Number(
        serde_json::Number::from_f64(f64::from(score)).unwrap_or(serde_json::Number::from(0)),
    ));
    values.push(serde_json::Value::String(doc.id.clone()));
    values
}

/// Compare a document's sort values against a `search_after` cursor.
/// Returns `Ordering::Greater` when doc should come AFTER the cursor
/// (i.e., cursor is smaller than or equal to doc's values).
fn compare_sort_values_list(
    doc_values: &[serde_json::Value],
    cursor: &[serde_json::Value],
    sort: Option<&SortSpec>,
) -> std::cmp::Ordering {
    for (i, cursor_val) in cursor.iter().enumerate() {
        if i >= doc_values.len() {
            return std::cmp::Ordering::Less;
        }
        let doc_val = &doc_values[i];
        let ordering = compare_json_values(doc_val, cursor_val, sort);
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_json_values(
    left: &serde_json::Value,
    right: &serde_json::Value,
    sort: Option<&SortSpec>,
) -> std::cmp::Ordering {
    let left_comp = comparable_value(left);
    let right_comp = comparable_value(right);

    let ordering = match (left_comp, right_comp) {
        (Some(l), Some(r)) => match (&l, &r) {
            (ComparableValue::Number(ln), ComparableValue::Number(rn)) => ln.total_cmp(rn),
            (ComparableValue::Timestamp(lt), ComparableValue::Timestamp(rt)) => lt.cmp(rt),
            (ComparableValue::String(ls), ComparableValue::String(rs)) => ls.cmp(rs),
            (ComparableValue::Boolean(lb), ComparableValue::Boolean(rb)) => lb.cmp(rb),
            _ => std::cmp::Ordering::Equal,
        },
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    };

    if let Some(sort_spec) = sort {
        match sort_spec.order {
            SortOrder::Asc => ordering,
            SortOrder::Desc => ordering.reverse(),
        }
    } else {
        ordering
    }
}

fn compare_hits(left: &SearchHit, right: &SearchHit, sort: &SortSpec) -> std::cmp::Ordering {
    let left_value = left.source.get(&sort.field).and_then(comparable_value);
    let right_value = right.source.get(&sort.field).and_then(comparable_value);

    let ordering = match (left_value, right_value) {
        (Some(left), Some(right)) => compare_sort_values(&left, &right, sort),
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, None) => left.id.cmp(&right.id),
    };

    if ordering == std::cmp::Ordering::Equal {
        left.id.cmp(&right.id)
    } else {
        ordering
    }
}

fn compare_sort_values(
    left: &ComparableValue,
    right: &ComparableValue,
    sort: &SortSpec,
) -> std::cmp::Ordering {
    let ordering = match (left, right) {
        (ComparableValue::Number(left), ComparableValue::Number(right)) => left.total_cmp(right),
        (ComparableValue::Timestamp(left), ComparableValue::Timestamp(right)) => left.cmp(right),
        (ComparableValue::String(left), ComparableValue::String(right)) => left.cmp(right),
        (ComparableValue::Boolean(left), ComparableValue::Boolean(right)) => left.cmp(right),
        _ => std::cmp::Ordering::Equal,
    };

    match sort.order {
        SortOrder::Asc => ordering,
        SortOrder::Desc => ordering.reverse(),
    }
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

fn validate_namespace(namespace: &str) -> Result<()> {
    if namespace.is_empty() {
        return Err(CloudSearchError::InvalidNamespace(
            "namespace cannot be empty".to_string(),
        ));
    }
    if namespace.len() > 64 {
        return Err(CloudSearchError::InvalidNamespace(
            "namespace cannot exceed 64 characters".to_string(),
        ));
    }
    if !namespace
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(CloudSearchError::InvalidNamespace(
            "namespace must be alphanumeric, hyphens, or underscores".to_string(),
        ));
    }
    Ok(())
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
        AggregationRequest, AggregationResult, BoolQuery, BulkDeleteOperation, BulkIndexOperation,
        BulkOperation, BulkRequest, CreateIndexRequest, DateHistogramAggregationRequest,
        DateHistogramInterval, FieldType, IndexSettings, MappingMode, MatchQuery, PrefixQuery,
        RangeQuery, SearchQuery, SearchRequest, SortOrder, SortSpec, StatsAggregationRequest,
        TermQuery, TermsAggregationRequest, TermsQuery, WildcardQuery,
    };
    use tempfile::TempDir;
    use tokio::sync::Barrier;

    #[allow(dead_code)]
    async fn test_catalog() -> (TempDir, IndexCatalog) {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");
        (temp_dir, catalog)
    }

    #[allow(dead_code)]
    fn doc(id: &str, source: serde_json::Value) -> IndexDocument {
        IndexDocument {
            id: id.to_string(),
            source,
        }
    }

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
                        namespace: None,
                        retention_secs: None,
                        merge_threshold_docs: None,
                    },
                    ..Default::default()
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
    async fn creates_index_with_namespace() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let metadata = catalog
            .create_index(
                "logs_v1",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::ControlledDynamic,
                        primary_time_field: None,
                        namespace: Some("tenant-abc".to_string()),
                        retention_secs: None,
                        merge_threshold_docs: None,
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let loaded = catalog.get_index("logs_v1").await.expect("load index");

        assert_eq!(loaded.name, metadata.name);
        assert_eq!(loaded.settings.namespace.as_deref(), Some("tenant-abc"));
    }

    #[tokio::test]
    async fn rejects_invalid_namespace_empty() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let error = catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::ControlledDynamic,
                        primary_time_field: None,
                        namespace: Some(String::new()),
                        retention_secs: None,
                        merge_threshold_docs: None,
                    },
                    ..Default::default()
                },
            )
            .await
            .expect_err("empty namespace should fail");

        assert!(matches!(error, CloudSearchError::InvalidNamespace(_)));
    }

    #[tokio::test]
    async fn rejects_invalid_namespace_too_long() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let long_namespace = "a".repeat(65);
        let error = catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::ControlledDynamic,
                        primary_time_field: None,
                        namespace: Some(long_namespace),
                        retention_secs: None,
                        merge_threshold_docs: None,
                    },
                    ..Default::default()
                },
            )
            .await
            .expect_err("too long namespace should fail");

        assert!(matches!(error, CloudSearchError::InvalidNamespace(_)));
    }

    #[tokio::test]
    async fn rejects_invalid_namespace_special_chars() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let error = catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::ControlledDynamic,
                        primary_time_field: None,
                        namespace: Some("tenant@abc".to_string()),
                        retention_secs: None,
                        merge_threshold_docs: None,
                    },
                    ..Default::default()
                },
            )
            .await
            .expect_err("special char namespace should fail");

        assert!(matches!(error, CloudSearchError::InvalidNamespace(_)));
    }

    #[tokio::test]
    async fn accepts_max_length_namespace() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let ns_64 = "a".repeat(64);
        let metadata = catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::ControlledDynamic,
                        primary_time_field: None,
                        namespace: Some(ns_64.clone()),
                        retention_secs: None,
                        merge_threshold_docs: None,
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("64-char namespace should succeed");

        assert_eq!(metadata.settings.namespace.as_deref(), Some(ns_64.as_str()));
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
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let error = catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("duplicate should fail");

        assert!(matches!(error, CloudSearchError::IndexAlreadyExists(_)));
    }

    #[tokio::test]
    async fn rejects_uppercase_index_name() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let error = catalog
            .create_index(
                "Logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("uppercase should fail");

        assert!(matches!(error, CloudSearchError::InvalidIndexName(_)));
    }

    #[tokio::test]
    async fn rejects_index_name_too_long() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let long_name = "a".repeat(256);
        let error = catalog
            .create_index(
                &long_name,
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("256 chars should fail");

        assert!(matches!(error, CloudSearchError::InvalidIndexName(_)));
    }

    #[tokio::test]
    async fn rejects_index_name_with_special_chars() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        for name in ["log s", "log.name", "log@name", "log/name"] {
            let error = catalog
                .create_index(
                    name,
                    CreateIndexRequest {
                        settings: IndexSettings::default(),
                        ..Default::default()
                    },
                )
                .await
                .expect_err(name);
            assert!(
                matches!(error, CloudSearchError::InvalidIndexName(_)),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn delete_index_removes_storage_and_allows_recreate() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let index_dir = temp_dir.path().join("indexes").join("logs");
        assert!(index_dir.exists());

        catalog.delete_index("logs").await.expect("delete index");
        assert!(!index_dir.exists());

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("recreate index");

        assert!(index_dir.exists());
    }

    #[tokio::test]
    async fn delete_missing_index_returns_not_found() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let error = catalog
            .delete_index("missing")
            .await
            .expect_err("missing index should fail");

        assert!(matches!(error, CloudSearchError::IndexNotFound(_)));
    }

    #[tokio::test]
    async fn registry_reuses_and_evicted_handles_correctly() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let registry = Arc::new(IndexRegistry::new(catalog));

        registry
            .catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let barrier = Arc::new(Barrier::new(2));
        let first_task = {
            let registry = registry.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.index_handle("logs").await.expect("first handle")
            })
        };
        let second_task = {
            let registry = registry.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.index_handle("logs").await.expect("second handle")
            })
        };

        let first = first_task.await.expect("join first task");
        let second = second_task.await.expect("join second task");
        assert!(Arc::ptr_eq(&first, &second));

        registry.delete_index("logs").await.expect("delete index");

        registry
            .catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("recreate index");

        let barrier = Arc::new(Barrier::new(2));
        let third_task = {
            let registry = registry.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.index_handle("logs").await.expect("third handle")
            })
        };
        let fourth_task = {
            let registry = registry.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.index_handle("logs").await.expect("fourth handle")
            })
        };

        let third = third_task.await.expect("join third task");
        let fourth = fourth_task.await.expect("join fourth task");
        assert!(Arc::ptr_eq(&third, &fourth));
        assert!(!Arc::ptr_eq(&first, &third));
    }

    #[tokio::test]
    async fn infers_and_persists_mappings_across_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({
                    "service": "billing",
                    "latency": 42,
                    "active": true,
                    "timestamp": "2026-03-14T10:00:00Z"
                }),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(
            reopened.metadata().mappings["service"].field_type,
            FieldType::Keyword
        );
        assert_eq!(
            reopened.metadata().mappings["latency"].field_type,
            FieldType::Integer
        );
        assert_eq!(
            reopened.metadata().mappings["active"].field_type,
            FieldType::Boolean
        );
        assert_eq!(
            reopened.metadata().mappings["timestamp"].field_type,
            FieldType::Timestamp
        );
    }

    #[tokio::test]
    async fn strict_mode_rejects_unknown_fields_and_arrays() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::Strict,
                        primary_time_field: None,
                        namespace: None,
                        retention_secs: None,
                        merge_threshold_docs: None,
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");

        let unknown = handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing"}),
            })
            .await
            .expect_err("strict mode should reject unknown field");
        assert!(matches!(unknown, CloudSearchError::UnknownFieldRejected(_)));

        let array = handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"services": ["billing", "search"]}),
            })
            .await
            .expect_err("arrays should be rejected");
        assert!(matches!(array, CloudSearchError::UnsupportedArrayField(_)));
    }

    #[tokio::test]
    async fn explicit_mappings_are_respected_and_conflicts_are_rejected() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let mut mappings = BTreeMap::new();
        mappings.insert(
            "status".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
            },
        );
        mappings.insert(
            "count".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
            },
        );

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::ControlledDynamic,
                        primary_time_field: None,
                        namespace: None,
                        retention_secs: None,
                        merge_threshold_docs: None,
                    },
                    mappings: Some(mappings),
                },
            )
            .await
            .expect("create index with explicit mappings");

        let mut handle = catalog.open_index("logs").await.expect("open index");

        // doc matching declared types succeeds
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"status": "active", "count": 42}),
            })
            .await
            .expect("index doc with matching declared types");

        // type conflict on declared field is rejected
        let conflict = handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"status": 123, "count": 1}),
            })
            .await
            .expect_err("keyword field receiving integer should fail");
        assert!(matches!(conflict, CloudSearchError::MappingConflict(_)));

        // new fields are inferred normally in ControlledDynamic mode
        handle
            .index_document(IndexDocument {
                id: "doc-3".to_string(),
                source: serde_json::json!({"status": "ok", "count": 10, "message": "hello"}),
            })
            .await
            .expect("new fields inferred normally");

        // check mappings were persisted
        let loaded = catalog.get_index("logs").await.expect("get index");
        assert_eq!(loaded.mappings["status"].field_type, FieldType::Keyword);
        assert_eq!(loaded.mappings["count"].field_type, FieldType::Integer);
        assert_eq!(loaded.mappings["message"].field_type, FieldType::Keyword);
    }

    #[tokio::test]
    async fn rejects_mapping_conflicts_and_invalid_query_usage() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "meta": {"host": "a"}}),
            })
            .await
            .expect("index doc");

        let conflict = handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"meta": "not-an-object"}),
            })
            .await
            .expect_err("conflict should fail");
        assert!(matches!(conflict, CloudSearchError::MappingConflict(_)));

        let invalid_query = handle.validate_search_request(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "service".to_string(),
                gte: Some(serde_json::json!("a")),
                gt: None,
                lte: None,
                lt: None,
            })),
            ..Default::default()
        });
        assert!(matches!(
            invalid_query,
            Err(CloudSearchError::InvalidSearchRequest(_))
        ));
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
    async fn flush_persists_searchable_state_but_not_pending_writes() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "visible"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");
        handle.flush().await.expect("flush");

        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "pending"}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(reopened.search(&SearchRequest::default()).hits.total, 2);
        assert_eq!(
            reopened.get_document("doc-1").unwrap().source["message"],
            "visible"
        );
        assert_eq!(
            reopened.get_document("doc-2").unwrap().source["message"],
            "pending"
        );
    }

    #[tokio::test]
    async fn flush_response_reports_sequence_and_document_count() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
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

        let flushed = handle.flush().await.expect("flush");

        assert_eq!(flushed.result, "flushed");
        assert_eq!(flushed.flushed_documents, 1);
        assert_eq!(flushed.sequence_number, 1);
    }

    #[tokio::test]
    async fn flush_rolls_over_and_trims_covered_wal_generations() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
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
        handle.flush().await.expect("flush");

        let wal_dir = temp_dir.path().join("indexes").join("logs").join("wal");
        assert!(!wal_dir.join("000001.log").exists());

        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "tail"}),
            })
            .await
            .expect("index doc");

        assert!(wal_dir.join("000002.log").exists());
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                fuzziness: None,
            })),
            ..Default::default()
        });
        assert_eq!(term.hits.total, 1);
        assert_eq!(term.hits.hits[0].id, "doc-1");

        let filtered = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![SearchQuery::Term(TermQuery {
                    field: "level".to_string(),
                    value: serde_json::json!("info"),
                    fuzziness: None,
                })],
                ..Default::default()
            })),
            ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                fuzziness: None,
            })),
            ..Default::default()
        });
        assert_eq!(missing_field.hits.total, 0);

        let multiple_filters = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![
                    SearchQuery::Term(TermQuery {
                        field: "service".to_string(),
                        value: serde_json::json!("billing"),
                        fuzziness: None,
                    }),
                    SearchQuery::Term(TermQuery {
                        field: "active".to_string(),
                        value: serde_json::json!(true),
                        fuzziness: None,
                    }),
                ],
                ..Default::default()
            })),
            ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                fuzziness: None,
            })),
            ..Default::default()
        });
        assert_eq!(bool_query.hits.total, 1);

        let numeric_query = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "latency".to_string(),
                value: serde_json::json!(42),
                fuzziness: None,
            })),
            ..Default::default()
        });
        assert_eq!(numeric_query.hits.total, 1);

        let wrong_type = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "latency".to_string(),
                value: serde_json::json!("42"),
                fuzziness: None,
            })),
            ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
                    settings: IndexSettings::default(),
                    ..Default::default()
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
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![],
                ..Default::default()
            })),
            ..Default::default()
        });

        assert_eq!(result.hits.total, 2);
    }

    #[tokio::test]
    async fn expanded_bool_query_supports_must_should_and_must_not() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            (
                "doc-1",
                serde_json::json!({"service": "billing", "level": "info"}),
            ),
            (
                "doc-2",
                serde_json::json!({"service": "billing", "level": "error"}),
            ),
            (
                "doc-3",
                serde_json::json!({"service": "search", "level": "info"}),
            ),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let must = handle.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                must: vec![SearchQuery::Term(TermQuery {
                    field: "service".to_string(),
                    value: serde_json::json!("billing"),
                    fuzziness: None,
                })],
                ..Default::default()
            })),
            ..Default::default()
        });
        assert_eq!(must.hits.total, 2);

        let should = handle.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                should: vec![
                    SearchQuery::Term(TermQuery {
                        field: "service".to_string(),
                        value: serde_json::json!("billing"),
                        fuzziness: None,
                    }),
                    SearchQuery::Term(TermQuery {
                        field: "service".to_string(),
                        value: serde_json::json!("search"),
                        fuzziness: None,
                    }),
                ],
                ..Default::default()
            })),
            ..Default::default()
        });
        assert_eq!(should.hits.total, 3);

        let must_not = handle.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![SearchQuery::Term(TermQuery {
                    field: "service".to_string(),
                    value: serde_json::json!("billing"),
                    fuzziness: None,
                })],
                must_not: vec![SearchQuery::Term(TermQuery {
                    field: "level".to_string(),
                    value: serde_json::json!("error"),
                    fuzziness: None,
                })],
                ..Default::default()
            })),
            ..Default::default()
        });
        assert_eq!(must_not.hits.total, 1);
        assert_eq!(must_not.hits.hits[0].id, "doc-1");
    }

    #[tokio::test]
    async fn bulk_apply_orders_updates_and_requires_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        let response = handle
            .bulk_apply(BulkRequest {
                operations: vec![
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "first"}),
                    }),
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "second"}),
                    }),
                    BulkOperation::Delete(BulkDeleteOperation {
                        id: "doc-2".to_string(),
                    }),
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-2".to_string(),
                        source: serde_json::json!({"message": "survivor"}),
                    }),
                ],
            })
            .await
            .expect("bulk apply");

        assert!(!response.errors);
        assert_eq!(response.items.len(), 4);
        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 0);

        handle.refresh().await.expect("refresh");

        let all = handle.search(&SearchRequest::default());
        assert_eq!(all.hits.total, 2);
        assert_eq!(
            handle.get_document("doc-1").unwrap().source["message"],
            "second"
        );
        assert_eq!(
            handle.get_document("doc-2").unwrap().source["message"],
            "survivor"
        );
    }

    #[tokio::test]
    async fn bulk_state_survives_reopen_after_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .bulk_apply(BulkRequest {
                operations: vec![
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"service": "billing"}),
                    }),
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-2".to_string(),
                        source: serde_json::json!({"service": "search"}),
                    }),
                    BulkOperation::Delete(BulkDeleteOperation {
                        id: "doc-1".to_string(),
                    }),
                ],
            })
            .await
            .expect("bulk apply");
        handle.refresh().await.expect("refresh");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(reopened.search(&SearchRequest::default()).hits.total, 1);
        assert!(reopened.get_document("doc-1").is_none());
        assert_eq!(
            reopened.get_document("doc-2").unwrap().source["service"],
            "search"
        );
    }

    #[tokio::test]
    async fn terms_query_matches_multiple_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, service) in [("doc-1", "billing"), ("doc-2", "search"), ("doc-3", "auth")] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source: serde_json::json!({"service": service}),
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let response = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "service".to_string(),
                values: vec![serde_json::json!("billing"), serde_json::json!("auth")],
            })),
            ..Default::default()
        });

        assert_eq!(response.hits.total, 2);
        assert_eq!(response.hits.hits[0].id, "doc-1");
        assert_eq!(response.hits.hits[1].id, "doc-3");
    }

    #[tokio::test]
    async fn terms_query_handles_empty_duplicate_numeric_and_boolean_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            (
                "doc-1",
                serde_json::json!({"service": "billing", "latency": 30, "active": true}),
            ),
            (
                "doc-2",
                serde_json::json!({"service": "search", "latency": 10, "active": false}),
            ),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let empty = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "service".to_string(),
                values: vec![],
            })),
            ..Default::default()
        });
        assert_eq!(empty.hits.total, 0);

        let duplicates = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "service".to_string(),
                values: vec![serde_json::json!("billing"), serde_json::json!("billing")],
            })),
            ..Default::default()
        });
        assert_eq!(duplicates.hits.total, 1);
        assert_eq!(duplicates.hits.hits[0].id, "doc-1");

        let numeric = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "latency".to_string(),
                values: vec![serde_json::json!(10), serde_json::json!(99)],
            })),
            ..Default::default()
        });
        assert_eq!(numeric.hits.total, 1);
        assert_eq!(numeric.hits.hits[0].id, "doc-2");

        let booleans = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "active".to_string(),
                values: vec![serde_json::json!(true)],
            })),
            ..Default::default()
        });
        assert_eq!(booleans.hits.total, 1);
        assert_eq!(booleans.hits.hits[0].id, "doc-1");
    }

    #[tokio::test]
    async fn search_supports_pagination_and_sorting() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, latency, service, active, timestamp) in [
            ("doc-1", 30, "billing", true, "2026-03-14T10:00:00Z"),
            ("doc-2", 10, "search", false, "2026-03-14T11:00:00Z"),
            ("doc-3", 20, "auth", true, "2026-03-14T12:00:00Z"),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source: serde_json::json!({
                        "latency": latency,
                        "service": service,
                        "active": active,
                        "timestamp": timestamp,
                    }),
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let paged = handle.search(&SearchRequest {
            query: None,
            from: Some(1),
            size: Some(1),
            sort: Some(SortSpec {
                field: "latency".to_string(),
                order: SortOrder::Asc,
            }),
            ..Default::default()
        });
        assert_eq!(paged.hits.total, 3);
        assert_eq!(paged.hits.hits.len(), 1);
        assert_eq!(paged.hits.hits[0].id, "doc-3");

        let strings = handle.search(&SearchRequest {
            query: None,
            size: Some(2),
            sort: Some(SortSpec {
                field: "service".to_string(),
                order: SortOrder::Desc,
            }),
            ..Default::default()
        });
        assert_eq!(strings.hits.hits[0].id, "doc-2");
        assert_eq!(strings.hits.hits[1].id, "doc-1");

        let booleans = handle.search(&SearchRequest {
            query: None,
            sort: Some(SortSpec {
                field: "active".to_string(),
                order: SortOrder::Asc,
            }),
            ..Default::default()
        });
        assert_eq!(booleans.hits.hits[0].id, "doc-2");

        let timestamps = handle.search(&SearchRequest {
            query: None,
            sort: Some(SortSpec {
                field: "timestamp".to_string(),
                order: SortOrder::Desc,
            }),
            ..Default::default()
        });
        assert_eq!(timestamps.hits.hits[0].id, "doc-3");
    }

    #[allow(clippy::float_cmp)]
    #[tokio::test]
    async fn terms_and_stats_aggregations_respect_query_and_ignore_pagination() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            (
                "doc-1",
                serde_json::json!({"service": "billing", "level": "info", "latency": 10}),
            ),
            (
                "doc-2",
                serde_json::json!({"service": "billing", "level": "error", "latency": 20}),
            ),
            (
                "doc-3",
                serde_json::json!({"service": "search", "level": "info", "latency": 30}),
            ),
            (
                "doc-4",
                serde_json::json!({"service": "search", "level": "info"}),
            ),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let response = handle.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "level".to_string(),
                value: serde_json::json!("info"),
                fuzziness: None,
            })),
            from: Some(0),
            size: Some(1),
            aggs: Some(BTreeMap::from([
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
        });

        assert_eq!(response.hits.total, 3);
        assert_eq!(response.hits.hits.len(), 1);

        match response.aggregations.get("services") {
            Some(AggregationResult::Terms(terms)) => {
                assert_eq!(terms.buckets.len(), 2);
                assert_eq!(terms.buckets[0].key, serde_json::json!("search"));
                assert_eq!(terms.buckets[0].doc_count, 2);
                assert_eq!(terms.buckets[1].key, serde_json::json!("billing"));
                assert_eq!(terms.buckets[1].doc_count, 1);
            }
            other => panic!("unexpected aggregation result: {other:?}"),
        }

        match response.aggregations.get("latency_stats") {
            Some(AggregationResult::Stats(stats)) => {
                assert_eq!(stats.count, 2);
                assert_eq!(stats.min, Some(10.0));
                assert_eq!(stats.max, Some(30.0));
                assert_eq!(stats.avg, Some(20.0));
                assert_eq!(stats.sum, 40.0);
            }
            other => panic!("unexpected aggregation result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn terms_aggregation_rejects_object_field() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "test",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("test").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"metadata": {"key": "value"}}),
            })
            .await
            .expect("index doc");

        handle
            .validate_search_request(&SearchRequest {
                query: None,
                aggs: Some(std::collections::BTreeMap::from([(
                    "by_field".to_string(),
                    AggregationRequest::Terms(TermsAggregationRequest {
                        field: "metadata".to_string(),
                    }),
                )])),
                ..Default::default()
            })
            .expect_err("terms on Object field should fail");
    }

    #[tokio::test]
    async fn stats_aggregation_rejects_object_field() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "test",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("test").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"metadata": {"key": "value"}}),
            })
            .await
            .expect("index doc");

        handle
            .validate_search_request(&SearchRequest {
                query: None,
                aggs: Some(std::collections::BTreeMap::from([(
                    "field_stats".to_string(),
                    AggregationRequest::Stats(StatsAggregationRequest {
                        field: "metadata".to_string(),
                    }),
                )])),
                ..Default::default()
            })
            .expect_err("stats on Object field should fail");
    }

    #[tokio::test]
    async fn date_histogram_aggregation_rejects_non_timestamp_field() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "test",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("test").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"count": 42}),
            })
            .await
            .expect("index doc");

        handle
            .validate_search_request(&SearchRequest {
                query: None,
                aggs: Some(std::collections::BTreeMap::from([(
                    "over_time".to_string(),
                    AggregationRequest::DateHistogram(DateHistogramAggregationRequest {
                        field: "count".to_string(),
                        interval: DateHistogramInterval::Day,
                    }),
                )])),
                ..Default::default()
            })
            .expect_err("date_histogram on non-timestamp field should fail");
    }

    #[tokio::test]
    async fn date_histogram_buckets_by_hour_and_ignores_missing_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            (
                "doc-1",
                serde_json::json!({"timestamp": "2026-03-14T10:05:00Z", "service": "billing"}),
            ),
            (
                "doc-2",
                serde_json::json!({"timestamp": "2026-03-14T10:45:00Z", "service": "billing"}),
            ),
            (
                "doc-3",
                serde_json::json!({"timestamp": "2026-03-14T11:15:00Z", "service": "billing"}),
            ),
            ("doc-4", serde_json::json!({"service": "billing"})),
            ("doc-5", serde_json::json!({"service": "billing"})),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let response = handle.search(&SearchRequest {
            aggs: Some(BTreeMap::from([(
                "events_over_time".to_string(),
                AggregationRequest::DateHistogram(DateHistogramAggregationRequest {
                    field: "timestamp".to_string(),
                    interval: DateHistogramInterval::Hour,
                }),
            )])),
            ..Default::default()
        });

        match response.aggregations.get("events_over_time") {
            Some(AggregationResult::DateHistogram(histogram)) => {
                assert_eq!(histogram.buckets.len(), 2);
                assert_eq!(histogram.buckets[0].key, "2026-03-14T10:00:00Z");
                assert_eq!(histogram.buckets[0].doc_count, 2);
                assert_eq!(histogram.buckets[1].key, "2026-03-14T11:00:00Z");
                assert_eq!(histogram.buckets[1].doc_count, 1);
            }
            other => panic!("unexpected aggregation result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn pagination_edges_and_missing_sort_field_are_stable() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            ("doc-1", serde_json::json!({"latency": 30})),
            ("doc-2", serde_json::json!({"latency": 10})),
            ("doc-3", serde_json::json!({"message": "missing-latency"})),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let zero_size = handle.search(&SearchRequest {
            size: Some(0),
            ..Default::default()
        });
        assert_eq!(zero_size.hits.total, 3);
        assert!(zero_size.hits.hits.is_empty());

        let exact_end = handle.search(&SearchRequest {
            from: Some(3),
            size: Some(2),
            ..Default::default()
        });
        assert_eq!(exact_end.hits.total, 3);
        assert!(exact_end.hits.hits.is_empty());

        let missing_last = handle.search(&SearchRequest {
            sort: Some(SortSpec {
                field: "latency".to_string(),
                order: SortOrder::Asc,
            }),
            ..Default::default()
        });
        assert_eq!(missing_last.hits.hits[0].id, "doc-2");
        assert_eq!(missing_last.hits.hits[1].id, "doc-1");
        assert_eq!(missing_last.hits.hits[2].id, "doc-3");
    }

    #[tokio::test]
    async fn range_query_rejects_string_and_boolean_fields() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "active": true}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        let string_range = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "service".to_string(),
                gte: Some(serde_json::json!("a")),
                gt: None,
                lte: None,
                lt: None,
            })),
            ..Default::default()
        });
        assert_eq!(string_range.hits.total, 0);

        let bool_range = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "active".to_string(),
                gte: Some(serde_json::json!(true)),
                gt: None,
                lte: None,
                lt: None,
            })),
            ..Default::default()
        });
        assert_eq!(bool_range.hits.total, 0);
    }

    #[tokio::test]
    async fn repeated_flushes_without_new_writes_are_stable() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
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

        let first = handle.flush().await.expect("first flush");
        let second = handle.flush().await.expect("second flush");

        assert_eq!(first.flushed_documents, 1);
        assert_eq!(second.flushed_documents, 1);

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(reopened.search(&SearchRequest::default()).hits.total, 1);
    }

    #[tokio::test]
    async fn merge_plan_is_empty_for_small_indexes() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
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

        let _snapshot = SegmentSnapshot {
            last_sequence_number: handle.last_sequence_number,
            documents: handle.searchable_documents.values().cloned().collect(),
        };

        assert!(handle.plan_merge().is_none());
    }

    #[tokio::test]
    async fn merge_plan_appears_once_threshold_is_met() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");

        for i in 0..MERGE_TRIGGER_DOCUMENT_COUNT {
            handle
                .index_document(IndexDocument {
                    id: format!("doc-{i}"),
                    source: serde_json::json!({"message": format!("doc {i}")}),
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");
        handle.flush().await.expect("flush");

        let plan = handle.plan_merge().expect("merge plan");
        assert_eq!(plan.segments.len(), 1);
        assert_eq!(
            plan.segments[0].document_count,
            MERGE_TRIGGER_DOCUMENT_COUNT as u64
        );
        assert_eq!(
            plan.segments[0].last_sequence_number,
            MERGE_TRIGGER_DOCUMENT_COUNT as u64
        );
    }

    #[tokio::test]
    async fn merge_removes_duplicate_overwrites() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "v1"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "v2"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        let merged = handle.merge().await.expect("merge");
        assert_eq!(merged.merged_documents, 1);

        let result = handle.search(&SearchRequest::default());
        assert_eq!(result.hits.total, 1);
        assert_eq!(result.hits.hits[0].source["message"], "v2");
    }

    #[tokio::test]
    async fn merge_removes_deleted_documents() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
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
        handle.refresh().await.expect("refresh");

        let merged = handle.merge().await.expect("merge");
        assert_eq!(merged.merged_documents, 0);

        let result = handle.search(&SearchRequest::default());
        assert_eq!(result.hits.total, 0);
    }

    #[tokio::test]
    async fn reopen_after_merge_is_stable() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
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
        handle.merge().await.expect("merge");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(reopened.search(&SearchRequest::default()).hits.total, 1);
    }

    #[tokio::test]
    async fn merge_writes_persisted_snapshot_without_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let segments_dir = temp_dir
            .path()
            .join("indexes")
            .join("logs")
            .join("segments");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello"}),
            })
            .await
            .expect("index doc");

        handle.merge().await.expect("merge");

        let persisted = read_segment_snapshot(&segments_dir)
            .await
            .expect("read snapshot")
            .expect("snapshot exists");
        assert_eq!(persisted.documents.len(), 1);
        assert_eq!(persisted.documents[0].id, "doc-1");
    }

    #[tokio::test]
    async fn merge_compacts_overwrites_without_prior_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let segments_dir = temp_dir
            .path()
            .join("indexes")
            .join("logs")
            .join("segments");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "v1"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "v2"}),
            })
            .await
            .expect("index doc");

        handle.merge().await.expect("merge");

        let persisted = read_segment_snapshot(&segments_dir)
            .await
            .expect("read snapshot")
            .expect("snapshot exists");
        assert_eq!(persisted.documents.len(), 1);
        assert_eq!(persisted.documents[0].source["message"], "v2");
    }

    #[tokio::test]
    async fn merge_applies_pending_delete_without_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let segments_dir = temp_dir
            .path()
            .join("indexes")
            .join("logs")
            .join("segments");

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

        handle.merge().await.expect("merge");

        let persisted = read_segment_snapshot(&segments_dir)
            .await
            .expect("read snapshot")
            .expect("snapshot exists");
        assert_eq!(persisted.documents.len(), 0);
    }

    #[tokio::test]
    async fn prefix_queries_match_string_prefixes() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "auth-service", "message": "hello"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "auth-worker", "message": "world"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-3".to_string(),
                source: serde_json::json!({"service": "billing-api", "message": "hi"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        // Match prefix "auth-"
        let auth_prefix = handle.search(&SearchRequest {
            query: Some(SearchQuery::Prefix(PrefixQuery {
                field: "service".to_string(),
                value: "auth-".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(auth_prefix.hits.total, 2);

        // Match prefix "auth-worker"
        let exact_match = handle.search(&SearchRequest {
            query: Some(SearchQuery::Prefix(PrefixQuery {
                field: "service".to_string(),
                value: "auth-worker".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(exact_match.hits.total, 1);

        // No match for "xyz" prefix
        let no_match = handle.search(&SearchRequest {
            query: Some(SearchQuery::Prefix(PrefixQuery {
                field: "service".to_string(),
                value: "xyz".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(no_match.hits.total, 0);

        // Prefix matching is case-sensitive
        let case_sensitive = handle.search(&SearchRequest {
            query: Some(SearchQuery::Prefix(PrefixQuery {
                field: "service".to_string(),
                value: "Auth-".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(case_sensitive.hits.total, 0);

        // Prefix on non-existent field
        let missing_field = handle.search(&SearchRequest {
            query: Some(SearchQuery::Prefix(PrefixQuery {
                field: "nonexistent".to_string(),
                value: "test".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(missing_field.hits.total, 0);

        // Empty prefix matches any string
        let empty_prefix = handle.search(&SearchRequest {
            query: Some(SearchQuery::Prefix(PrefixQuery {
                field: "service".to_string(),
                value: String::new(),
            })),
            ..Default::default()
        });
        assert_eq!(empty_prefix.hits.total, 3);
    }

    #[tokio::test]
    async fn wildcard_queries_match_string_patterns() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "auth-service", "message": "hello"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "auth-worker", "message": "world"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-3".to_string(),
                source: serde_json::json!({"service": "billing-api", "message": "hi"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-4".to_string(),
                source: serde_json::json!({"service": "search-service", "message": "test"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        // Wildcard "auth-*" matches doc-1 and doc-2
        let auth_star = handle.search(&SearchRequest {
            query: Some(SearchQuery::Wildcard(WildcardQuery {
                field: "service".to_string(),
                value: "auth-*".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(auth_star.hits.total, 2);

        // Wildcard "*service" matches doc-1 and doc-4
        let star_service = handle.search(&SearchRequest {
            query: Some(SearchQuery::Wildcard(WildcardQuery {
                field: "service".to_string(),
                value: "*service".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(star_service.hits.total, 2);

        // Wildcard "*-api" matches doc-3 only
        let dash_api = handle.search(&SearchRequest {
            query: Some(SearchQuery::Wildcard(WildcardQuery {
                field: "service".to_string(),
                value: "*-api".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(dash_api.hits.total, 1);

        // Wildcard "*-api" with * matches doc-3 only (billing-api)
        let dash_api_star = handle.search(&SearchRequest {
            query: Some(SearchQuery::Wildcard(WildcardQuery {
                field: "service".to_string(),
                value: "*-api".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(dash_api_star.hits.total, 1);

        // Wildcard "*-service" matches doc-1 and doc-4 (ends with -service)
        let end_service = handle.search(&SearchRequest {
            query: Some(SearchQuery::Wildcard(WildcardQuery {
                field: "service".to_string(),
                value: "*-service".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(end_service.hits.total, 2);

        // No match for "xyz*"
        let no_match = handle.search(&SearchRequest {
            query: Some(SearchQuery::Wildcard(WildcardQuery {
                field: "service".to_string(),
                value: "xyz*".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(no_match.hits.total, 0);

        // Case-sensitive: "Auth-*" doesn't match "auth-service"
        let case_sensitive = handle.search(&SearchRequest {
            query: Some(SearchQuery::Wildcard(WildcardQuery {
                field: "service".to_string(),
                value: "Auth-*".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(case_sensitive.hits.total, 0);
    }

    #[tokio::test]
    async fn match_queries_find_tokens_in_text_fields() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello world"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "hello there world"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-3".to_string(),
                source: serde_json::json!({"message": "foo bar"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        // Match "hello" finds doc-1 and doc-2
        let hello = handle.search(&SearchRequest {
            query: Some(SearchQuery::Match(MatchQuery {
                field: "message".to_string(),
                value: "hello".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(hello.hits.total, 2);
        // With BM25 scoring, "hello" has df=2 (doc-1 and doc-2), n=3.
        // IDF = max(0, ln((3-2+0.5)/(2+0.5))) ≈ 0, so score ≈ 0 unless TF is high.
        // Both docs match "hello" with equal TF, so tie-breaker is alphabetical id.
        assert_eq!(hello.hits.hits[0].id, "doc-1");
        assert!(hello.hits.hits[0].score.unwrap_or(0.0) >= 0.0);

        // Match "hello world" - both docs match 2/2 tokens = 1.0 (tie goes to lower doc id)
        let both = handle.search(&SearchRequest {
            query: Some(SearchQuery::Match(MatchQuery {
                field: "message".to_string(),
                value: "hello world".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(both.hits.total, 2);
        // Both docs match 2 tokens. With BM25, doc-1 ("hello world" → 2 tokens, len=2)
        // and doc-2 ("hello there world" → 3 tokens, len=3) get different scores even with
        // identical term frequencies, due to field length normalization.
        // Tie-breaker is alphabetical: doc-1 < doc-2.
        assert_eq!(both.hits.hits[0].id, "doc-1");
        assert!(both.hits.hits[0].score.unwrap_or(0.0) >= 0.0);
        assert_eq!(both.hits.hits[1].id, "doc-2");
        assert!(both.hits.hits[1].score.unwrap_or(0.0) >= 0.0);

        // Match "xyz" finds nothing
        let no_match = handle.search(&SearchRequest {
            query: Some(SearchQuery::Match(MatchQuery {
                field: "message".to_string(),
                value: "xyz".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(no_match.hits.total, 0);

        // Match is case-insensitive
        let case_insensitive = handle.search(&SearchRequest {
            query: Some(SearchQuery::Match(MatchQuery {
                field: "message".to_string(),
                value: "HELLO".to_string(),
            })),
            ..Default::default()
        });
        assert_eq!(case_insensitive.hits.total, 2);
    }

    #[tokio::test]
    async fn index_reopen_after_wal_corruption_is_graceful() {
        // Create index, write docs, flush (snapshot), write WAL entries,
        // corrupt the active WAL, re-open index — should handle gracefully
        // with data from snapshot intact.
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "persisted"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");
        handle.flush().await.expect("flush");

        // Write another doc to create an active WAL entry.
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "tail"}),
            })
            .await
            .expect("index doc tail");

        let wal_dir = temp_dir.path().join("indexes").join("logs").join("wal");
        // Corrupt the active WAL log file by flipping a byte in the header.
        if let Ok(log_file) = fs::read_dir(&wal_dir).await {
            let mut entries = log_file;
            while let Some(entry) = entries.next_entry().await.expect("read dir") {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "log") {
                    let mut bytes = fs::read(&path).await.expect("read wal");
                    if bytes.len() > 26 {
                        bytes[26] ^= 0xFF; // corrupt header byte
                        fs::write(&path, bytes).await.expect("rewrite wal");
                    }
                    break;
                }
            }
        }

        drop(handle);

        // Re-opening should return an error (not panic) when WAL is corrupted.
        // The error is propagated from wal.replay(), which detects checksum mismatch.
        let error = catalog
            .open_index("logs")
            .await
            .expect_err("open_index should fail when WAL is corrupted");
        assert!(matches!(
            error,
            CloudSearchError::WalChecksumMismatch | CloudSearchError::InvalidWalRecord(_)
        ));
    }

    #[test]
    fn tokenize_lowercase_converts_correctly() {
        let result = tokenize("Hello World");
        assert_eq!(result, vec!["hello", "world"]);
    }

    #[test]
    fn tokenize_preserves_single_tokens() {
        let result = tokenize("test");
        assert_eq!(result, vec!["test"]);
    }

    #[test]
    fn tokenize_multiple_whitespace_collapsed() {
        let result = tokenize("a   b");
        assert_eq!(result, vec!["a", "b"]);
    }

    #[test]
    fn tokenize_empty_string() {
        let result = tokenize("");
        assert!(result.is_empty());
    }

    #[test]
    fn levenshtein_distance_empty() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("", "abc"), 3);
        assert_eq!(levenshtein_distance("abc", ""), 3);
    }

    #[test]
    fn levenshtein_distance_identical() {
        assert_eq!(levenshtein_distance("hello", "hello"), 0);
        assert_eq!(levenshtein_distance("", ""), 0);
    }

    #[test]
    fn levenshtein_distance_one_edit() {
        assert_eq!(levenshtein_distance("hello", "hallo"), 1); // substitution
        assert_eq!(levenshtein_distance("hello", "hell"), 1); // deletion
        assert_eq!(levenshtein_distance("hello", "helloo"), 1); // insertion
    }

    #[test]
    fn levenshtein_distance_case_sensitive() {
        assert_eq!(levenshtein_distance("HELLO", "hello"), 5); // all chars different
        assert_eq!(levenshtein_distance("Hello", "hello"), 1); // case only
    }

    #[test]
    fn levenshtein_distance_complex() {
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
    }

    #[test]
    fn fuzzy_term_match_exact_no_fuzziness() {
        use cloudsearch_common::TermQuery;
        let doc = IndexDocument {
            id: "1".to_string(),
            source: serde_json::json!({"name": "admin"}),
        };
        // No fuzziness - exact match required
        let term = TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("admin"),
            fuzziness: None,
        };
        let result = fuzzy_term_match(&doc, &term);
        assert_eq!(
            result,
            Some(true),
            "exact match should return Some(true), got {result:?}"
        );

        // Non-matching value returns None (no match, same as original behavior)
        let term_miss = TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("xyz"),
            fuzziness: None,
        };
        let result_miss = fuzzy_term_match(&doc, &term_miss);
        assert_eq!(
            result_miss, None,
            "non-matching value should return None, got {result_miss:?}"
        );

        // Missing field returns None
        let term_missing = TermQuery {
            field: "nonexistent".to_string(),
            value: serde_json::json!("admin"),
            fuzziness: None,
        };
        let result_missing = fuzzy_term_match(&doc, &term_missing);
        assert_eq!(
            result_missing, None,
            "missing field should return None, got {result_missing:?}"
        );
    }

    #[test]
    fn fuzzy_term_match_exact_with_fuzziness() {
        use cloudsearch_common::{Fuzziness, TermQuery};
        let doc = IndexDocument {
            id: "1".to_string(),
            source: serde_json::json!({"name": "admin"}),
        };
        // Fuzziness::Exact(0) is still exact match
        let term = TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("admin"),
            fuzziness: Some(Fuzziness::Exact(0)),
        };
        let result = fuzzy_term_match(&doc, &term);
        assert_eq!(
            result,
            Some(true),
            "exact match with Exact(0) should return Some(true), got {result:?}"
        );

        // Different value with threshold 0 - no match
        let term_miss = TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("xyz"),
            fuzziness: Some(Fuzziness::Exact(0)),
        };
        let result_miss = fuzzy_term_match(&doc, &term_miss);
        assert_eq!(
            result_miss, None,
            "threshold=0 fuzzy with string mismatch returns None, got {result_miss:?}"
        );
    }

    #[test]
    fn fuzzy_term_match_auto_mode() {
        use cloudsearch_common::{Fuzziness, TermQuery};
        let doc = IndexDocument {
            id: "1".to_string(),
            source: serde_json::json!({"name": "admin"}),
        };
        // "admin" (6 chars) → Auto threshold = 2, exact match passes
        let term = TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("admin"),
            fuzziness: Some(Fuzziness::Auto),
        };
        assert_eq!(fuzzy_term_match(&doc, &term), Some(true));

        // Edit distance 1 (admim vs admin) → should match with threshold 2
        let term_fuzzy = TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("admim"),
            fuzziness: Some(Fuzziness::Auto),
        };
        assert_eq!(fuzzy_term_match(&doc, &term_fuzzy), Some(true));

        // Edit distance 5 > threshold 2 → returns Some(false)
        let term_no_match = TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("xyz"),
            fuzziness: Some(Fuzziness::Auto),
        };
        let result_no_match = fuzzy_term_match(&doc, &term_no_match);
        assert_eq!(
            result_no_match,
            Some(false),
            "edit distance 5 > threshold 2 should return Some(false), got {result_no_match:?}"
        );
    }

    #[test]
    fn fuzzy_term_match_numeric_stored_value() {
        use cloudsearch_common::{Fuzziness, TermQuery};
        let doc = IndexDocument {
            id: "1".to_string(),
            source: serde_json::json!({"count": 42}),
        };
        // Numeric stored value with fuzziness - should return None (fuzzy only works with strings)
        let term = TermQuery {
            field: "count".to_string(),
            value: serde_json::json!(42),
            fuzziness: Some(Fuzziness::Auto),
        };
        assert_eq!(fuzzy_term_match(&doc, &term), None); // fuzzy requires string
    }
}
