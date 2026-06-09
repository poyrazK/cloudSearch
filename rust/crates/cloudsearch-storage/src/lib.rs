use chrono::Utc;
use cloudsearch_common::{CloudSearchError, IndexDocument, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

pub mod inverted_index;
pub mod positions_writer;
pub mod suggest_index;
pub mod suggest_writer;

const WAL_VERSION: u8 = 1;
const HEADER_LEN: usize = 26;
const DEFAULT_GENERATION: u64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalRecord {
    IndexDocument {
        document: IndexDocument,
    },
    DeleteDocument {
        document_id: String,
    },
    MappingUpdate {
        mapping_version: u64,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub sequence_number: u64,
    pub recorded_at_unix_ms: i64,
    pub record: WalRecord,
}

#[derive(Debug, Clone)]
pub struct WalManager {
    wal_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentSnapshot {
    pub last_sequence_number: u64,
    pub documents: Vec<IndexDocument>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentManifest {
    pub last_sequence_number: u64,
    pub document_count: u64,
}

impl From<&SegmentSnapshot> for SegmentManifest {
    fn from(snapshot: &SegmentSnapshot) -> Self {
        Self {
            last_sequence_number: snapshot.last_sequence_number,
            document_count: snapshot.documents.len() as u64,
        }
    }
}

/// Metadata for a single immutable segment, referenced by the index manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentMeta {
    pub segment_number: u64,
    pub last_sequence_number: u64,
    pub document_count: u64,
    pub checksum: u32,
}

/// The index manifest — tracks all active immutable segments.
/// Replaces the single mutable `current.json` pattern.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexManifest {
    pub version: u64,
    pub last_updated: chrono::DateTime<chrono::Utc>,
    pub segments: Vec<SegmentMeta>,
}

impl IndexManifest {
    #[must_use]
    pub fn new() -> Self {
        Self {
            version: 1,
            last_updated: chrono::Utc::now(),
            segments: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_segment(mut self, meta: SegmentMeta) -> Self {
        self.segments.push(meta);
        self
    }

    #[must_use]
    pub fn next_segment_number(&self) -> u64 {
        self.segments.last().map_or(1, |s| s.segment_number + 1)
    }
}

impl Default for IndexManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata for a named snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_sequence_number: u64,
    pub document_count: usize,
    pub checksum: u32,
}

/// `DocValueType` for columnar sidecar storage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DocValueType {
    Keyword,
    Integer,
    Long,
    Double,
    Boolean,
    Timestamp,
}

/// Header stored at the start of each doc values sidecar file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocValuesHeader {
    pub field: String,
    pub value_type: DocValueType,
    pub doc_count: u64,
}

/// A single field's doc values, stored in packed binary format.
#[derive(Debug, Clone)]
pub struct DocValuesField {
    pub field: String,
    pub value_type: DocValueType,
    pub doc_count: u64,
    /// Packed binary data. Format depends on `value_type`:
    /// - Keyword: (u32 offset, u32 len) pairs, then string pool
    /// - Integer/Long/Timestamp: packed i64 array
    /// - Double: packed f64 array
    /// - Boolean: packed u8 bit array
    pub data: Vec<u8>,
}

impl WalManager {
    /// Opens or creates a WAL manager for the given directory.
    ///
    /// # Errors
    /// Returns an error if directory creation fails or if no current generation exists.
    pub async fn open(wal_dir: impl Into<PathBuf>) -> Result<Self> {
        let wal_dir = wal_dir.into();
        fs::create_dir_all(&wal_dir).await?;

        let manager = Self { wal_dir };
        manager.ensure_current_generation().await?;
        Ok(manager)
    }

    /// Appends a WAL record to the current generation.
    ///
    /// # Errors
    /// Returns an error if file operations or serialization fails.
    ///
    /// # Panics
    /// Panics if the payload exceeds 4 GB.
    pub async fn append(&self, sequence_number: u64, record: WalRecord) -> Result<()> {
        let generation = self.current_generation().await?;
        let path = self.generation_path(generation);
        let payload = serde_json::to_vec(&record)?;
        let checksum = crc32c::crc32c(&payload);
        let timestamp = Utc::now().timestamp_millis();

        let mut header = Vec::with_capacity(HEADER_LEN);
        header.push(WAL_VERSION);
        header.push(record_type(&record));
        header.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        header.extend_from_slice(&sequence_number.to_le_bytes());
        header.extend_from_slice(&timestamp.to_le_bytes());
        header.extend_from_slice(&checksum.to_le_bytes());

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        file.write_all(&header).await?;
        file.write_all(&payload).await?;
        file.flush().await?;
        file.sync_all().await?;

        Ok(())
    }

    /// Replays all WAL entries from the beginning.
    ///
    /// # Errors
    /// Returns an error if file reading or parsing fails.
    pub async fn replay(&self) -> Result<Vec<WalEntry>> {
        self.replay_from(0).await
    }

    /// Replays WAL entries starting from the given sequence number (exclusive).
    ///
    /// # Errors
    /// Returns an error if file reading or parsing fails.
    pub async fn replay_from(&self, sequence_number_exclusive: u64) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let current_generation = self.current_generation().await?;

        for generation in self.list_generations().await? {
            let path = self.generation_path(generation);
            let mut file = OpenOptions::new().read(true).open(path).await?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await?;

            let mut offset = 0usize;

            while offset + HEADER_LEN <= bytes.len() {
                let header = &bytes[offset..offset + HEADER_LEN];
                let version = header[0];
                if version != WAL_VERSION {
                    return Err(CloudSearchError::InvalidWalRecord(format!(
                        "unsupported WAL version {version}"
                    )));
                }

                let record_type = header[1];
                // SAFETY: header is always HEADER_LEN (26) bytes due to the loop guard above
                let payload_len = {
                    let mut buf = [0u8; 4];
                    buf.copy_from_slice(&header[2..6]);
                    u32::from_le_bytes(buf)
                } as usize;
                let sequence_number = {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&header[6..14]);
                    u64::from_le_bytes(buf)
                };
                let recorded_at_unix_ms = {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&header[14..22]);
                    i64::from_le_bytes(buf)
                };
                let checksum = {
                    let mut buf = [0u8; 4];
                    buf.copy_from_slice(&header[22..26]);
                    u32::from_le_bytes(buf)
                };

                let payload_start = offset + HEADER_LEN;
                let payload_end = payload_start + payload_len;

                if payload_end > bytes.len() {
                    if generation == current_generation {
                        break;
                    }

                    return Err(CloudSearchError::InvalidWalRecord(format!(
                        "partial record in inactive WAL generation {generation:06}"
                    )));
                }

                let payload = &bytes[payload_start..payload_end];
                if crc32c::crc32c(payload) != checksum {
                    return Err(CloudSearchError::WalChecksumMismatch);
                }

                let record = decode_record(record_type, payload)?;
                if sequence_number > sequence_number_exclusive {
                    entries.push(WalEntry {
                        sequence_number,
                        recorded_at_unix_ms,
                        record,
                    });
                }

                offset = payload_end;
            }
        }

        Ok(entries)
    }

    /// Returns the next available sequence number.
    ///
    /// # Errors
    /// Returns an error if replay fails.
    pub async fn next_sequence_number(&self) -> Result<u64> {
        let last = self
            .replay()
            .await?
            .last()
            .map_or(0, |entry| entry.sequence_number);
        Ok(last + 1)
    }

    #[must_use]
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Lists all WAL generation numbers.
    ///
    /// # Errors
    /// Returns an error if directory reading fails.
    pub async fn list_generations(&self) -> Result<Vec<u64>> {
        let mut generations = Vec::new();
        let mut entries = fs::read_dir(&self.wal_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
                continue;
            }

            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };

            let generation = stem.parse::<u64>().map_err(|_| {
                CloudSearchError::InvalidWalRecord(format!(
                    "invalid WAL generation file '{}'",
                    path.display()
                ))
            })?;
            generations.push(generation);
        }

        generations.sort_unstable();
        Ok(generations)
    }

    /// Creates a new WAL generation.
    ///
    /// # Errors
    /// Returns an error if file writing fails.
    pub async fn rollover(&self) -> Result<u64> {
        let next_generation = self.current_generation().await? + 1;
        fs::write(self.current_path(), format!("{next_generation:06}\n")).await?;
        Ok(next_generation)
    }

    /// Trims WAL entries up to and including the given sequence number.
    ///
    /// # Errors
    /// Returns an error if file operations fail.
    pub async fn trim_through(&self, sequence_number_inclusive: u64) -> Result<usize> {
        let current_generation = self.current_generation().await?;
        let mut trimmed = 0;

        for generation in self.list_generations().await? {
            if generation == current_generation {
                continue;
            }

            let max_sequence = self.max_sequence_in_generation(generation).await?;
            if let Some(max_sequence) = max_sequence
                && max_sequence <= sequence_number_inclusive
            {
                fs::remove_file(self.generation_path(generation)).await?;
                trimmed += 1;
            }
        }

        Ok(trimmed)
    }

    async fn ensure_current_generation(&self) -> Result<()> {
        let current_path = self.current_path();
        if !fs::try_exists(&current_path).await? {
            fs::write(current_path, format!("{DEFAULT_GENERATION:06}\n")).await?;
        }

        Ok(())
    }

    async fn current_generation(&self) -> Result<u64> {
        let content = fs::read_to_string(self.current_path()).await?;
        content.trim().parse::<u64>().map_err(|_| {
            CloudSearchError::InvalidWalRecord("invalid CURRENT generation".to_string())
        })
    }

    fn current_path(&self) -> PathBuf {
        self.wal_dir.join("CURRENT")
    }

    fn generation_path(&self, generation: u64) -> PathBuf {
        self.wal_dir.join(format!("{generation:06}.log"))
    }

    async fn max_sequence_in_generation(&self, generation: u64) -> Result<Option<u64>> {
        let path = self.generation_path(generation);
        if !fs::try_exists(&path).await? {
            return Ok(None);
        }

        let mut file = OpenOptions::new().read(true).open(path).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;

        let mut offset = 0usize;
        let mut max_sequence = None;

        while offset + HEADER_LEN <= bytes.len() {
            let header = &bytes[offset..offset + HEADER_LEN];
            // SAFETY: header is always HEADER_LEN (26) bytes due to the loop guard above
            let payload_len = {
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&header[2..6]);
                u32::from_le_bytes(buf)
            } as usize;
            let sequence_number = {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&header[6..14]);
                u64::from_le_bytes(buf)
            };
            let payload_end = offset + HEADER_LEN + payload_len;

            if payload_end > bytes.len() {
                break;
            }

            max_sequence = Some(sequence_number);
            offset = payload_end;
        }

        Ok(max_sequence)
    }
}

/// Reads the latest segment snapshot from the segments directory.
///
/// # Errors
/// Returns an error if file operations or deserialization fails.
pub async fn read_segment_snapshot(
    segments_dir: impl AsRef<Path>,
) -> Result<Option<SegmentSnapshot>> {
    let path = segment_snapshot_path(segments_dir.as_ref());

    if !fs::try_exists(&path).await? {
        return Ok(None);
    }

    let bytes = fs::read(path).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

/// Reads a segment snapshot from a specific segment file path (absolute path).
///
/// # Errors
/// Returns an error if file operations or deserialization fails.
pub async fn read_segment_file(path: impl AsRef<Path>) -> Result<Option<SegmentSnapshot>> {
    if !fs::try_exists(path.as_ref()).await? {
        return Ok(None);
    }
    let bytes = fs::read(path.as_ref()).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn doc_values_dir(segments_dir: &Path) -> PathBuf {
    segments_dir.join("doc_values")
}

fn doc_values_path(segments_dir: &Path, field: &str) -> PathBuf {
    doc_values_dir(segments_dir).join(format!("{field}.bin"))
}

/// Writes all doc values fields to the sidecar directory.
///
/// # Errors
/// Returns an error if file operations or serialization fails.
///
/// # Panics
/// Panics if any header exceeds 4 GB.
pub async fn write_doc_values(
    segments_dir: impl AsRef<Path>,
    fields: &BTreeMap<String, DocValuesField>,
) -> Result<()> {
    let segments_dir = segments_dir.as_ref();
    let dv_dir = doc_values_dir(segments_dir);
    fs::create_dir_all(&dv_dir).await?;

    for (field, f) in fields {
        let path = doc_values_path(segments_dir, field);
        // Format: header (JSON) + binary data
        let header = DocValuesHeader {
            field: f.field.clone(),
            value_type: f.value_type.clone(),
            doc_count: f.doc_count,
        };
        let header_bytes = serde_json::to_vec(&header)?;
        let len_bytes = u32::try_from(header_bytes.len()).unwrap().to_le_bytes();

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;
        file.write_all(&len_bytes).await?;
        file.write_all(&header_bytes).await?;
        file.write_all(&f.data).await?;
        file.flush().await?;
        file.sync_all().await?;
    }

    // Sync parent directory
    let dir_file = OpenOptions::new().read(true).open(&dv_dir).await?;
    dir_file.sync_all().await?;

    Ok(())
}

/// Writes positions.bin sidecar file for a segment.
///
/// # Errors
/// Returns an error if the file cannot be written or synced.
pub async fn write_positions(
    segments_dir: impl AsRef<Path>,
    segment_num: u64,
    index: &inverted_index::InvertedIndex,
) -> std::io::Result<()> {
    positions_writer::write_positions(segments_dir.as_ref(), segment_num, index).await
}

/// Reads all doc values fields from the sidecar directory.
///
/// # Errors
/// Returns an error if file operations or deserialization fails.
pub async fn read_doc_values(
    segments_dir: impl AsRef<Path>,
) -> Result<BTreeMap<String, DocValuesField>> {
    let segments_dir = segments_dir.as_ref();
    let dv_dir = doc_values_dir(segments_dir);

    if !fs::try_exists(&dv_dir).await? {
        return Ok(BTreeMap::new());
    }

    let mut entries = fs::read_dir(&dv_dir).await?;
    let mut result = BTreeMap::new();

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "bin") {
            let mut file = OpenOptions::new().read(true).open(&path).await?;
            let mut len_buf = [0u8; 4];
            file.read_exact(&mut len_buf).await?;
            let header_len = u32::from_le_bytes(len_buf) as usize;

            let mut header_buf = vec![0u8; header_len];
            file.read_exact(&mut header_buf).await?;
            let header: DocValuesHeader = serde_json::from_slice(&header_buf)?;
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;

            result.insert(
                header.field.clone(),
                DocValuesField {
                    field: header.field,
                    value_type: header.value_type,
                    doc_count: header.doc_count,
                    data,
                },
            );
        }
    }

    Ok(result)
}

/// Writes a segment snapshot to the segments directory.
///
/// # Errors
/// Returns an error if file operations or serialization fails.
pub async fn write_segment_snapshot(
    segments_dir: impl AsRef<Path>,
    snapshot: &SegmentSnapshot,
) -> Result<()> {
    let segments_dir = segments_dir.as_ref();
    fs::create_dir_all(segments_dir).await?;

    let path = segment_snapshot_path(segments_dir);
    let temp_path = segments_dir.join("current.tmp");
    let bytes = serde_json::to_vec_pretty(snapshot)?;

    fs::write(&temp_path, bytes).await?;
    fs::rename(temp_path, path).await?;

    // Sync the directory to ensure the rename is durable on disk
    let dir_file = OpenOptions::new().read(true).open(segments_dir).await?;
    dir_file.sync_all().await?;

    Ok(())
}

fn snapshots_dir(segments_dir: &Path) -> PathBuf {
    segments_dir.join("snapshots")
}

fn validate_snapshot_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CloudSearchError::InvalidWalRecord(
            "snapshot name cannot be empty".to_string(),
        ));
    }
    if name == "." || name == ".." {
        return Err(CloudSearchError::InvalidWalRecord(
            "snapshot name cannot be '.' or '..'".to_string(),
        ));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(CloudSearchError::InvalidWalRecord(
            "snapshot name cannot contain path separators".to_string(),
        ));
    }
    if Path::new(name).components().any(|c| c.as_os_str() == ".") {
        return Err(CloudSearchError::InvalidWalRecord(
            "snapshot name cannot contain path components".to_string(),
        ));
    }
    Ok(())
}

fn snapshot_data_path(segments_dir: &Path, name: &str) -> PathBuf {
    snapshots_dir(segments_dir).join(format!("{name}.json"))
}

fn snapshot_meta_path(segments_dir: &Path, name: &str) -> PathBuf {
    snapshots_dir(segments_dir).join(format!("{name}.meta.json"))
}

/// Writes a named snapshot and its metadata to disk.
///
/// # Errors
/// Returns an error if validation fails, file operations fail, or checksums don't match.
pub async fn write_named_snapshot(
    segments_dir: impl AsRef<Path>,
    name: &str,
    snapshot: &SegmentSnapshot,
    metadata: &SnapshotMetadata,
) -> Result<()> {
    let segments_dir = segments_dir.as_ref();
    validate_snapshot_name(name)?;
    let dir = snapshots_dir(segments_dir);
    fs::create_dir_all(&dir).await?;

    let data_bytes = serde_json::to_vec(snapshot)?;
    let data_checksum = crc32c::crc32c(&data_bytes);
    if data_checksum != metadata.checksum {
        return Err(CloudSearchError::InvalidWalRecord(
            "snapshot checksum mismatch".to_string(),
        ));
    }

    let data_pretty = serde_json::to_vec_pretty(snapshot)?;
    let data_temp = dir.join(format!("{name}.tmp"));
    fs::write(&data_temp, data_pretty).await?;
    fs::rename(data_temp, snapshot_data_path(segments_dir, name)).await?;

    let meta_bytes = serde_json::to_vec(metadata)?;
    let meta_pretty = serde_json::to_vec_pretty(metadata)?;
    let meta_temp = dir.join(format!("{name}.meta.tmp"));
    fs::write(&meta_temp, meta_pretty).await?;
    fs::rename(meta_temp, snapshot_meta_path(segments_dir, name)).await?;

    // Sync directory to ensure all renames are durable
    let dir_file = OpenOptions::new().read(true).open(&dir).await?;
    dir_file.sync_all().await?;

    // Silence unused variable warning for meta_bytes
    let _ = meta_bytes;

    Ok(())
}

/// Reads a named snapshot from disk.
///
/// # Errors
/// Returns an error if validation fails or file operations fail.
pub async fn read_named_snapshot(
    segments_dir: impl AsRef<Path>,
    name: &str,
) -> Result<Option<SegmentSnapshot>> {
    validate_snapshot_name(name)?;
    let path = snapshot_data_path(segments_dir.as_ref(), name);
    if !fs::try_exists(&path).await? {
        return Ok(None);
    }
    let bytes = fs::read(path).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

/// Reads metadata for a named snapshot.
///
/// # Errors
/// Returns an error if validation fails or file operations fail.
pub async fn read_snapshot_metadata(
    segments_dir: impl AsRef<Path>,
    name: &str,
) -> Result<Option<SnapshotMetadata>> {
    validate_snapshot_name(name)?;
    let path = snapshot_meta_path(segments_dir.as_ref(), name);
    if !fs::try_exists(&path).await? {
        return Ok(None);
    }
    let bytes = fs::read(path).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

/// Lists all named snapshots for an index.
///
/// # Errors
/// Returns an error if directory reading fails.
pub async fn list_snapshots(segments_dir: impl AsRef<Path>) -> Result<Vec<SnapshotMetadata>> {
    let segments_dir = segments_dir.as_ref();
    let dir = snapshots_dir(segments_dir);
    if !fs::try_exists(&dir).await? {
        return Ok(Vec::new());
    }

    let mut entries = fs::read_dir(&dir).await?;
    let mut snapshots = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        let meta_path = entry.path();
        let meta_name = meta_path.file_name().and_then(|s| s.to_str());
        let Some(meta_name) = meta_name else { continue };

        if !meta_name.ends_with(".meta.json") {
            continue;
        }

        let bytes = fs::read(&meta_path).await?;
        let meta: SnapshotMetadata = serde_json::from_slice(&bytes)?;

        // Check that the sibling data file exists
        let snapshot_name = &meta.name;
        let data_path = snapshot_data_path(segments_dir, snapshot_name);
        if !fs::try_exists(&data_path).await? {
            return Err(CloudSearchError::InvalidWalRecord(format!(
                "missing snapshot data file for '{snapshot_name}'"
            )));
        }

        snapshots.push(meta);
    }

    snapshots.sort_by_key(|a| a.created_at);
    Ok(snapshots)
}

/// Deletes a named snapshot.
///
/// # Errors
/// Returns an error if validation fails or file operations fail.
pub async fn delete_snapshot(segments_dir: impl AsRef<Path>, name: &str) -> Result<()> {
    validate_snapshot_name(name)?;
    let segments_dir = segments_dir.as_ref();
    let data_path = snapshot_data_path(segments_dir, name);
    let meta_path = snapshot_meta_path(segments_dir, name);

    if fs::try_exists(&data_path).await? {
        fs::remove_file(data_path).await?;
    }
    if fs::try_exists(&meta_path).await? {
        fs::remove_file(meta_path).await?;
    }
    Ok(())
}

fn manifest_path(segments_dir: &Path) -> PathBuf {
    segments_dir.join("manifest.json")
}

#[must_use]
pub fn segment_file_path(segments_dir: &Path, segment_number: u64) -> PathBuf {
    segments_dir.join(format!("seg_{segment_number:06}.json"))
}

/// Writes the index manifest atomically (rename + dir sync).
///
/// # Errors
/// Returns an error if file operations or serialization fails.
pub async fn write_index_manifest(
    segments_dir: impl AsRef<Path>,
    manifest: &IndexManifest,
) -> Result<()> {
    let segments_dir = segments_dir.as_ref();
    let path = manifest_path(segments_dir);
    let temp_path = segments_dir.join("manifest.tmp");
    let bytes = serde_json::to_vec_pretty(manifest)?;
    fs::write(&temp_path, bytes).await?;
    fs::rename(temp_path, &path).await?;
    // Sync directory so manifest entry is durable
    let dir_file = OpenOptions::new().read(true).open(segments_dir).await?;
    dir_file.sync_all().await?;
    Ok(())
}

/// Reads the index manifest, returning None if it doesn't exist.
///
/// # Errors
/// Returns an error if file operations or deserialization fails.
pub async fn read_index_manifest(segments_dir: impl AsRef<Path>) -> Result<Option<IndexManifest>> {
    let path = manifest_path(segments_dir.as_ref());
    if !fs::try_exists(&path).await? {
        return Ok(None);
    }
    let bytes = fs::read(&path).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

/// Checks if the legacy `current.json` exists (for migration).
///
/// # Errors
/// Returns an error if file operations fail.
pub async fn legacy_snapshot_exists(segments_dir: impl AsRef<Path>) -> Result<bool> {
    Ok(fs::try_exists(segment_snapshot_path(segments_dir.as_ref())).await?)
}

fn segment_snapshot_path(segments_dir: &Path) -> PathBuf {
    segments_dir.join("current.json")
}

fn record_type(record: &WalRecord) -> u8 {
    match record {
        WalRecord::IndexDocument { .. } => 1,
        WalRecord::DeleteDocument { .. } => 2,
        WalRecord::MappingUpdate { .. } => 3,
    }
}

fn decode_record(record_type: u8, payload: &[u8]) -> Result<WalRecord> {
    match record_type {
        1..=3 => Ok(serde_json::from_slice(payload)?),
        _ => Err(CloudSearchError::InvalidWalRecord(format!(
            "unknown record type {record_type}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn writes_and_reads_segment_snapshot() {
        let temp_dir = TempDir::new().expect("temp dir");
        let snapshot = SegmentSnapshot {
            last_sequence_number: 7,
            documents: vec![IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello"}),
            }],
        };

        write_segment_snapshot(temp_dir.path(), &snapshot)
            .await
            .expect("write snapshot");

        let loaded = read_segment_snapshot(temp_dir.path())
            .await
            .expect("read snapshot")
            .expect("snapshot exists");

        assert_eq!(loaded, snapshot);
    }

    #[test]
    fn segment_manifest_is_derived_from_snapshot() {
        let snapshot = SegmentSnapshot {
            last_sequence_number: 42,
            documents: vec![
                IndexDocument {
                    id: "doc-1".to_string(),
                    source: serde_json::json!({"message": "hello"}),
                },
                IndexDocument {
                    id: "doc-2".to_string(),
                    source: serde_json::json!({"message": "world"}),
                },
            ],
        };

        let manifest = SegmentManifest::from(&snapshot);

        assert_eq!(manifest.last_sequence_number, 42);
        assert_eq!(manifest.document_count, 2);
    }

    #[tokio::test]
    async fn missing_segment_snapshot_returns_none() {
        let temp_dir = TempDir::new().expect("temp dir");

        let loaded = read_segment_snapshot(temp_dir.path())
            .await
            .expect("read snapshot");

        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn appends_and_replays_records_in_order() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append doc");

        manager
            .append(
                2,
                WalRecord::DeleteDocument {
                    document_id: "doc-1".to_string(),
                },
            )
            .await
            .expect("append delete");

        let entries = manager.replay().await.expect("replay");

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence_number, 1);
        assert_eq!(entries[1].sequence_number, 2);
    }

    #[tokio::test]
    async fn ignores_partial_tail_record() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let log_path = manager.wal_dir().join("000001.log");
        let mut file = OpenOptions::new()
            .append(true)
            .open(log_path)
            .await
            .expect("open log");
        file.write_all(&[1, 2, 3, 4]).await.expect("append partial");
        file.flush().await.expect("flush");

        let entries = manager.replay().await.expect("replay");
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn fails_on_checksum_mismatch() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let log_path = manager.wal_dir().join("000001.log");
        let mut bytes = fs::read(&log_path).await.expect("read wal");
        let payload_offset = HEADER_LEN;
        bytes[payload_offset] ^= 0x01;
        fs::write(log_path, bytes).await.expect("rewrite wal");

        let error = manager
            .replay()
            .await
            .expect_err("checksum mismatch should fail");
        assert!(matches!(error, CloudSearchError::WalChecksumMismatch));
    }

    #[tokio::test]
    async fn next_sequence_number_advances_with_replayed_entries() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        assert_eq!(manager.next_sequence_number().await.expect("next seq"), 1);

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append doc");

        assert_eq!(manager.next_sequence_number().await.expect("next seq"), 2);
    }

    #[tokio::test]
    async fn replay_from_skips_covered_sequence_numbers() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        for sequence_number in 1..=3 {
            manager
                .append(
                    sequence_number,
                    WalRecord::IndexDocument {
                        document: IndexDocument {
                            id: format!("doc-{sequence_number}"),
                            source: serde_json::json!({"value": sequence_number}),
                        },
                    },
                )
                .await
                .expect("append doc");
        }

        let entries = manager.replay_from(2).await.expect("replay from");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence_number, 3);
    }

    #[tokio::test]
    async fn rollover_creates_new_current_generation_and_replays_across_generations() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "first"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let next_generation = manager.rollover().await.expect("rollover");
        assert_eq!(next_generation, 2);

        manager
            .append(
                2,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-2".to_string(),
                        source: serde_json::json!({"message": "second"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let generations = manager.list_generations().await.expect("list generations");
        assert_eq!(generations, vec![1, 2]);

        let entries = manager.replay().await.expect("replay");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence_number, 1);
        assert_eq!(entries[1].sequence_number, 2);
    }

    #[tokio::test]
    async fn trim_through_removes_covered_inactive_generations_only() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "first"}),
                    },
                },
            )
            .await
            .expect("append doc");
        manager.rollover().await.expect("rollover");
        manager
            .append(
                2,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-2".to_string(),
                        source: serde_json::json!({"message": "second"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let trimmed = manager.trim_through(1).await.expect("trim");
        assert_eq!(trimmed, 1);
        let generations = manager.list_generations().await.expect("list generations");
        assert_eq!(generations, vec![2]);
    }

    #[tokio::test]
    async fn trim_through_is_noop_when_nothing_is_eligible() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "first"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let trimmed = manager.trim_through(0).await.expect("trim");
        assert_eq!(trimmed, 0);
        let generations = manager.list_generations().await.expect("list generations");
        assert_eq!(generations, vec![1]);
    }

    #[tokio::test]
    async fn rollover_twice_without_writes_keeps_empty_active_generation() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        assert_eq!(manager.rollover().await.expect("first rollover"), 2);
        assert_eq!(manager.rollover().await.expect("second rollover"), 3);

        let generations = manager.list_generations().await.expect("list generations");
        assert!(generations.is_empty());
        assert_eq!(
            manager
                .current_generation()
                .await
                .expect("current generation"),
            3
        );
        assert!(manager.replay().await.expect("replay").is_empty());
    }

    #[tokio::test]
    async fn fails_when_current_generation_file_is_invalid() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        fs::write(manager.wal_dir().join("CURRENT"), "not-a-number\n")
            .await
            .expect("rewrite current");

        let error = manager
            .replay()
            .await
            .expect_err("invalid current should fail");
        assert!(matches!(error, CloudSearchError::InvalidWalRecord(_)));
    }

    #[tokio::test]
    async fn fails_on_unknown_record_type() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let log_path = manager.wal_dir().join("000001.log");
        let mut bytes = fs::read(&log_path).await.expect("read wal");
        bytes[1] = 99;
        fs::write(log_path, bytes).await.expect("rewrite wal");

        let error = manager
            .replay()
            .await
            .expect_err("unknown record type should fail");
        assert!(matches!(error, CloudSearchError::InvalidWalRecord(_)));
    }

    #[tokio::test]
    async fn mapping_update_records_replay_successfully() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::MappingUpdate {
                    mapping_version: 2,
                    reason: "dynamic_inference".to_string(),
                },
            )
            .await
            .expect("append mapping update");

        let entries = manager.replay().await.expect("replay");
        assert_eq!(entries.len(), 1);
        assert!(matches!(
            &entries[0].record,
            WalRecord::MappingUpdate {
                mapping_version: 2,
                reason,
            } if reason == "dynamic_inference"
        ));
    }

    #[tokio::test]
    async fn replay_stops_at_partial_second_record_and_keeps_first() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append first doc");

        let payload = serde_json::to_vec(&WalRecord::IndexDocument {
            document: IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "world"}),
            },
        })
        .expect("serialize payload");
        let checksum = crc32c::crc32c(&payload);

        let mut header = Vec::with_capacity(HEADER_LEN);
        header.push(WAL_VERSION);
        header.push(1);
        header.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        header.extend_from_slice(&2u64.to_le_bytes());
        header.extend_from_slice(&Utc::now().timestamp_millis().to_le_bytes());
        header.extend_from_slice(&checksum.to_le_bytes());

        let log_path = manager.wal_dir().join("000001.log");
        let mut file = OpenOptions::new()
            .append(true)
            .open(log_path)
            .await
            .expect("open log");
        file.write_all(&header).await.expect("write partial header");
        file.write_all(&payload[..payload.len() / 2])
            .await
            .expect("write partial payload");
        file.flush().await.expect("flush");

        let entries = manager.replay().await.expect("replay");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence_number, 1);
    }

    #[tokio::test]
    async fn fails_on_unsupported_wal_version() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let log_path = manager.wal_dir().join("000001.log");
        let mut bytes = fs::read(&log_path).await.expect("read wal");
        bytes[0] = 99; // corrupt version byte
        fs::write(log_path, bytes).await.expect("rewrite wal");

        let error = manager
            .replay()
            .await
            .expect_err("unsupported version should fail");
        assert!(matches!(error, CloudSearchError::InvalidWalRecord(_)));
        assert!(error.to_string().contains("unsupported WAL version"));
    }

    #[tokio::test]
    async fn fails_on_partial_record_in_inactive_generation() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append doc");

        manager.rollover().await.expect("rollover");

        manager
            .append(
                2,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-2".to_string(),
                        source: serde_json::json!({"message": "world"}),
                    },
                },
            )
            .await
            .expect("append doc to new gen");

        // Corrupt a byte in the inactive generation (000001.log)
        let log_path = manager.wal_dir().join("000001.log");
        let mut bytes = fs::read(&log_path).await.expect("read wal");
        bytes.truncate(bytes.len() - 1); // remove last byte → partial record
        fs::write(log_path, bytes).await.expect("rewrite wal");

        let error = manager
            .replay()
            .await
            .expect_err("partial record in inactive gen should fail");
        assert!(matches!(error, CloudSearchError::InvalidWalRecord(_)));
    }

    #[tokio::test]
    async fn replay_skips_empty_inactive_generation() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        // Write two records to gen 1
        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"x": 1}),
                    },
                },
            )
            .await
            .expect("append to gen 1");

        manager.rollover().await.expect("rollover to gen 2");

        // Write one record to gen 2 (active generation)
        manager
            .append(
                2,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-2".to_string(),
                        source: serde_json::json!({"x": 2}),
                    },
                },
            )
            .await
            .expect("append to gen 2");

        // After rollover to gen 3, manually create an empty generation file 000003.log
        // to simulate a generation that was rolled over to but never had any records written.
        let empty_gen_path = manager.wal_dir().join("000003.log");
        fs::write(&empty_gen_path, Vec::new())
            .await
            .expect("create empty gen file");

        // Replay should succeed — empty inactive generations are skipped, not errors
        let entries = manager
            .replay()
            .await
            .expect("replay with empty inactive gen");
        assert_eq!(entries.len(), 2, "should recover all 2 records");
        assert_eq!(entries[0].sequence_number, 1);
        assert_eq!(entries[1].sequence_number, 2);
    }

    #[tokio::test]
    async fn fails_on_malformed_generation_filename() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append doc");

        // Create a file with unparseable stem alongside valid ones
        let malformed = manager.wal_dir().join("abc.log");
        fs::write(&malformed, &[])
            .await
            .expect("create malformed file");

        let error = manager
            .replay()
            .await
            .expect_err("malformed filename should fail");
        assert!(matches!(error, CloudSearchError::InvalidWalRecord(_)));
    }

    #[tokio::test]
    async fn fails_on_corrupted_json_payload() {
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "hello"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let log_path = manager.wal_dir().join("000001.log");
        let mut bytes = fs::read(&log_path).await.expect("read wal");
        bytes[HEADER_LEN] = 0xFF; // corrupt first byte of JSON payload → checksum mismatch
        fs::write(log_path, bytes).await.expect("rewrite wal");

        let error = manager
            .replay()
            .await
            .expect_err("corrupted payload should fail");
        assert!(matches!(error, CloudSearchError::WalChecksumMismatch));
    }

    #[tokio::test]
    async fn replay_handles_sequence_gaps_across_generations() {
        // Simulates a crash where gen 1 has seq 1-2, then gen 2 has seq 5-6 (gap after crash).
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"seq": 1}),
                    },
                },
            )
            .await
            .expect("append seq 1");
        manager
            .append(
                2,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-2".to_string(),
                        source: serde_json::json!({"seq": 2}),
                    },
                },
            )
            .await
            .expect("append seq 2");

        manager.rollover().await.expect("rollover");

        manager
            .append(
                5,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-5".to_string(),
                        source: serde_json::json!({"seq": 5}),
                    },
                },
            )
            .await
            .expect("append seq 5");
        manager
            .append(
                6,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-6".to_string(),
                        source: serde_json::json!({"seq": 6}),
                    },
                },
            )
            .await
            .expect("append seq 6");

        let entries = manager.replay().await.expect("replay");
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].sequence_number, 1);
        assert_eq!(entries[1].sequence_number, 2);
        assert_eq!(entries[2].sequence_number, 5);
        assert_eq!(entries[3].sequence_number, 6);
    }

    #[tokio::test]
    async fn opens_fresh_wal_directory_without_error() {
        // WalManager should open cleanly on an empty directory (no CURRENT, no .log files).
        let temp_dir = TempDir::new().expect("temp dir");

        let manager = WalManager::open(temp_dir.path())
            .await
            .expect("open wal on empty dir");

        manager
            .append(
                1,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "first"}),
                    },
                },
            )
            .await
            .expect("append doc");

        let entries = manager.replay().await.expect("replay");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence_number, 1);
    }

    #[tokio::test]
    async fn fails_on_header_corruption_in_active_generation() {
        // Corrupt the header (length/checksum fields) of a record in the active gen log.
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        for i in 1..=3 {
            manager
                .append(
                    i,
                    WalRecord::IndexDocument {
                        document: IndexDocument {
                            id: format!("doc-{i}"),
                            source: serde_json::json!({"seq": i}),
                        },
                    },
                )
                .await
                .expect("append doc");
        }

        // Corrupt the checksum field (bytes 22-26) in the header of a middle record.
        let log_path = manager.wal_dir().join("000001.log");
        let mut bytes = fs::read(&log_path).await.expect("read wal");
        bytes[22] ^= 0xFF; // corrupt first byte of checksum → checksum mismatch
        fs::write(log_path, bytes).await.expect("rewrite wal");

        let error = manager
            .replay()
            .await
            .expect_err("header corruption should fail");
        assert!(matches!(
            error,
            CloudSearchError::WalChecksumMismatch | CloudSearchError::InvalidWalRecord(_)
        ));
    }

    #[tokio::test]
    async fn replay_from_skips_gaps_and_resumes() {
        // replay_from(N) skips sequences <= N and resumes from the next available.
        // After a rollover, entries with higher sequences should still be replayed.
        let temp_dir = TempDir::new().expect("temp dir");
        let manager = WalManager::open(temp_dir.path()).await.expect("open wal");

        for seq in 1..=3 {
            manager
                .append(
                    seq,
                    WalRecord::IndexDocument {
                        document: IndexDocument {
                            id: format!("doc-{seq}"),
                            source: serde_json::json!({"seq": seq}),
                        },
                    },
                )
                .await
                .expect("append doc");
        }

        manager.rollover().await.expect("rollover");

        manager
            .append(
                6,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-6".to_string(),
                        source: serde_json::json!({"seq": 6}),
                    },
                },
            )
            .await
            .expect("append seq 6");
        manager
            .append(
                7,
                WalRecord::IndexDocument {
                    document: IndexDocument {
                        id: "doc-7".to_string(),
                        source: serde_json::json!({"seq": 7}),
                    },
                },
            )
            .await
            .expect("append seq 7");

        // Replay from seq 3 → should skip seq 1-3 (already covered), resume at seq 6, 7.
        let entries = manager.replay_from(3).await.expect("replay from 3");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence_number, 6);
        assert_eq!(entries[1].sequence_number, 7);
    }

    // Named snapshot tests

    #[tokio::test]
    async fn write_and_read_named_snapshot() {
        let temp_dir = TempDir::new().expect("temp dir");
        let snapshot = SegmentSnapshot {
            last_sequence_number: 7,
            documents: vec![
                IndexDocument {
                    id: "doc-1".to_string(),
                    source: serde_json::json!({"message": "hello"}),
                },
                IndexDocument {
                    id: "doc-2".to_string(),
                    source: serde_json::json!({"message": "world"}),
                },
            ],
        };

        let data_bytes = serde_json::to_vec(&snapshot).unwrap();
        let checksum = crc32c::crc32c(&data_bytes);

        let metadata = SnapshotMetadata {
            name: "backup-1".to_string(),
            created_at: chrono::Utc::now(),
            last_sequence_number: 7,
            document_count: 2,
            checksum,
        };

        write_named_snapshot(temp_dir.path(), "backup-1", &snapshot, &metadata)
            .await
            .expect("write named snapshot");

        let loaded = read_named_snapshot(temp_dir.path(), "backup-1")
            .await
            .expect("read named snapshot")
            .expect("snapshot exists");

        assert_eq!(loaded, snapshot);
    }

    #[tokio::test]
    async fn list_named_snapshots() {
        let temp_dir = TempDir::new().expect("temp dir");

        for i in 1..=3 {
            let snapshot = SegmentSnapshot {
                last_sequence_number: i,
                documents: vec![IndexDocument {
                    id: format!("doc-{i}"),
                    source: serde_json::json!({"n": i}),
                }],
            };
            let data_bytes = serde_json::to_vec(&snapshot).unwrap();
            let checksum = crc32c::crc32c(&data_bytes);

            let metadata = SnapshotMetadata {
                name: format!("backup-{i}"),
                created_at: chrono::Utc::now(),
                last_sequence_number: i,
                document_count: 1,
                checksum,
            };

            write_named_snapshot(
                temp_dir.path(),
                &format!("backup-{i}"),
                &snapshot,
                &metadata,
            )
            .await
            .expect("write snapshot");
        }

        let snapshots = list_snapshots(temp_dir.path())
            .await
            .expect("list snapshots");

        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].name, "backup-1");
        assert_eq!(snapshots[1].name, "backup-2");
        assert_eq!(snapshots[2].name, "backup-3");
    }

    #[tokio::test]
    async fn delete_named_snapshot() {
        let temp_dir = TempDir::new().expect("temp dir");
        let snapshot = SegmentSnapshot {
            last_sequence_number: 5,
            documents: vec![IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"x": 1}),
            }],
        };

        let data_bytes = serde_json::to_vec(&snapshot).unwrap();
        let checksum = crc32c::crc32c(&data_bytes);

        let metadata = SnapshotMetadata {
            name: "to-delete".to_string(),
            created_at: chrono::Utc::now(),
            last_sequence_number: 5,
            document_count: 1,
            checksum,
        };

        write_named_snapshot(temp_dir.path(), "to-delete", &snapshot, &metadata)
            .await
            .expect("write snapshot");

        delete_snapshot(temp_dir.path(), "to-delete")
            .await
            .expect("delete snapshot");

        let loaded = read_named_snapshot(temp_dir.path(), "to-delete")
            .await
            .expect("read after delete");
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn named_snapshot_metadata_round_trip() {
        let temp_dir = TempDir::new().expect("temp dir");
        let snapshot = SegmentSnapshot {
            last_sequence_number: 10,
            documents: vec![],
        };

        let data_bytes = serde_json::to_vec(&snapshot).unwrap();
        let checksum = crc32c::crc32c(&data_bytes);

        let metadata = SnapshotMetadata {
            name: "meta-test".to_string(),
            created_at: chrono::Utc::now(),
            last_sequence_number: 10,
            document_count: 0,
            checksum,
        };

        write_named_snapshot(temp_dir.path(), "meta-test", &snapshot, &metadata)
            .await
            .expect("write snapshot");

        let loaded = read_snapshot_metadata(temp_dir.path(), "meta-test")
            .await
            .expect("read metadata")
            .expect("metadata exists");

        assert_eq!(loaded.name, "meta-test");
        assert_eq!(loaded.last_sequence_number, 10);
        assert_eq!(loaded.document_count, 0);
    }

    #[tokio::test]
    async fn missing_named_snapshot_returns_none() {
        let temp_dir = TempDir::new().expect("temp dir");

        let loaded = read_named_snapshot(temp_dir.path(), "nonexistent")
            .await
            .expect("read nonexistent");
        assert!(loaded.is_none());

        let meta = read_snapshot_metadata(temp_dir.path(), "nonexistent")
            .await
            .expect("read nonexistent meta");
        assert!(meta.is_none());
    }
}
