use chrono::Utc;
use cloudsearch_common::{CloudSearchError, IndexDocument, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

const WAL_VERSION: u8 = 1;
const HEADER_LEN: usize = 26;
const DEFAULT_GENERATION: u64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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

impl WalManager {
    pub async fn open(wal_dir: impl Into<PathBuf>) -> Result<Self> {
        let wal_dir = wal_dir.into();
        fs::create_dir_all(&wal_dir).await?;

        let manager = Self { wal_dir };
        manager.ensure_current_generation().await?;
        Ok(manager)
    }

    pub async fn append(&self, sequence_number: u64, record: WalRecord) -> Result<()> {
        let generation = self.current_generation().await?;
        let path = self.generation_path(generation);
        let payload = serde_json::to_vec(&record)?;
        let checksum = crc32c::crc32c(&payload);
        let timestamp = Utc::now().timestamp_millis();

        let mut header = Vec::with_capacity(HEADER_LEN);
        header.push(WAL_VERSION);
        header.push(record_type(&record));
        header.extend_from_slice(&(payload.len() as u32).to_le_bytes());
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

        Ok(())
    }

    pub async fn replay(&self) -> Result<Vec<WalEntry>> {
        self.replay_from(0).await
    }

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
                let payload_len = u32::from_le_bytes(header[2..6].try_into().unwrap()) as usize;
                let sequence_number = u64::from_le_bytes(header[6..14].try_into().unwrap());
                let recorded_at_unix_ms = i64::from_le_bytes(header[14..22].try_into().unwrap());
                let checksum = u32::from_le_bytes(header[22..26].try_into().unwrap());

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

    pub async fn next_sequence_number(&self) -> Result<u64> {
        let last = self
            .replay()
            .await?
            .last()
            .map(|entry| entry.sequence_number)
            .unwrap_or(0);
        Ok(last + 1)
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

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

    pub async fn rollover(&self) -> Result<u64> {
        let next_generation = self.current_generation().await? + 1;
        fs::write(self.current_path(), format!("{next_generation:06}\n")).await?;
        Ok(next_generation)
    }

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
            let payload_len = u32::from_le_bytes(header[2..6].try_into().unwrap()) as usize;
            let sequence_number = u64::from_le_bytes(header[6..14].try_into().unwrap());
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
    Ok(())
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
        header.extend_from_slice(&(payload.len() as u32).to_le_bytes());
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
}
