use crate::inverted_index::{InvertedIndex, PostingList};
use std::collections::BTreeMap;
use tokio::io::AsyncWriteExt;

/// Writes positions.bin sidecar files from an inverted index.
/// Follows the same atomic-write-via-temp-file pattern as `doc_values`.
pub struct PositionsWriter {
    /// Terms in sorted order (the `BTreeMap` already sorts).
    terms: BTreeMap<String, PostingList>,
}

impl PositionsWriter {
    /// Start a new writer for a segment's inverted index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            terms: BTreeMap::new(),
        }
    }

    /// Add all postings from a per-document inverted index.
    /// The `doc_index` should be a map from `term` to `Vec<u32>` (byte offsets).
    #[allow(clippy::cast_possible_truncation)]
    pub fn add_document(&mut self, doc_id: u64, doc_index: &BTreeMap<String, Vec<u32>>) {
        for (term, positions) in doc_index {
            let term_freq = positions.len() as u32;
            self.terms
                .entry(term.clone())
                .or_insert_with(|| PostingList { docs: Vec::new() })
                .docs
                .push(crate::inverted_index::Posting {
                    doc_id,
                    positions: positions.clone(),
                    term_freq,
                });
        }
    }

    /// Build binary format and return the serialized data.
    ///
    /// # Panics
    /// Panics if the number of terms exceeds `u32::MAX`.
    #[must_use]
    pub fn build(self) -> Vec<u8> {
        let mut result = Vec::new();

        // Header
        result.extend_from_slice(&0x50_4F_53_49u32.to_le_bytes()); // MAGIC
        result.push(1); // VERSION
        let term_count = u32::try_from(self.terms.len()).unwrap();
        result.extend_from_slice(&term_count.to_le_bytes());

        // Collect body offsets by scanning terms in sorted order
        // We build the term dict entries and body in one pass
        let mut body_offsets: Vec<(String, u64)> = Vec::new();
        let mut body = Vec::new();

        for (term, posting_list) in &self.terms {
            body_offsets.push((term.clone(), body.len() as u64));
            // Serialize posting list to body
            let doc_count = u32::try_from(posting_list.docs.len()).unwrap();
            body.extend_from_slice(&doc_count.to_le_bytes());
            for posting in &posting_list.docs {
                body.extend_from_slice(&posting.doc_id.to_le_bytes());
                body.extend_from_slice(&posting.term_freq.to_le_bytes());
                let pos_count = u32::try_from(posting.positions.len()).unwrap();
                body.extend_from_slice(&pos_count.to_le_bytes());
                for p in &posting.positions {
                    body.extend_from_slice(&p.to_le_bytes());
                }
            }
        }

        // Term dictionary: (str_len[4], str[bytes], body_offset[8])
        for (term, body_offset) in &body_offsets {
            let term_bytes = term.as_bytes();
            result.extend_from_slice(&u32::try_from(term_bytes.len()).unwrap().to_le_bytes());
            result.extend_from_slice(term_bytes);
            result.extend_from_slice(&(*body_offset).to_le_bytes());
        }

        // Body section
        result.extend_from_slice(&body);

        result
    }

    /// Write positions.bin atomically to the segment directory.
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened, written, or synced.
    pub async fn write(
        self,
        segments_dir: &std::path::Path,
        segment_num: u64,
    ) -> std::io::Result<()> {
        let data = self.build();
        let path = positions_path(segments_dir, segment_num);
        let tmp_path = positions_path(segments_dir, segment_num).with_extension("tmp");

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;
        file.write_all(&data).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);

        tokio::fs::rename(&tmp_path, &path).await?;
        // Sync parent directory so the rename is durable
        let dir = tokio::fs::OpenOptions::new()
            .read(true)
            .open(segments_dir)
            .await?;
        dir.sync_all().await?;

        Ok(())
    }
}

impl Default for PositionsWriter {
    fn default() -> Self {
        Self::new()
    }
}

fn positions_path(segments_dir: &std::path::Path, segment_num: u64) -> std::path::PathBuf {
    segments_dir.join(format!("positions_{segment_num:020}.bin"))
}

/// Write positions.bin for a segment.
///
/// # Errors
/// Returns an error if the file cannot be written or synced.
///
/// # Panics
/// Panics if the inverted index is too large to serialize (u32 overflow).
#[allow(clippy::must_use_candidate)]
pub async fn write_positions(
    segments_dir: &std::path::Path,
    segment_num: u64,
    index: &InvertedIndex,
) -> std::io::Result<()> {
    let mut writer = PositionsWriter::new();
    for (term, posting_list) in &index.terms {
        for posting in &posting_list.docs {
            writer
                .terms
                .entry(term.clone())
                .or_insert_with(|| PostingList { docs: Vec::new() })
                .docs
                .push(posting.clone());
        }
    }
    writer.write(segments_dir, segment_num).await
}
