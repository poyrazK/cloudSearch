//! Suggest index file writer and reader.
//!
//! Writes `suggest_{segment}.bin` sidecar files during flush/merge.
//! Reads them back during index open.

use crate::suggest_index::SuggestReader;
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

/// Path for suggest sidecar file for a given segment number.
#[must_use]
pub fn suggest_path(segments_dir: &Path, segment_num: u64) -> PathBuf {
    segments_dir.join(format!("suggest_{segment_num:020}.bin"))
}

/// Writes a suggest index binary file atomically (write to .tmp, then rename).
///
/// # Errors
/// Returns an error if file operations fail.
pub async fn write_suggest(
    segments_dir: &Path,
    segment_num: u64,
    data: &[u8],
) -> std::io::Result<()> {
    let path = suggest_path(segments_dir, segment_num);
    let tmp_path = path.with_extension("bin.tmp");

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .await?;
    file.write_all(data).await?;
    file.flush().await?;
    file.sync_all().await?;
    drop(file);

    fs::rename(&tmp_path, &path).await?;

    // Sync parent directory so the rename is durable
    let dir_file = OpenOptions::new().read(true).open(segments_dir).await?;
    dir_file.sync_all().await?;

    Ok(())
}

/// Reads a suggest index from disk.
///
/// # Errors
/// Returns an error if file operations or parsing fails.
pub async fn read_suggest(segments_dir: &Path, segment_num: u64) -> std::io::Result<SuggestReader> {
    let path = suggest_path(segments_dir, segment_num);
    let mut file = OpenOptions::new().read(true).open(&path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;
    SuggestReader::from_bytes(&data)
}

/// Checks if a suggest sidecar file exists for a given segment.
pub async fn suggest_exists(segments_dir: &Path, segment_num: u64) -> bool {
    tokio::fs::try_exists(suggest_path(segments_dir, segment_num))
        .await
        .unwrap_or(false)
}
