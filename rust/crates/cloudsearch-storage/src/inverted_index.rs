use std::collections::BTreeMap;
use tokio::io::AsyncReadExt;

/// A term and its posting list (list of documents containing the term).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingList {
    /// Postings sorted by `doc_id` for efficient merge during scoring.
    pub docs: Vec<Posting>,
}

/// A single posting: document ID, byte offsets of term occurrences, and term frequency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    /// Document identifier (internal sequence number).
    pub doc_id: u64,
    /// Byte offsets of each occurrence of the term in the field text.
    pub positions: Vec<u32>,
    /// Number of term occurrences in this document.
    pub term_freq: u32,
}

/// Inverted index mapping terms to their posting lists.
#[derive(Debug, Clone, Default)]
pub struct InvertedIndex {
    /// Maps term string to its posting list.
    pub terms: std::collections::BTreeMap<String, PostingList>,
}

impl InvertedIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a posting for a term within a document.
    /// If the term already exists, append to its posting list.
    #[allow(clippy::cast_possible_truncation)]
    pub fn insert(&mut self, term: String, doc_id: u64, positions: Vec<u32>) {
        let term_freq = positions.len() as u32;
        let posting = Posting {
            doc_id,
            positions,
            term_freq,
        };
        self.terms
            .entry(term)
            .or_insert_with(|| PostingList { docs: Vec::new() })
            .docs
            .push(posting);
    }

    /// Get the posting list for a term.
    #[must_use]
    #[allow(clippy::must_use_candidate)]
    pub fn get(&self, term: &str) -> Option<&PostingList> {
        self.terms.get(term)
    }

    /// Number of unique terms in the index.
    #[must_use]
    #[allow(clippy::must_use_candidate)]
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    /// Total number of postings across all terms.
    #[must_use]
    #[allow(clippy::must_use_candidate)]
    pub fn total_postings(&self) -> usize {
        self.terms.values().map(|pl| pl.docs.len()).sum()
    }
}

/// Reads an inverted index from a positions.bin sidecar file.
#[derive(Debug, Clone)]
pub struct PositionsReader {
    /// Maps term string to its byte offset in the body section of the file.
    term_dict: BTreeMap<String, u64>,
    /// Memory-mapped or loaded body section data.
    body_data: Vec<u8>,
}

impl PositionsReader {
    const MAGIC: u32 = 0x50_4F_53_49; // "POSI"
    const VERSION: u8 = 1;

    /// Read and parse a positions.bin file from disk.
    ///
    /// # Errors
    /// Returns an error if the file format is invalid.
    #[allow(clippy::must_use_candidate)]
    pub async fn read(path: &std::path::Path) -> std::io::Result<Self> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        drop(file);

        Self::from_bytes(&data).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Parse from raw bytes.
    ///
    /// # Errors
    /// Returns an error if the data format is invalid.
    ///
    /// # Panics
    /// Panics if the magic bytes don't match (corrupt file).
    #[allow(clippy::must_use_candidate)]
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if data.len() < 12 {
            return Err("positions.bin file too short".to_string());
        }

        let magic = u32::from_le_bytes(data[..4].try_into().unwrap());
        if magic != Self::MAGIC {
            return Err(format!(
                "invalid magic: expected {:x}, got {:x}",
                Self::MAGIC,
                magic
            ));
        }

        let version = data[4];
        if version != Self::VERSION {
            return Err(format!("unsupported version: {version}"));
        }

        let term_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        let mut pos = 12;

        // Read term dictionary: (str_len[4], str[bytes], body_offset[8])
        let mut term_dict = BTreeMap::new();
        for _ in 0..term_count {
            if pos + 4 > data.len() {
                return Err("truncated term dictionary".to_string());
            }
            let str_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + str_len > data.len() {
                return Err("truncated term string".to_string());
            }
            let term = String::from_utf8(data[pos..pos + str_len].to_vec())
                .map_err(|e| format!("invalid UTF-8 in term: {e}"))?;
            pos += str_len;
            if pos + 8 > data.len() {
                return Err("truncated term entry".to_string());
            }
            let body_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            term_dict.insert(term, body_offset);
        }

        // Everything from pos to end is the body section
        let body_data = data[pos..].to_vec();

        Ok(Self {
            term_dict,
            body_data,
        })
    }

    /// Get postings for a term.
    #[must_use]
    pub fn get(&self, term: &str) -> Option<PostingList> {
        let &body_offset = self.term_dict.get(term)?;
        self.read_postings_at(body_offset)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn read_postings_at(&self, offset: u64) -> Option<PostingList> {
        let offset = offset as usize;
        if offset >= self.body_data.len() {
            return None;
        }

        // postings format: doc_count[4], then for each posting:
        //   doc_id[8], freq[4], pos_count[4], positions[pos_count * 4]
        let remaining = &self.body_data[offset..];
        if remaining.len() < 4 {
            return None;
        }

        let doc_count = u32::from_le_bytes(remaining[..4].try_into().unwrap()) as usize;
        let mut pos = 4;
        let mut docs = Vec::with_capacity(doc_count);

        for _ in 0..doc_count {
            if pos + 8 > remaining.len() {
                return None;
            }
            let doc_id = u64::from_le_bytes(remaining[pos..pos + 8].try_into().unwrap());
            pos += 8;

            if pos + 4 > remaining.len() {
                return None;
            }
            let freq = u32::from_le_bytes(remaining[pos..pos + 4].try_into().unwrap());
            pos += 4;

            if pos + 4 > remaining.len() {
                return None;
            }
            let pos_count =
                u32::from_le_bytes(remaining[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + pos_count * 4 > remaining.len() {
                return None;
            }
            let mut positions = Vec::with_capacity(pos_count);
            for i in 0..pos_count {
                let off =
                    u32::from_le_bytes(remaining[pos + i * 4..pos + i * 4 + 4].try_into().unwrap());
                positions.push(off);
            }
            pos += pos_count * 4;

            docs.push(Posting {
                doc_id,
                positions,
                term_freq: freq,
            });
        }

        Some(PostingList { docs })
    }

    /// Iterate over all terms.
    #[allow(dead_code)]
    pub fn terms(&self) -> impl Iterator<Item = &str> {
        self.term_dict.keys().map(String::as_str)
    }

    /// Total number of terms.
    #[must_use]
    #[allow(clippy::must_use_candidate)]
    pub fn term_count(&self) -> usize {
        self.term_dict.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{Posting, PostingList, PositionsReader};

    #[test]
    fn positions_reader_parses_valid_positions_file() {
        // Build a minimal binary positions file manually:
        // Header: MAGIC (4) + VERSION (1) + PADDING (3) + TERM_COUNT (4) = 12 bytes
        // Term dict entry: str_len[4] + str[n] + body_offset[8] = 15 bytes for "hello"
        // Body: doc_count[4] + postings (doc_id[8] + freq[4] + pos_count[4] + positions[n])
        //   1 posting with 2 positions = 24 bytes (4+8+4+4+8)
        // File layout: [header=12][dict=15][body=24] = 51 bytes total
        // body_offset=0 means "postings start at byte 0 of body section"

        let mut data = Vec::new();

        // Header
        data.extend_from_slice(&0x50_4F_53_49u32.to_le_bytes()); // MAGIC
        data.push(1); // VERSION
        data.extend_from_slice(&[0u8, 0u8, 0u8]); // padding
        data.extend_from_slice(&1u32.to_le_bytes()); // term_count = 1

        // Term dict entry: term="hello", body_offset=0
        data.extend_from_slice(&5u32.to_le_bytes()); // str_len = 5
        data.extend_from_slice(b"hello");
        data.extend_from_slice(&0u64.to_le_bytes()); // body_offset = 0 (start of body section)

        // Body: postings for "hello" — doc_count=1, doc_id=0, freq=2, pos_count=2, positions=[0, 6]
        data.extend_from_slice(&1u32.to_le_bytes()); // doc_count = 1
        data.extend_from_slice(&0u64.to_le_bytes()); // doc_id = 0
        data.extend_from_slice(&2u32.to_le_bytes()); // freq = 2
        data.extend_from_slice(&2u32.to_le_bytes()); // pos_count = 2
        data.extend_from_slice(&0u32.to_le_bytes()); // position 0
        data.extend_from_slice(&6u32.to_le_bytes()); // position 6

        let reader = PositionsReader::from_bytes(&data).expect("parse positions file");
        let pl = reader.get("hello").expect("get term hello");
        assert_eq!(pl.docs.len(), 1);
        assert_eq!(pl.docs[0].doc_id, 0);
        assert_eq!(pl.docs[0].term_freq, 2);
        assert_eq!(pl.docs[0].positions, vec![0, 6]);
    }

    #[test]
    fn positions_reader_returns_none_for_missing_term() {
        let mut data = Vec::new();

        // Header with one term entry
        data.extend_from_slice(&0x50_4F_53_49u32.to_le_bytes());
        data.push(1);
        data.extend_from_slice(&[0u8, 0u8, 0u8]);
        data.extend_from_slice(&1u32.to_le_bytes());

        // Term dict entry: term="hello", body_offset=0
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(b"hello");
        data.extend_from_slice(&0u64.to_le_bytes());

        // Body: one posting for "hello"
        let mut body = Vec::new();
        body.extend_from_slice(&1u32.to_le_bytes()); // doc_count = 1
        body.extend_from_slice(&0u64.to_le_bytes()); // doc_id = 0
        body.extend_from_slice(&1u32.to_le_bytes()); // freq = 1
        body.extend_from_slice(&1u32.to_le_bytes()); // pos_count = 1
        body.extend_from_slice(&0u32.to_le_bytes()); // position 0
        data.extend_from_slice(&body);

        let reader = PositionsReader::from_bytes(&data).expect("parse positions file");
        assert!(reader.get("nonexistent").is_none());
        assert!(reader.get("goodbye").is_none());
    }

    #[test]
    fn positions_reader_handles_multiple_terms() {
        let mut data = Vec::new();

        // Header: term_count = 2
        data.extend_from_slice(&0x50_4F_53_49u32.to_le_bytes());
        data.push(1);
        data.extend_from_slice(&[0u8, 0u8, 0u8]);
        data.extend_from_slice(&2u32.to_le_bytes());

        // Term dict entry 1: term="foo", body_offset=0
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(b"foo");
        data.extend_from_slice(&0u64.to_le_bytes());

        // Term dict entry 2: term="bar", body_offset=24
        // (posting list for "foo" occupies 24 bytes: 4 + 8 + 4 + 4 + 4)
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(b"bar");
        data.extend_from_slice(&24u64.to_le_bytes());

        // Body: posting list for "foo" at offset 0
        let mut body = Vec::new();
        body.extend_from_slice(&1u32.to_le_bytes()); // doc_count = 1
        body.extend_from_slice(&0u64.to_le_bytes()); // doc_id = 0
        body.extend_from_slice(&1u32.to_le_bytes()); // freq = 1
        body.extend_from_slice(&1u32.to_le_bytes()); // pos_count = 1
        body.extend_from_slice(&5u32.to_le_bytes()); // position 5

        // Posting list for "bar" at offset 24
        body.extend_from_slice(&1u32.to_le_bytes()); // doc_count = 1
        body.extend_from_slice(&1u64.to_le_bytes()); // doc_id = 1
        body.extend_from_slice(&1u32.to_le_bytes()); // freq = 1
        body.extend_from_slice(&1u32.to_le_bytes()); // pos_count = 1
        body.extend_from_slice(&10u32.to_le_bytes()); // position 10
        data.extend_from_slice(&body);

        let reader = PositionsReader::from_bytes(&data).expect("parse positions file");
        assert_eq!(reader.term_count(), 2);

        let foo_pl = reader.get("foo").expect("get term foo");
        assert_eq!(foo_pl.docs.len(), 1);
        assert_eq!(foo_pl.docs[0].positions, vec![5]);

        let bar_pl = reader.get("bar").expect("get term bar");
        assert_eq!(bar_pl.docs.len(), 1);
        assert_eq!(bar_pl.docs[0].doc_id, 1);
        assert_eq!(bar_pl.docs[0].positions, vec![10]);
    }

    #[test]
    fn positions_reader_rejects_invalid_magic() {
        let mut data = Vec::new();
        data.extend_from_slice(&0xDEAD_BEEFu32.to_le_bytes()); // invalid magic
        data.push(1);
        data.extend_from_slice(&[0u8, 0u8, 0u8]);
        data.extend_from_slice(&0u32.to_le_bytes());

        let result = PositionsReader::from_bytes(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("invalid magic"), "expected magic error, got: {err}");
    }

    #[test]
    fn positions_reader_rejects_truncated_data() {
        // Header only, no term dict
        let mut data = Vec::new();
        data.extend_from_slice(&0x50_4F_53_49u32.to_le_bytes());
        data.push(1);
        data.extend_from_slice(&[0u8, 0u8, 0u8]);
        data.extend_from_slice(&1u32.to_le_bytes()); // term_count = 1 but no dict follows

        let result = PositionsReader::from_bytes(&data);
        assert!(result.is_err());
    }
}
