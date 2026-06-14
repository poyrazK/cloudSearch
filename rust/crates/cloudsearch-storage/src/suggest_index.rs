//! Suggest index for autocomplete / as-you-type suggestions.
//!
//! Provides O(log n + m) prefix lookup via sorted term arrays, where n = vocabulary size
//! and m = number of matching terms.

use std::collections::BTreeMap;
use std::sync::Arc;

/// MAGIC bytes for suggest sidecar file: "SUGG" in ASCII.
const SUGGEST_MAGIC: u32 = 0x5355_4747;
const SUGGEST_VERSION: u8 = 1;

/// A single suggest entry — a term with its popularity score.
#[derive(Debug, Clone, PartialEq)]
pub struct SuggestEntry {
    /// The completion text (tokenized, lowercase).
    pub term: String,
    /// Number of documents containing this term.
    pub doc_freq: u32,
    /// Normalized score (`doc_freq` / `n_docs`).
    pub score: f32,
}

/// In-memory suggest index — per-field sorted term arrays.
#[derive(Debug, Clone, Default)]
pub struct SuggestIndex {
    /// Per-field sorted term arrays. Each field's entries are sorted by term ascending.
    pub fields: BTreeMap<String, Vec<SuggestEntry>>,
}

impl SuggestIndex {
    /// Creates a new empty suggest index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the total number of terms across all fields.
    #[must_use]
    pub fn total_terms(&self) -> usize {
        self.fields.values().map(Vec::len).sum()
    }

    /// Returns true if the index has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// In-memory reader for suggest index — loaded from disk.
#[derive(Debug, Clone)]
pub struct SuggestReader {
    /// Per-field sorted term arrays.
    fields: BTreeMap<String, Vec<SuggestEntry>>,
    /// Backing memory map — kept to own the mapping. When None, data was heap-allocated.
    _mmap: Option<Arc<memmap2::Mmap>>,
}

impl SuggestReader {
    /// Loads a suggest reader from previously-written binary data.
    ///
    /// # Errors
    /// Returns an error if the data is corrupted or has an invalid header.
    pub fn from_bytes(data: &[u8]) -> std::io::Result<Self> {
        let mut offset = 0usize;

        // Header: MAGIC (4) + VERSION (1) + PADDING (3) + FIELD_COUNT (4)
        if data.len() < 12 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "suggest data too short for header",
            ));
        }

        let magic = u32::from_le_bytes(data[offset..4].try_into().unwrap());
        if magic != SUGGEST_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid suggest magic: 0x{magic:08X}"),
            ));
        }
        offset += 4;

        let version = data[offset];
        if version != SUGGEST_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported suggest version: {version}"),
            ));
        }
        offset += 4; // skip padding bytes

        let field_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let mut fields = BTreeMap::new();

        for _ in 0..field_count {
            // Field name length + name
            let field_name_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            let field_name = String::from_utf8(data[offset..offset + field_name_len].to_vec())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            offset += field_name_len;

            // Term count for this field
            let term_count =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            let mut entries = Vec::with_capacity(term_count);
            for _ in 0..term_count {
                let term_len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let term = String::from_utf8(data[offset..offset + term_len].to_vec())
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                offset += term_len;

                let doc_freq = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                offset += 4;

                let score = f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                offset += 4;

                entries.push(SuggestEntry {
                    term,
                    doc_freq,
                    score,
                });
            }

            fields.insert(field_name, entries);
        }

        Ok(Self {
            fields,
            _mmap: None,
        })
    }

    /// Creates a suggest reader from a memory-mapped file.
    ///
    /// The backing `Mmap` is kept alive by storing it in the returned struct.
    /// Since sidecar files are immutable after atomic rename, a read-only mmap is safe.
    ///
    /// # Errors
    /// Returns an error if the file format is invalid.
    pub fn from_mmap(mmap: memmap2::Mmap) -> std::io::Result<Self> {
        let result = Self::from_bytes(&mmap)?;
        Ok(Self {
            fields: result.fields,
            _mmap: Some(Arc::new(mmap)),
        })
    }

    /// Returns the sorted entries for a specific field.
    #[must_use]
    pub fn get_field(&self, field: &str) -> Option<&Vec<SuggestEntry>> {
        self.fields.get(field)
    }

    /// Returns all field names in this reader.
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.fields.keys().map(String::as_str)
    }

    /// Finds the first entry index where term >= prefix (lexicographically).
    /// Returns None if no term matches the prefix.
    #[must_use]
    pub fn find_first_prefix(&self, field: &str, prefix: &str) -> Option<usize> {
        let entries = self.fields.get(field)?;
        if entries.is_empty() {
            return None;
        }

        let mut lo = 0usize;
        let mut hi = entries.len();

        // Binary search for lower bound
        while lo < hi {
            let mid = usize::midpoint(lo, hi);
            if entries[mid].term.as_str() < prefix {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if lo < entries.len() && entries[lo].term.starts_with(prefix) {
            Some(lo)
        } else {
            None
        }
    }

    /// Returns all suggestions for a given field and prefix.
    #[must_use]
    pub fn suggest_for_field<'a>(
        &'a self,
        field: &'a str,
        prefix: &'a str,
    ) -> Vec<&'a SuggestEntry> {
        if prefix.is_empty() {
            return Vec::new();
        }
        let Some(start) = self.find_first_prefix(field, prefix) else {
            return Vec::new();
        };

        let Some(entries) = self.fields.get(field) else {
            return Vec::new();
        };

        entries[start..]
            .iter()
            .take_while(|e| e.term.starts_with(prefix))
            .collect()
    }
}

impl SuggestIndex {
    /// Serializes the suggest index to binary format.
    ///
    /// # Panics
    ///
    /// Panics if the number of fields or entries exceeds `u32::MAX`.
    ///
    /// # File format
    /// - Header: MAGIC (4) + VERSION (1) + PADDING (3) + `FIELD_COUNT` (4)
    /// - Per field: `FIELD_NAME_LEN` (4) + `FIELD_NAME` (bytes) + `TERM_COUNT` (4)
    /// - Per term: `STR_LEN` (4) + TERM (bytes) + `DOC_FREQ` (4) + SCORE (4)
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::new();

        // Header
        data.extend_from_slice(&SUGGEST_MAGIC.to_le_bytes());
        data.push(SUGGEST_VERSION);
        data.extend_from_slice(&[0u8, 0u8, 0u8]); // padding
        data.extend_from_slice(&u32::try_from(self.fields.len()).unwrap().to_le_bytes());

        for (field_name, entries) in &self.fields {
            // Field header
            let field_bytes = field_name.as_bytes();
            data.extend_from_slice(&u32::try_from(field_bytes.len()).unwrap().to_le_bytes());
            data.extend_from_slice(field_bytes);
            data.extend_from_slice(&u32::try_from(entries.len()).unwrap().to_le_bytes());

            for entry in entries {
                data.extend_from_slice(&u32::try_from(entry.term.len()).unwrap().to_le_bytes());
                data.extend_from_slice(entry.term.as_bytes());
                data.extend_from_slice(&entry.doc_freq.to_le_bytes());
                data.extend_from_slice(&entry.score.to_le_bytes());
            }
        }

        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries() -> Vec<SuggestEntry> {
        vec![
            SuggestEntry {
                term: "elastic".to_string(),
                doc_freq: 10,
                score: 0.5,
            },
            SuggestEntry {
                term: "elasticsearch".to_string(),
                doc_freq: 5,
                score: 0.25,
            },
            SuggestEntry {
                term: "kubernetes".to_string(),
                doc_freq: 8,
                score: 0.4,
            },
            SuggestEntry {
                term: "rust".to_string(),
                doc_freq: 3,
                score: 0.15,
            },
        ]
    }

    fn make_reader(fields: std::collections::BTreeMap<String, Vec<SuggestEntry>>) -> SuggestReader {
        SuggestReader {
            fields,
            _mmap: None,
        }
    }

    #[test]
    fn find_first_prefix_finds_exact_match() {
        let entries = make_entries();
        let reader = make_reader(std::collections::BTreeMap::from([(
            "title".to_string(),
            entries,
        )]));

        // "elastic" exists
        assert_eq!(reader.find_first_prefix("title", "elastic"), Some(0));
        // "elasticsearch" exists
        assert_eq!(reader.find_first_prefix("title", "elasticsearch"), Some(1));
        // "kube" should match "kubernetes"
        assert_eq!(reader.find_first_prefix("title", "kube"), Some(2));
        // "rust" exists
        assert_eq!(reader.find_first_prefix("title", "rust"), Some(3));
    }

    #[test]
    fn find_first_prefix_returns_none_for_non_matching_prefix() {
        let entries = make_entries();
        let reader = make_reader(std::collections::BTreeMap::from([(
            "title".to_string(),
            entries,
        )]));

        // No term starts with "z"
        assert_eq!(reader.find_first_prefix("title", "z"), None);
        // No term starts with "java"
        assert_eq!(reader.find_first_prefix("title", "java"), None);
    }

    #[test]
    fn find_first_prefix_returns_none_for_empty_entries() {
        let reader = make_reader(std::collections::BTreeMap::new());

        assert_eq!(reader.find_first_prefix("title", "elastic"), None);
    }

    #[test]
    fn find_first_prefix_returns_none_for_missing_field() {
        let entries = make_entries();
        let reader = make_reader(std::collections::BTreeMap::from([(
            "title".to_string(),
            entries,
        )]));

        assert_eq!(reader.find_first_prefix("body", "elastic"), None);
    }

    #[test]
    fn suggest_for_field_iterates_correctly() {
        let entries = make_entries();
        let reader = make_reader(std::collections::BTreeMap::from([(
            "title".to_string(),
            entries,
        )]));

        let suggestions = reader.suggest_for_field("title", "elast");

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].term, "elastic");
        assert_eq!(suggestions[1].term, "elasticsearch");
    }

    #[test]
    fn suggest_for_field_returns_empty_for_no_match() {
        let entries = make_entries();
        let reader = make_reader(std::collections::BTreeMap::from([(
            "title".to_string(),
            entries,
        )]));

        let suggestions = reader.suggest_for_field("title", "z");

        assert!(suggestions.is_empty());
    }

    #[test]
    fn suggest_for_field_stops_at_prefix_boundary() {
        let entries = make_entries();
        let reader = make_reader(std::collections::BTreeMap::from([(
            "title".to_string(),
            entries,
        )]));

        // "elast" should only match "elastic" and "elasticsearch", not "kubernetes"
        let suggestions = reader.suggest_for_field("title", "elast");

        assert_eq!(suggestions.len(), 2);
        assert!(suggestions.iter().all(|e| e.term.starts_with("elast")));
    }

    #[test]
    fn suggest_reader_from_bytes_round_trip() {
        let index = SuggestIndex {
            fields: std::collections::BTreeMap::from([
                (
                    "title".to_string(),
                    vec![
                        SuggestEntry {
                            term: "elastic".to_string(),
                            doc_freq: 10,
                            score: 0.5,
                        },
                        SuggestEntry {
                            term: "elasticsearch".to_string(),
                            doc_freq: 5,
                            score: 0.25,
                        },
                    ],
                ),
                (
                    "description".to_string(),
                    vec![SuggestEntry {
                        term: "rust".to_string(),
                        doc_freq: 3,
                        score: 0.15,
                    }],
                ),
            ]),
        };

        let data = index.to_bytes();
        let loaded = SuggestReader::from_bytes(&data).expect("should load");

        assert_eq!(loaded.fields.len(), 2);
        let title_entries = loaded.get_field("title").expect("title field");
        assert_eq!(title_entries.len(), 2);
        assert_eq!(title_entries[0].term, "elastic");
        assert_eq!(title_entries[0].doc_freq, 10);

        let desc_entries = loaded.get_field("description").expect("description field");
        assert_eq!(desc_entries.len(), 1);
        assert_eq!(desc_entries[0].term, "rust");
    }

    #[test]
    fn suggest_for_field_returns_empty_for_empty_prefix() {
        let entries = make_entries();
        let reader = make_reader(std::collections::BTreeMap::from([(
            "title".to_string(),
            entries,
        )]));

        // Empty prefix should return no results, not the entire vocabulary
        let suggestions = reader.suggest_for_field("title", "");
        assert!(
            suggestions.is_empty(),
            "empty prefix must not return all terms"
        );
    }

    #[test]
    fn suggest_reader_from_mmap_round_trip() {
        use std::io::Write;

        let index = SuggestIndex {
            fields: std::collections::BTreeMap::from([(
                "title".to_string(),
                vec![
                    SuggestEntry {
                        term: "elastic".to_string(),
                        doc_freq: 10,
                        score: 0.5,
                    },
                    SuggestEntry {
                        term: "elasticsearch".to_string(),
                        doc_freq: 5,
                        score: 0.25,
                    },
                ],
            )]),
        };

        let data = index.to_bytes();

        // Write to temp file
        let mut tmpfile = tempfile::NamedTempFile::new().expect("temp file");
        tmpfile.write_all(&data).expect("write");
        tmpfile.flush().expect("flush");

        // Load via mmap
        let std_file = std::fs::File::open(tmpfile.path()).expect("open file");
        let mmap = unsafe { memmap2::Mmap::map(&std_file) }.expect("mmap");
        let from_mmap_reader = SuggestReader::from_mmap(mmap).expect("from_mmap");

        // Compare with from_bytes
        let from_bytes_reader = SuggestReader::from_bytes(&data).expect("from_bytes");

        assert_eq!(
            from_bytes_reader.fields.len(),
            from_mmap_reader.fields.len()
        );
        let title_from_mmap = from_mmap_reader.get_field("title").expect("title field");
        let title_from_bytes = from_bytes_reader.get_field("title").expect("title field");
        assert_eq!(title_from_mmap.len(), title_from_bytes.len());
        assert_eq!(title_from_mmap[0].term, "elastic");
        assert!((title_from_mmap[0].score - 0.5).abs() < 1e-6);
    }
}
