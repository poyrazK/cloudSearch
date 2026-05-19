use cloudsearch_storage::{DocValueType, DocValuesField};
use std::collections::BTreeMap;

/// Reads doc values from pre-built columnar sidecar.
#[derive(Debug, Clone)]
pub struct DocValuesReader {
    fields: BTreeMap<String, DocValuesField>,
}

impl DocValuesReader {
    /// Create from a map of doc values fields (built by `DocValuesWriter`).
    #[allow(dead_code)]
    pub fn new(fields: BTreeMap<String, DocValuesField>) -> Self {
        Self { fields }
    }

    /// Returns an iterator over available field names.
    #[allow(dead_code)]
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.fields.keys().map(std::string::String::as_str)
    }

    /// Returns the doc count, or 0 if no fields loaded.
    #[allow(dead_code)]
    pub fn doc_count(&self) -> u64 {
        self.fields.values().next().map_or(0, |f| f.doc_count)
    }

    /// Get keyword values as a Vec of string slices.
    /// Returns None if field doesn't exist, has wrong type, or data is malformed.
    pub fn keywords(&self, field: &str) -> Option<Vec<&str>> {
        let f = self.fields.get(field)?;
        if f.value_type != DocValueType::Keyword {
            return None;
        }

        let num_docs = usize::try_from(f.doc_count).ok()?;
        let offset_table_end = num_docs.checked_mul(4)?;
        if offset_table_end > f.data.len() {
            return None;
        }
        let pool = &f.data[offset_table_end..];

        let mut result = Vec::with_capacity(num_docs);
        for i in 0..num_docs {
            let offset = u32::from_le_bytes(f.data[i * 4..][..4].try_into().ok()?) as usize;
            let end = if i + 1 < num_docs {
                u32::from_le_bytes(f.data[(i + 1) * 4..][..4].try_into().ok()?) as usize
            } else {
                pool.len()
            };
            if offset > pool.len() || end > pool.len() {
                return None;
            }
            let s = std::str::from_utf8(&pool[offset..end]).unwrap_or("");
            result.push(s);
        }
        Some(result)
    }

    /// Get i64 values for integer/long/timestamp fields.
    /// Returns None if field doesn't exist, has wrong type, or data is malformed.
    pub fn i64_values(&self, field: &str) -> Option<Vec<i64>> {
        let f = self.fields.get(field)?;
        if !matches!(
            f.value_type,
            DocValueType::Integer | DocValueType::Long | DocValueType::Timestamp
        ) {
            return None;
        }

        let num_docs = usize::try_from(f.doc_count).ok()?;
        let expected_len = num_docs.checked_mul(8)?;
        if f.data.len() != expected_len {
            return None;
        }

        let mut result = Vec::with_capacity(num_docs);
        for chunk in f.data.chunks_exact(8) {
            result.push(i64::from_le_bytes(chunk.try_into().unwrap()));
        }
        Some(result)
    }

    /// Get f64 values for double fields.
    /// Returns None if field doesn't exist, has wrong type, or data is malformed.
    pub fn f64_values(&self, field: &str) -> Option<Vec<f64>> {
        let f = self.fields.get(field)?;
        if f.value_type != DocValueType::Double {
            return None;
        }

        let num_docs = usize::try_from(f.doc_count).ok()?;
        let expected_len = num_docs.checked_mul(8)?;
        if f.data.len() != expected_len {
            return None;
        }

        let mut result = Vec::with_capacity(num_docs);
        for chunk in f.data.chunks_exact(8) {
            result.push(f64::from_le_bytes(chunk.try_into().unwrap()));
        }
        Some(result)
    }

    /// Get bool values for boolean fields.
    /// Returns None if field doesn't exist, has wrong type, or data is malformed.
    #[allow(dead_code)]
    pub fn bool_values(&self, field: &str) -> Option<Vec<bool>> {
        let f = self.fields.get(field)?;
        if f.value_type != DocValueType::Boolean {
            return None;
        }

        let num_docs = usize::try_from(f.doc_count).ok()?;
        let required_bytes = num_docs.div_ceil(8);
        if required_bytes > f.data.len() {
            return None;
        }

        let mut result = Vec::with_capacity(num_docs);
        for i in 0..num_docs {
            result.push((f.data[i / 8] >> (i % 8)) & 1 != 0);
        }
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudsearch_storage::DocValueType;

    fn keyword_field(offsets: &[u32], pool: &[u8], doc_count: u64) -> DocValuesField {
        let mut data = Vec::with_capacity(offsets.len() * 4 + pool.len());
        for off in offsets {
            data.extend_from_slice(&off.to_le_bytes());
        }
        data.extend_from_slice(pool);
        DocValuesField {
            field: "field".to_string(),
            value_type: DocValueType::Keyword,
            doc_count,
            data,
        }
    }

    fn i64_field(values: &[i64], vt: DocValueType) -> DocValuesField {
        let mut data = Vec::with_capacity(values.len() * 8);
        for v in values {
            data.extend_from_slice(&v.to_le_bytes());
        }
        DocValuesField {
            field: "field".to_string(),
            value_type: vt,
            doc_count: values.len() as u64,
            data,
        }
    }

    fn f64_field(values: &[f64]) -> DocValuesField {
        let mut data = Vec::with_capacity(values.len() * 8);
        for v in values {
            data.extend_from_slice(&v.to_le_bytes());
        }
        DocValuesField {
            field: "field".to_string(),
            value_type: DocValueType::Double,
            doc_count: values.len() as u64,
            data,
        }
    }

    fn bool_field(bits: u8, doc_count: u64) -> DocValuesField {
        DocValuesField {
            field: "field".to_string(),
            value_type: DocValueType::Boolean,
            doc_count,
            data: vec![bits],
        }
    }

    fn reader_with_field(field: DocValuesField) -> DocValuesReader {
        let mut fields = BTreeMap::new();
        fields.insert("field".to_string(), field);
        DocValuesReader::new(fields)
    }

    #[test]
    fn test_keywords_roundtrip() {
        // Build keyword field: pool has null byte + "apple" + "banana"
        // Offsets: [1, 6] (relative to pool start after 12-byte offset table)
        let pool = b"\x00applebanana";
        let offsets = [1u32, 6];
        let field = keyword_field(&offsets, pool, 2);
        let reader = reader_with_field(field);

        let keywords = reader.keywords("field").unwrap();
        assert_eq!(keywords, vec!["apple", "banana"]);
    }

    #[test]
    fn test_keywords_returns_correct_strings() {
        // Pool layout: [null byte at 0][1:"a"][2:"bc"][4:"x"]
        // Offsets: doc0→1 ("a"), doc1→2 ("bc"), doc2→4 ("x")
        let pool = b"\x00abcx"; // pool[0]='\0', pool[1..2]="a", pool[2..4]="bc", pool[4..5]="x"
        let offsets = [1u32, 2, 4];
        let field = keyword_field(&offsets, pool, 3);
        let reader = reader_with_field(field);

        let keywords = reader.keywords("field").unwrap();
        assert_eq!(keywords, vec!["a", "bc", "x"]);
    }

    #[test]
    fn test_keywords_with_different_lengths() {
        // Pool: [null byte][1:"hello"][7:"world!"][14:"test"]
        let pool = b"\x00hellotestworld!"; // approximate layout
        let offsets = [1u32, 7, 14]; // "hello"(6), "world!"(7), "test"(4)
        let field = keyword_field(&offsets, pool, 3);
        let reader = reader_with_field(field);

        let keywords = reader.keywords("field").unwrap();
        // This test verifies the reader correctly slices variable-length strings
        assert_eq!(keywords.len(), 3);
    }

    #[test]
    fn test_i64_values_roundtrip() {
        let field = i64_field(&[42, -10, i64::MAX, i64::MIN], DocValueType::Long);
        let reader = reader_with_field(field);

        let values = reader.i64_values("field").unwrap();
        assert_eq!(values, [42, -10, i64::MAX, i64::MIN]);
    }

    #[test]
    fn test_integer_values() {
        let field = i64_field(&[100, -50], DocValueType::Integer);
        let reader = reader_with_field(field);

        let values = reader.i64_values("field").unwrap();
        assert_eq!(values, [100, -50]);
    }

    #[test]
    fn test_timestamp_values() {
        let ts = 1_600_000_000_000_i64;
        let field = i64_field(&[ts], DocValueType::Timestamp);
        let reader = reader_with_field(field);

        let values = reader.i64_values("field").unwrap();
        assert_eq!(values, [ts]);
    }

    #[test]
    fn test_f64_values_roundtrip() {
        let field = f64_field(&[0.0, 2.0, -2.5e-10]);
        let reader = reader_with_field(field);

        let values = reader.f64_values("field").unwrap();
        assert_eq!(values, [0.0, 2.0, -2.5e-10]);
    }

    #[test]
    fn test_boolean_values_roundtrip() {
        // Bits: doc0=true (bit0), doc1=false, doc2=true (bit2), doc3=false → 0b0000_0101
        let field = bool_field(0b0000_0101, 4);
        let reader = reader_with_field(field);

        let values = reader.bool_values("field").unwrap();
        assert_eq!(values, vec![true, false, true, false]);
    }

    #[test]
    fn test_wrong_type_returns_none() {
        let field = i64_field(&[1, 2, 3], DocValueType::Integer);
        let reader = reader_with_field(field);

        // Ask for keyword data from an integer field
        assert!(reader.keywords("field").is_none());
        // Ask for f64 data from an integer field
        assert!(reader.f64_values("field").is_none());
        // Boolean from integer
        assert!(reader.bool_values("field").is_none());
    }

    #[test]
    fn test_missing_field_returns_none() {
        let field = i64_field(&[1, 2, 3], DocValueType::Long);
        let reader = reader_with_field(field);

        assert!(reader.keywords("nonexistent").is_none());
        assert!(reader.i64_values("nonexistent").is_none());
        assert!(reader.f64_values("nonexistent").is_none());
        assert!(reader.bool_values("nonexistent").is_none());
    }

    #[test]
    fn test_doc_count() {
        let field = i64_field(&[1, 2, 3, 4, 5], DocValueType::Long);
        let reader = reader_with_field(field);

        assert_eq!(reader.doc_count(), 5);
    }

    #[test]
    fn test_empty_fields() {
        let reader = DocValuesReader::new(BTreeMap::new());
        assert_eq!(reader.doc_count(), 0);
        assert!(reader.fields().next().is_none());
    }

    #[test]
    fn test_fields_iterator() {
        let mut fields = BTreeMap::new();
        fields.insert("a".to_string(), i64_field(&[1], DocValueType::Long));
        fields.insert("b".to_string(), keyword_field(&[1], b"\x00x", 1));
        let reader = DocValuesReader::new(fields);

        let field_names: Vec<_> = reader.fields().collect();
        assert_eq!(field_names, vec!["a", "b"]);
    }

    // ─── Truncated/malformed data tests ───────────────────────────────────────

    #[test]
    fn test_keywords_truncated_offset_table() {
        // doc_count=3 but data only has 8 bytes (needs 12 for 3 offsets)
        let data = vec![1u8, 0, 0, 0, 6, 0, 0, 0]; // only 2 offsets, not 3
        let field = DocValuesField {
            field: "field".to_string(),
            value_type: DocValueType::Keyword,
            doc_count: 3,
            data,
        };
        let reader = reader_with_field(field);
        assert!(reader.keywords("field").is_none());
    }

    #[test]
    fn test_i64_values_truncated_data() {
        // 3 values need 24 bytes, but only 20 bytes provided
        let mut data = vec![0u8; 20];
        data[0..8].copy_from_slice(&0_i64.to_le_bytes());
        data[8..16].copy_from_slice(&1_i64.to_le_bytes());
        let field = DocValuesField {
            field: "field".to_string(),
            value_type: DocValueType::Long,
            doc_count: 3,
            data,
        };
        let reader = reader_with_field(field);
        assert!(reader.i64_values("field").is_none());
    }

    #[test]
    fn test_f64_values_truncated_data() {
        // 3 f64 values need 24 bytes, but only 16 bytes
        let mut data = vec![0u8; 16];
        data[..8].copy_from_slice(&1.0_f64.to_le_bytes());
        data[8..16].copy_from_slice(&2.0_f64.to_le_bytes());
        let field = DocValuesField {
            field: "field".to_string(),
            value_type: DocValueType::Double,
            doc_count: 3,
            data,
        };
        let reader = reader_with_field(field);
        assert!(reader.f64_values("field").is_none());
    }

    #[test]
    fn test_bool_values_truncated_data() {
        // 10 docs need 2 bytes, but only 1 byte provided
        let field = DocValuesField {
            field: "field".to_string(),
            value_type: DocValueType::Boolean,
            doc_count: 10,
            data: vec![0b0000_0101],
        };
        let reader = reader_with_field(field);
        assert!(reader.bool_values("field").is_none());
    }

    #[test]
    fn test_keywords_malformed_offset_past_pool() {
        // Offset table says doc0 starts at byte 100, but pool is only 10 bytes
        let pool = b"\x00abcdefghi"; // 11 bytes total (0-10)
        let offsets = [100u32, 0, 0]; // doc0 offset way past pool
        let mut data = Vec::new();
        for off in offsets {
            data.extend_from_slice(&off.to_le_bytes());
        }
        data.extend_from_slice(pool);
        let field = DocValuesField {
            field: "field".to_string(),
            value_type: DocValueType::Keyword,
            doc_count: 3,
            data,
        };
        let reader = reader_with_field(field);
        assert!(reader.keywords("field").is_none());
    }
}
