use cloudsearch_storage::{DocValueType, DocValuesField};
use std::collections::BTreeMap;

/// Reads doc values from pre-built columnar sidecar.
#[derive(Debug, Clone)]
pub struct DocValuesReader {
    fields: BTreeMap<String, DocValuesField>,
}

impl DocValuesReader {
    /// Create from a map of doc values fields (built by DocValuesWriter).
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
    /// Returns None if field doesn't exist or has wrong type.
    pub fn keywords(&self, field: &str) -> Option<Vec<&str>> {
        let f = self.fields.get(field)?;
        if f.value_type != DocValueType::Keyword {
            return None;
        }

        let num_docs = f.doc_count as usize;
        let offset_table_end = num_docs * 4;
        let pool = &f.data[offset_table_end..];

        let mut result = Vec::with_capacity(num_docs);
        for i in 0..num_docs {
            let offset = u32::from_le_bytes(f.data[i * 4..][..4].try_into().unwrap()) as usize;
            let end = if i + 1 < num_docs {
                u32::from_le_bytes(f.data[(i + 1) * 4..][..4].try_into().unwrap()) as usize
            } else {
                pool.len()
            };
            let s = std::str::from_utf8(&pool[offset..end]).unwrap_or("");
            result.push(s);
        }
        Some(result)
    }

    /// Get i64 values for integer/long/timestamp fields.
    /// Returns None if field doesn't exist or has wrong type.
    pub fn i64_values(&self, field: &str) -> Option<Vec<i64>> {
        let f = self.fields.get(field)?;
        if !matches!(
            f.value_type,
            DocValueType::Integer | DocValueType::Long | DocValueType::Timestamp
        ) {
            return None;
        }

        let num_docs = f.doc_count as usize;
        let mut result = Vec::with_capacity(num_docs);
        for chunk in f.data.chunks(8) {
            result.push(i64::from_le_bytes(chunk.try_into().unwrap()));
        }
        Some(result)
    }

    /// Get f64 values for double fields.
    /// Returns None if field doesn't exist or has wrong type.
    pub fn f64_values(&self, field: &str) -> Option<Vec<f64>> {
        let f = self.fields.get(field)?;
        if f.value_type != DocValueType::Double {
            return None;
        }

        let num_docs = f.doc_count as usize;
        let mut result = Vec::with_capacity(num_docs);
        for chunk in f.data.chunks(8) {
            result.push(f64::from_le_bytes(chunk.try_into().unwrap()));
        }
        Some(result)
    }

    /// Get bool values for boolean fields.
    /// Returns None if field doesn't exist or has wrong type.
    #[allow(dead_code)]
    pub fn bool_values(&self, field: &str) -> Option<Vec<bool>> {
        let f = self.fields.get(field)?;
        if f.value_type != DocValueType::Boolean {
            return None;
        }

        let num_docs = f.doc_count as usize;
        let mut result = Vec::with_capacity(num_docs);
        for i in 0..num_docs {
            result.push((f.data[i / 8] >> (i % 8)) & 1 != 0);
        }
        Some(result)
    }
}
