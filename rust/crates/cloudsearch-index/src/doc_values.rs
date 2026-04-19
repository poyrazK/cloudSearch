use cloudsearch_common::{FieldMapping, FieldType, IndexDocument};
use cloudsearch_storage::{DocValueType, DocValuesField};
use std::collections::BTreeMap;

/// Builds columnar doc values from documents using field mappings.
pub struct DocValuesWriter;

impl DocValuesWriter {
    /// Build doc values for all documents, using field type from mappings.
    /// Returns a map of field_name -> DocValuesField for each aggregatable field.
    pub fn build_from_documents(
        documents: &[IndexDocument],
        mappings: &BTreeMap<String, FieldMapping>,
    ) -> BTreeMap<String, DocValuesField> {
        let mut result: BTreeMap<String, DocValuesField> = BTreeMap::new();

        for (field, mapping) in mappings {
            if matches!(mapping.field_type, FieldType::Object) {
                continue;
            }

            let doc_value_type = match mapping.field_type {
                FieldType::Keyword => DocValueType::Keyword,
                FieldType::Boolean => DocValueType::Boolean,
                FieldType::Integer => DocValueType::Integer,
                FieldType::Long => DocValueType::Long,
                FieldType::Double => DocValueType::Double,
                FieldType::Timestamp => DocValueType::Timestamp,
                FieldType::Object => unreachable!(),
            };

            let data = match doc_value_type {
                DocValueType::Keyword => encode_keywords(documents, field),
                DocValueType::Integer | DocValueType::Long | DocValueType::Timestamp => {
                    encode_i64(documents, field)
                }
                DocValueType::Double => encode_f64(documents, field),
                DocValueType::Boolean => encode_boolean(documents, field),
            };

            result.insert(
                field.clone(),
                DocValuesField {
                    field: field.clone(),
                    value_type: doc_value_type,
                    doc_count: documents.len() as u64,
                    data,
                },
            );
        }

        result
    }
}

/// Encode keyword field as offset table (u32 per doc) + string pool.
/// Missing fields are represented as (0, 0) — empty string at pool offset 0.
fn encode_keywords(documents: &[IndexDocument], field: &str) -> Vec<u8> {
    let num_docs = documents.len();
    let offset_table_size = num_docs * 4;

    // Build string pool and offset table in one pass
    let mut pool: Vec<u8> = vec![0]; // offset 0 reserved for empty/missing
    let mut offsets: Vec<u32> = Vec::with_capacity(num_docs);

    for doc in documents {
        let offset_before = pool.len() as u32;
        if let Some(s) = doc.source.get(field).and_then(|v| v.as_str()) {
            offsets.push(offset_before);
            pool.extend_from_slice(s.as_bytes());
        } else {
            offsets.push(0); // missing = empty string
        }
    }

    // Pre-allocate: offset table + pool
    let mut result = Vec::with_capacity(offset_table_size + pool.len());
    for offset in offsets {
        result.extend_from_slice(&offset.to_le_bytes());
    }
    result.extend_from_slice(&pool);
    result
}

/// Encode a numeric field (integer/long/timestamp) as packed i64 array.
fn encode_i64(documents: &[IndexDocument], field: &str) -> Vec<u8> {
    let mut result = Vec::with_capacity(documents.len() * 8);
    for doc in documents {
        let n = doc
            .source
            .get(field)
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        result.extend_from_slice(&n.to_le_bytes());
    }
    result
}

/// Encode a double field as packed f64 array.
fn encode_f64(documents: &[IndexDocument], field: &str) -> Vec<u8> {
    let mut result = Vec::with_capacity(documents.len() * 8);
    for doc in documents {
        let n = doc
            .source
            .get(field)
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        result.extend_from_slice(&n.to_le_bytes());
    }
    result
}

/// Encode a boolean field as packed bits (1 bit per document, 8 per byte).
fn encode_boolean(documents: &[IndexDocument], field: &str) -> Vec<u8> {
    let num_bytes = documents.len().div_ceil(8);
    let mut result = vec![0u8; num_bytes];
    for (i, doc) in documents.iter().enumerate() {
        let bit = doc
            .source
            .get(field)
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if bit {
            result[i / 8] |= 1 << (i % 8);
        }
    }
    result
}
