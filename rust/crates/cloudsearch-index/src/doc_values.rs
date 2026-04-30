use cloudsearch_common::{FieldMapping, FieldType, IndexDocument};
use cloudsearch_storage::{DocValueType, DocValuesField};
use std::collections::BTreeMap;

/// Builds columnar doc values from documents using field mappings.
pub struct DocValuesWriter;

impl DocValuesWriter {
    /// Build doc values for all documents, using field type from mappings.
    /// Returns a map of `field_name` -> `DocValuesField` for each aggregatable field.
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
        let offset_before = u32::try_from(pool.len()).unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;
    use cloudsearch_common::{FieldMapping, FieldType, IndexDocument};

    fn keyword_mapping() -> BTreeMap<String, FieldMapping> {
        BTreeMap::from([("field".to_string(), FieldMapping {
            field_type: FieldType::Keyword,
        })])
    }

    fn i64_mapping(ft: FieldType) -> BTreeMap<String, FieldMapping> {
        BTreeMap::from([("field".to_string(), FieldMapping { field_type: ft })])
    }

    fn docs(values: &[serde_json::Value]) -> Vec<IndexDocument> {
        values
            .iter()
            .enumerate()
            .map(|(i, v)| IndexDocument {
                id: format!("doc-{i}"),
                source: v.clone(),
            })
            .collect()
    }

    #[test]
    fn test_encode_keyword_roundtrip() {
        let mappings = keyword_mapping();
        let documents = docs(&[
            serde_json::json!({"field": "apple"}),
            serde_json::json!({"field": "banana"}),
            serde_json::json!({"field": "cherry"}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        assert_eq!(field.doc_count, 3);
        assert_eq!(field.value_type, DocValueType::Keyword);

        // First 12 bytes (3 * 4) are the offset table
        let offsets = &field.data[..12];
        let pool = &field.data[12..];

        // Parse offsets and decode strings from pool
        let off0 = u32::from_le_bytes(offsets[0..4].try_into().unwrap()) as usize;
        let off1 = u32::from_le_bytes(offsets[4..8].try_into().unwrap()) as usize;
        let off2 = u32::from_le_bytes(offsets[8..12].try_into().unwrap()) as usize;
        let end2 = pool.len();

        let s0 = std::str::from_utf8(&pool[off0..off1]).unwrap();
        let s1 = std::str::from_utf8(&pool[off1..off2]).unwrap();
        let s2 = std::str::from_utf8(&pool[off2..end2]).unwrap();

        assert_eq!(s0, "apple");
        assert_eq!(s1, "banana");
        assert_eq!(s2, "cherry");
    }

    #[test]
    fn test_encode_keyword_missing() {
        let mappings = keyword_mapping();
        let documents = docs(&[
            serde_json::json!({"field": "present"}),
            serde_json::json!({}), // missing field
            serde_json::json!({"field": "also-present"}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        // Verify doc_count is 3
        assert_eq!(field.doc_count, 3);

        // First offset should be > 0 (some string in pool after null byte)
        let off0 = u32::from_le_bytes(field.data[0..4].try_into().unwrap());
        assert!(off0 > 0, "first offset should be > 0 since 'present' is non-empty");

        // Second offset (missing doc) should be 0
        let off1 = u32::from_le_bytes(field.data[4..8].try_into().unwrap());
        assert_eq!(off1, 0, "missing field should have offset 0");

        // Third offset should be > off0 (the pool grew after adding "also-present")
        let off2 = u32::from_le_bytes(field.data[8..12].try_into().unwrap());
        assert!(off2 > off0, "third string should be stored after the second in pool");
    }

    #[test]
    fn test_encode_i64_roundtrip() {
        let mappings = i64_mapping(FieldType::Long);
        let documents = docs(&[
            serde_json::json!({"field": 42_i64}),
            serde_json::json!({"field": -10_i64}),
            serde_json::json!({"field": i64::MAX}),
            serde_json::json!({"field": i64::MIN}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        assert_eq!(field.doc_count, 4);
        assert_eq!(field.value_type, DocValueType::Long);
        assert_eq!(field.data.len(), 32); // 4 docs * 8 bytes

        let values: Vec<i64> = field.data.chunks(8).map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(values, [42, -10, i64::MAX, i64::MIN]);
    }

    #[test]
    fn test_encode_i64_missing() {
        let mappings = i64_mapping(FieldType::Long);
        let documents = docs(&[
            serde_json::json!({"field": 5_i64}),
            serde_json::json!({}), // missing → 0
            serde_json::json!({"field": 99_i64}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        let values: Vec<i64> = field.data.chunks(8).map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(values, [5, 0, 99]);
    }

    #[test]
    fn test_encode_f64_roundtrip() {
        let mappings = i64_mapping(FieldType::Double);
        let documents = docs(&[
            serde_json::json!({"field": 0.0}),
            serde_json::json!({"field": 2.5}),
            serde_json::json!({"field": -2.5e-10}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        assert_eq!(field.doc_count, 3);
        assert_eq!(field.value_type, DocValueType::Double);

        let values: Vec<f64> = field.data.chunks(8).map(|c| f64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(values, [0.0, 2.5, -2.5e-10]);
    }

    #[test]
    fn test_encode_f64_missing() {
        let mappings = i64_mapping(FieldType::Double);
        let documents = docs(&[
            serde_json::json!({}), // missing → 0.0
            serde_json::json!({"field": 1.5}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        let values: Vec<f64> = field.data.chunks(8).map(|c| f64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(values, [0.0, 1.5]);
    }

    #[test]
    fn test_encode_boolean_roundtrip() {
        let mappings = i64_mapping(FieldType::Boolean);
        let documents = docs(&[
            serde_json::json!({"field": true}),
            serde_json::json!({"field": false}),
            serde_json::json!({"field": true}),
            serde_json::json!({"field": false}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        assert_eq!(field.doc_count, 4);
        assert_eq!(field.value_type, DocValueType::Boolean);
        assert_eq!(field.data.len(), 1); // 4 docs / 8 = 1 byte

        // Bit packing: doc0=true → bit0, doc1=false → bit1 clear, doc2=true → bit2, doc3=false → bit3 clear
        // byte 0 = 0b0000_0101 = 5
        assert_eq!(field.data[0], 0b0000_0101);
    }

    #[test]
    fn test_encode_boolean_all_true() {
        let mappings = i64_mapping(FieldType::Boolean);
        let documents = docs(&[
            serde_json::json!({"field": true}),
            serde_json::json!({"field": true}),
            serde_json::json!({"field": true}),
            serde_json::json!({"field": true}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        assert_eq!(field.data[0], 0b0000_1111); // all 4 bits set
    }

    #[test]
    fn test_encode_boolean_all_false() {
        let mappings = i64_mapping(FieldType::Boolean);
        let documents = docs(&[
            serde_json::json!({"field": false}),
            serde_json::json!({"field": false}),
            serde_json::json!({"field": false}),
            serde_json::json!({"field": false}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        assert_eq!(field.data[0], 0b0000_0000); // all bits clear
    }

    #[test]
    fn test_encode_empty_documents() {
        let mappings = keyword_mapping();
        let documents: Vec<IndexDocument> = vec![];

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        assert_eq!(field.doc_count, 0);
        // Pool has 1 byte (null offset), offset table is empty
        assert_eq!(field.data.len(), 1);
        assert_eq!(field.data[0], 0); // null offset
    }

    #[test]
    fn test_encode_timestamp() {
        let mappings = i64_mapping(FieldType::Timestamp);
        let ts = 1_600_000_000_000_i64; // ~2020-09-28
        let documents = docs(&[serde_json::json!({"field": ts})]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        assert_eq!(field.value_type, DocValueType::Timestamp);
        let val = i64::from_le_bytes(field.data.as_slice().try_into().unwrap());
        assert_eq!(val, ts);
    }

    #[test]
    fn test_encode_integer() {
        let mappings = i64_mapping(FieldType::Integer);
        let documents = docs(&[
            serde_json::json!({"field": 100_i64}),
            serde_json::json!({"field": -50_i64}),
        ]);

        let fields = DocValuesWriter::build_from_documents(&documents, &mappings);
        let field = fields.get("field").expect("field exists");

        assert_eq!(field.value_type, DocValueType::Integer);
        let values: Vec<i64> = field.data.chunks(8).map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(values, [100, -50]);
    }
}
