use std::collections::BTreeMap;

use cloudsearch_common::*;
use uuid::Uuid;

// Serialize to a serde_json::Value, then deserialize back to T.
// This avoids the HRTB inference issues that arise with from_slice on types
// containing &'static str fields.
fn round_trip<T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug>(original: &T) {
    let value: serde_json::Value = serde_json::to_value(original).unwrap();
    let decoded: T = serde_json::from_value(value.clone()).unwrap();
    let value2: serde_json::Value = serde_json::to_value(&decoded).unwrap();
    assert_eq!(value, value2, "JSON round-trip mismatch");
}

// Separate path for types with &'static str fields. These generate
// `impl Deserialize<'static>` rather than `impl for<'de> Deserialize<'de>`,
// so they can't be used with the generic round_trip helper. Instead,
// we serialize to Value, deserialize the Value itself (always works),
// then extract and compare fields manually.
fn round_trip_static_str<T: serde::Serialize + std::fmt::Debug>(
    original: &T,
    check: impl Fn(&serde_json::Value),
) {
    let value: serde_json::Value = serde_json::to_value(original).unwrap();
    // Re-serialize to confirm it's valid JSON (no data loss on serialize)
    let json = serde_json::to_string(&value).unwrap();
    let reparsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value, reparsed, "JSON re-serialize mismatch");
    check(&value);
}

// ─── Enums ────────────────────────────────────────────────────────────────────

// MappingMode

#[test]
fn test_mapping_mode_strict() {
    round_trip(&MappingMode::Strict);
}

#[test]
fn test_mapping_mode_controlled_dynamic() {
    round_trip(&MappingMode::ControlledDynamic);
}

// FieldType

#[test]
fn test_field_type_keyword() {
    round_trip(&FieldType::Keyword);
}

#[test]
fn test_field_type_boolean() {
    round_trip(&FieldType::Boolean);
}

#[test]
fn test_field_type_integer() {
    round_trip(&FieldType::Integer);
}

#[test]
fn test_field_type_long() {
    round_trip(&FieldType::Long);
}

#[test]
fn test_field_type_double() {
    round_trip(&FieldType::Double);
}

#[test]
fn test_field_type_timestamp() {
    round_trip(&FieldType::Timestamp);
}

#[test]
fn test_field_type_object() {
    round_trip(&FieldType::Object);
}

// SortOrder

#[test]
fn test_sort_order_asc() {
    round_trip(&SortOrder::Asc);
}

#[test]
fn test_sort_order_desc() {
    round_trip(&SortOrder::Desc);
}

// DateHistogramInterval

#[test]
fn test_date_histogram_interval_minute() {
    round_trip(&DateHistogramInterval::Minute);
}

#[test]
fn test_date_histogram_interval_hour() {
    round_trip(&DateHistogramInterval::Hour);
}

#[test]
fn test_date_histogram_interval_day() {
    round_trip(&DateHistogramInterval::Day);
}

// SearchQuery

#[test]
fn test_search_query_match_all() {
    round_trip(&SearchQuery::MatchAll);
}

#[test]
fn test_search_query_term() {
    round_trip(&SearchQuery::Term(TermQuery {
        field: "status".to_string(),
        value: serde_json::json!("active"),
        fuzziness: None,
        boost: None,
    }));
}

#[test]
fn test_search_query_terms() {
    round_trip(&SearchQuery::Terms(TermsQuery {
        field: "status".to_string(),
        values: vec![serde_json::json!("active"), serde_json::json!("pending")],
    }));
}

#[test]
fn test_search_query_range() {
    round_trip(&SearchQuery::Range(RangeQuery {
        field: "price".to_string(),
        gte: Some(serde_json::json!(10)),
        gt: None,
        lte: Some(serde_json::json!(100)),
        lt: None,
    }));
}

#[test]
fn test_search_query_bool() {
    round_trip(&SearchQuery::Bool(BoolQuery {
        must: vec![SearchQuery::Term(TermQuery {
            field: "status".to_string(),
            value: serde_json::json!("active"),
            fuzziness: None,
            boost: None,
        })],
        should: vec![SearchQuery::Term(TermQuery {
            field: "tag".to_string(),
            value: serde_json::json!("featured"),
            fuzziness: None,
            boost: None,
        })],
        filter: vec![],
        must_not: vec![SearchQuery::Term(TermQuery {
            field: "deleted".to_string(),
            value: serde_json::json!(true),
            fuzziness: None,
            boost: None,
        })],
        minimum_should_match: None,
    }));
}

// BulkOperation

#[test]
fn test_bulk_operation_index() {
    round_trip(&BulkOperation::Index(BulkIndexOperation {
        id: "doc1".to_string(),
        source: serde_json::json!({"title": "Hello"}),
    }));
}

#[test]
fn test_bulk_operation_delete() {
    round_trip(&BulkOperation::Delete(BulkDeleteOperation {
        id: "doc1".to_string(),
    }));
}

// BulkItem

#[test]
fn test_bulk_item_index() {
    round_trip(&BulkItem::Index(BulkItemResult {
        id: "doc1".to_string(),
        result: "created".to_string(),
        sequence_number: 1,
    }));
}

#[test]
fn test_bulk_item_delete() {
    round_trip(&BulkItem::Delete(BulkItemResult {
        id: "doc1".to_string(),
        result: "deleted".to_string(),
        sequence_number: 2,
    }));
}

// AggregationRequest

#[test]
fn test_aggregation_request_terms() {
    round_trip(&AggregationRequest::Terms(TermsAggregationRequest {
        field: "status".to_string(),
    }));
}

#[test]
fn test_aggregation_request_stats() {
    round_trip(&AggregationRequest::Stats(StatsAggregationRequest {
        field: "price".to_string(),
    }));
}

#[test]
fn test_aggregation_request_date_histogram() {
    round_trip(&AggregationRequest::DateHistogram(
        DateHistogramAggregationRequest {
            field: "created_at".to_string(),
            interval: DateHistogramInterval::Day,
        },
    ));
}

// AggregationResult

#[test]
fn test_aggregation_result_terms() {
    round_trip(&AggregationResult::Terms(TermsAggregationResult {
        buckets: vec![
            TermsBucket {
                key: serde_json::json!("active"),
                doc_count: 10,
            },
            TermsBucket {
                key: serde_json::json!("pending"),
                doc_count: 5,
            },
        ],
    }));
}

#[test]
fn test_aggregation_result_stats() {
    round_trip(&AggregationResult::Stats(StatsAggregationResult {
        count: 100,
        min: Some(1.0),
        max: Some(999.99),
        avg: Some(50.0),
        sum: 5000.0,
    }));
}

#[test]
fn test_aggregation_result_stats_with_nulls() {
    round_trip(&AggregationResult::Stats(StatsAggregationResult {
        count: 0,
        min: None,
        max: None,
        avg: None,
        sum: 0.0,
    }));
}

#[test]
fn test_aggregation_result_date_histogram() {
    round_trip(&AggregationResult::DateHistogram(
        DateHistogramAggregationResult {
            buckets: vec![
                DateHistogramBucket {
                    key: "2024-01-01".to_string(),
                    doc_count: 42,
                },
                DateHistogramBucket {
                    key: "2024-01-02".to_string(),
                    doc_count: 17,
                },
            ],
        },
    ));
}

// SortSpec

#[test]
fn test_sort_spec_asc() {
    round_trip(&SortSpec {
        field: "name".to_string(),
        order: SortOrder::Asc,
    });
}

#[test]
fn test_sort_spec_desc() {
    round_trip(&SortSpec {
        field: "name".to_string(),
        order: SortOrder::Desc,
    });
}

// ─── Structs ─────────────────────────────────────────────────────────────────

// IndexSettings

#[test]
fn test_index_settings_default() {
    round_trip(&IndexSettings::default());
}

#[test]
fn test_index_settings_with_namespace() {
    round_trip(&IndexSettings {
        mapping_mode: MappingMode::Strict,
        primary_time_field: Some("created_at".to_string()),
        namespace: Some("prod".to_string()),
        retention_secs: None,
        merge_threshold_docs: None,
    });
}

#[test]
fn test_index_settings_namespace_none() {
    // namespace: None exercises skip_serializing_if
    round_trip(&IndexSettings {
        mapping_mode: MappingMode::ControlledDynamic,
        primary_time_field: None,
        namespace: None,
        retention_secs: None,
        merge_threshold_docs: None,
    });
}

// IndexMetadata

#[test]
fn test_index_metadata() {
    let now = chrono::Utc::now();
    round_trip(&IndexMetadata {
        id: uuid::Uuid::new_v4(),
        name: "logs".to_string(),
        created_at: now,
        updated_at: now,
        settings: IndexSettings::default(),
        mappings: BTreeMap::from([
            (
                "message".to_string(),
                FieldMapping {
                    field_type: FieldType::Keyword,
                },
            ),
            (
                "count".to_string(),
                FieldMapping {
                    field_type: FieldType::Integer,
                },
            ),
        ]),
    });
}

// FieldMapping

#[test]
fn test_field_mapping() {
    round_trip(&FieldMapping {
        field_type: FieldType::Keyword,
    });
}

// CreateIndexRequest

#[test]
fn test_create_index_request_default() {
    round_trip(&CreateIndexRequest::default());
}

#[test]
fn test_create_index_request_with_settings() {
    round_trip(&CreateIndexRequest {
        settings: IndexSettings {
            mapping_mode: MappingMode::Strict,
            primary_time_field: Some("ts".to_string()),
            namespace: Some("test".to_string()),
            retention_secs: Some(86400),
            merge_threshold_docs: None,
        },
        ..Default::default()
    });
}

// IndexDocument

#[test]
fn test_index_document() {
    round_trip(&IndexDocument {
        id: "doc1".to_string(),
        source: serde_json::json!({"message": "hello world", "count": 42}),
    });
}

// IndexDocumentRequest

#[test]
fn test_index_document_request() {
    round_trip(&IndexDocumentRequest {
        id: "doc1".to_string(),
        source: serde_json::json!({"title": "Test"}),
    });
}

// IndexDocumentResponse
// &'static str fields prevent the generic round_trip helper. Verify the JSON
// representation is stable and check field values from the JSON.
#[test]
fn test_index_document_response() {
    let original = IndexDocumentResponse {
        id: "doc1".to_string(),
        result: "created",
        sequence_number: 5,
    };
    round_trip_static_str(&original, |value| {
        assert_eq!(value["id"], "doc1");
        assert_eq!(value["result"], "created");
        assert_eq!(value["sequence_number"], 5);
    });
}

// GetDocumentResponse

#[test]
fn test_get_document_response_found() {
    round_trip(&GetDocumentResponse {
        id: "doc1".to_string(),
        found: true,
        source: serde_json::json!({"key": "value"}),
    });
}

#[test]
fn test_get_document_response_not_found() {
    round_trip(&GetDocumentResponse {
        id: "missing".to_string(),
        found: false,
        source: serde_json::Value::Null,
    });
}

// BulkRequest

#[test]
fn test_bulk_request_empty() {
    round_trip(&BulkRequest { operations: vec![] });
}

#[test]
fn test_bulk_request_mixed_operations() {
    round_trip(&BulkRequest {
        operations: vec![
            BulkOperation::Index(BulkIndexOperation {
                id: "doc1".to_string(),
                source: serde_json::json!({"x": 1}),
            }),
            BulkOperation::Delete(BulkDeleteOperation {
                id: "doc2".to_string(),
            }),
            BulkOperation::Index(BulkIndexOperation {
                id: "doc3".to_string(),
                source: serde_json::json!({"x": 3}),
            }),
        ],
    });
}

// BulkIndexOperation

#[test]
fn test_bulk_index_operation() {
    round_trip(&BulkIndexOperation {
        id: "doc1".to_string(),
        source: serde_json::json!({"field": "value"}),
    });
}

// BulkDeleteOperation

#[test]
fn test_bulk_delete_operation() {
    round_trip(&BulkDeleteOperation {
        id: "doc1".to_string(),
    });
}

// BulkResponse

#[test]
fn test_bulk_response_no_errors() {
    round_trip(&BulkResponse {
        errors: false,
        items: vec![
            BulkItem::Index(BulkItemResult {
                id: "doc1".to_string(),
                result: "created".to_string(),
                sequence_number: 1,
            }),
            BulkItem::Index(BulkItemResult {
                id: "doc2".to_string(),
                result: "created".to_string(),
                sequence_number: 2,
            }),
        ],
    });
}

#[test]
fn test_bulk_response_with_errors() {
    round_trip(&BulkResponse {
        errors: true,
        items: vec![
            BulkItem::Index(BulkItemResult {
                id: "doc1".to_string(),
                result: "created".to_string(),
                sequence_number: 1,
            }),
            BulkItem::Delete(BulkItemResult {
                id: "doc2".to_string(),
                result: "not_found".to_string(),
                sequence_number: 0,
            }),
        ],
    });
}

// BulkItemResult

#[test]
fn test_bulk_item_result() {
    round_trip(&BulkItemResult {
        id: "doc1".to_string(),
        result: "updated".to_string(),
        sequence_number: 7,
    });
}

// RefreshResponse

#[test]
fn test_refresh_response() {
    let original = RefreshResponse {
        result: "refreshed",
        refreshed_documents: 10,
    };
    round_trip_static_str(&original, |value| {
        assert_eq!(value["result"], "refreshed");
        assert_eq!(value["refreshed_documents"], 10);
    });
}

// FlushResponse

#[test]
fn test_flush_response() {
    let original = FlushResponse {
        result: "flushed",
        flushed_documents: 50,
        sequence_number: 123,
    };
    round_trip_static_str(&original, |value| {
        assert_eq!(value["result"], "flushed");
        assert_eq!(value["flushed_documents"], 50);
        assert_eq!(value["sequence_number"], 123);
    });
}

// MergeResponse

#[test]
fn test_merge_response() {
    let original = MergeResponse {
        result: "merged",
        merged_documents: 200,
    };
    round_trip_static_str(&original, |value| {
        assert_eq!(value["result"], "merged");
        assert_eq!(value["merged_documents"], 200);
    });
}

// SearchRequest

#[test]
fn test_search_request_all_fields() {
    round_trip(&SearchRequest {
        query: Some(SearchQuery::Term(TermQuery {
            field: "status".to_string(),
            value: serde_json::json!("active"),
            fuzziness: None,
            boost: None,
        })),
        from: Some(10),
        size: Some(25),
        sort: Some(SortSpec {
            field: "created_at".to_string(),
            order: SortOrder::Desc,
        }),
        aggs: Some(BTreeMap::from([(
            "status_terms".to_string(),
            AggregationRequest::Terms(TermsAggregationRequest {
                field: "status".to_string(),
            }),
        )])),
        search_after: None,
    });
}

#[test]
fn test_search_request_all_none() {
    round_trip(&SearchRequest::default());
}

// TermQuery

#[test]
fn test_term_query_string_value() {
    round_trip(&TermQuery {
        field: "name".to_string(),
        value: serde_json::json!("alice"),
        ..Default::default()
    });
}

#[test]
fn test_term_query_numeric_value() {
    round_trip(&TermQuery {
        field: "count".to_string(),
        value: serde_json::json!(42),
        ..Default::default()
    });
}

#[test]
fn test_term_query_bool_value() {
    round_trip(&TermQuery {
        field: "active".to_string(),
        value: serde_json::json!(true),
        ..Default::default()
    });
}

// TermsQuery

#[test]
fn test_terms_query_multiple_values() {
    round_trip(&TermsQuery {
        field: "status".to_string(),
        values: vec![
            serde_json::json!("a"),
            serde_json::json!("b"),
            serde_json::json!("c"),
        ],
    });
}

// RangeQuery

#[test]
fn test_range_query_only_gte() {
    round_trip(&RangeQuery {
        field: "price".to_string(),
        gte: Some(serde_json::json!(0)),
        gt: None,
        lte: None,
        lt: None,
    });
}

#[test]
fn test_range_query_only_lt() {
    round_trip(&RangeQuery {
        field: "age".to_string(),
        gte: None,
        gt: None,
        lte: None,
        lt: Some(serde_json::json!(18)),
    });
}

#[test]
fn test_range_query_all_bounds() {
    round_trip(&RangeQuery {
        field: "price".to_string(),
        gte: Some(serde_json::json!(10.0)),
        gt: Some(serde_json::json!(5.0)),
        lte: Some(serde_json::json!(100.0)),
        lt: Some(serde_json::json!(200.0)),
    });
}

// BoolQuery

#[test]
fn test_bool_query_empty_clauses() {
    round_trip(&BoolQuery::default());
}

// SearchResponse

#[test]
fn test_search_response_with_hits_and_aggs() {
    round_trip(&SearchResponse {
        hits: HitsMetadata {
            total: 2,
            hits: vec![
                SearchHit {
                    id: "doc1".to_string(),
                    source: serde_json::json!({"title": "First"}),
                    score: None,
                    highlight: None,
                    sort_values: None,
                },
                SearchHit {
                    id: "doc2".to_string(),
                    source: serde_json::json!({"title": "Second"}),
                    score: None,
                    highlight: None,
                    sort_values: None,
                },
            ],
        },
        aggregations: BTreeMap::from([(
            "status_count".to_string(),
            AggregationResult::Terms(TermsAggregationResult {
                buckets: vec![TermsBucket {
                    key: serde_json::json!("active"),
                    doc_count: 42,
                }],
            }),
        )]),
    });
}

#[test]
fn test_search_response_empty_hits() {
    round_trip(&SearchResponse {
        hits: HitsMetadata {
            total: 0,
            hits: vec![],
        },
        aggregations: BTreeMap::new(),
    });
}

// HitsMetadata

#[test]
fn test_hits_metadata() {
    round_trip(&HitsMetadata {
        total: 5,
        hits: vec![SearchHit {
            id: "doc1".to_string(),
            source: serde_json::json!({"x": 1}),
            score: None,
            highlight: None,
            sort_values: None,
        }],
    });
}

// SearchHit

#[test]
fn test_search_hit() {
    round_trip(&SearchHit {
        id: "doc1".to_string(),
        source: serde_json::json!({"nested": {"field": "value"}}),
        score: None,
        highlight: None,
        sort_values: None,
    });
}

// TermsAggregationRequest

#[test]
fn test_terms_aggregation_request() {
    round_trip(&TermsAggregationRequest {
        field: "category".to_string(),
    });
}

// StatsAggregationRequest

#[test]
fn test_stats_aggregation_request() {
    round_trip(&StatsAggregationRequest {
        field: "price".to_string(),
    });
}

// DateHistogramAggregationRequest

#[test]
fn test_date_histogram_aggregation_request_minute() {
    round_trip(&DateHistogramAggregationRequest {
        field: "timestamp".to_string(),
        interval: DateHistogramInterval::Minute,
    });
}

#[test]
fn test_date_histogram_aggregation_request_hour() {
    round_trip(&DateHistogramAggregationRequest {
        field: "timestamp".to_string(),
        interval: DateHistogramInterval::Hour,
    });
}

#[test]
fn test_date_histogram_aggregation_request_day() {
    round_trip(&DateHistogramAggregationRequest {
        field: "timestamp".to_string(),
        interval: DateHistogramInterval::Day,
    });
}

// TermsAggregationResult

#[test]
fn test_terms_aggregation_result_multiple_buckets() {
    round_trip(&TermsAggregationResult {
        buckets: vec![
            TermsBucket {
                key: serde_json::json!("a"),
                doc_count: 1,
            },
            TermsBucket {
                key: serde_json::json!("b"),
                doc_count: 2,
            },
            TermsBucket {
                key: serde_json::json!("c"),
                doc_count: 3,
            },
        ],
    });
}

// DateHistogramBucket

#[test]
fn test_date_histogram_bucket() {
    round_trip(&DateHistogramBucket {
        key: "2024-06-15T00:00:00Z".to_string(),
        doc_count: 99,
    });
}

// HealthResponse

#[test]
fn test_health_response() {
    let original = HealthResponse { status: "green" };
    round_trip_static_str(&original, |value| {
        assert_eq!(value["status"], "green");
    });
}

// ErrorResponse

#[test]
fn test_error_response() {
    round_trip(&ErrorResponse {
        error: "index not found".to_string(),
    });
}

// AppConfig

#[test]
fn test_app_config_default() {
    round_trip(&AppConfig::default());
}

#[test]
fn test_app_config_custom() {
    round_trip(&AppConfig {
        bind_addr: "0.0.0.0:9000".to_string(),
        data_dir: std::path::PathBuf::from("/var/data/cloudsearch"),
        refresh_interval_secs: 5,
        flush_interval_secs: 60,
        merge_interval_secs: 120,
        retention_interval_secs: 30,
        max_indexes: Some(10),
        max_documents_per_index: Some(1_000_000),
        max_concurrent_background_ops: Some(4),
    });
}

// Snapshot types

#[test]
fn test_create_snapshot_response() {
    let original = CreateSnapshotResponse {
        name: "backup-1".to_string(),
        created_at: chrono::Utc::now(),
        last_sequence_number: 42,
        document_count: 100,
        checksum: 0xDEAD_BEEF,
    };
    round_trip(&original);
}

#[test]
fn test_list_snapshots_response() {
    let original = ListSnapshotsResponse {
        snapshots: vec![
            SnapshotMetadata {
                name: "backup-1".to_string(),
                created_at: chrono::Utc::now(),
                last_sequence_number: 10,
                document_count: 50,
                checksum: 0x1234_5678,
            },
            SnapshotMetadata {
                name: "backup-2".to_string(),
                created_at: chrono::Utc::now(),
                last_sequence_number: 20,
                document_count: 75,
                checksum: 0x8765_4321,
            },
        ],
    };
    round_trip(&original);
}

#[test]
fn test_snapshot_metadata() {
    let original = SnapshotMetadata {
        name: "weekly-backup".to_string(),
        created_at: chrono::Utc::now(),
        last_sequence_number: 100,
        document_count: 500,
        checksum: 0xABCD_EF01,
    };
    round_trip(&original);
}

// AppConfig normalize_intervals

#[test]
fn test_normalize_intervals_resets_zeros() {
    let mut config = AppConfig {
        refresh_interval_secs: 0,
        flush_interval_secs: 0,
        merge_interval_secs: 0,
        retention_interval_secs: 0,
        ..Default::default()
    };
    config.normalize_intervals();
    assert_eq!(config.refresh_interval_secs, 1);
    assert_eq!(config.flush_interval_secs, 30);
    assert_eq!(config.merge_interval_secs, 60);
    assert_eq!(config.retention_interval_secs, 60);
}

#[test]
fn test_normalize_intervals_preserves_nonzero() {
    let mut config = AppConfig {
        refresh_interval_secs: 2,
        flush_interval_secs: 15,
        merge_interval_secs: 90,
        retention_interval_secs: 120,
        ..Default::default()
    };
    config.normalize_intervals();
    assert_eq!(config.refresh_interval_secs, 2);
    assert_eq!(config.flush_interval_secs, 15);
    assert_eq!(config.merge_interval_secs, 90);
    assert_eq!(config.retention_interval_secs, 120);
}

// CloudSearchError

#[test]
fn test_cloudsearch_error_display() {
    assert_eq!(
        format!(
            "{}",
            CloudSearchError::IndexAlreadyExists("my-index".to_string())
        ),
        "index 'my-index' already exists"
    );
    assert_eq!(
        format!("{}", CloudSearchError::IndexNotFound("logs".to_string())),
        "index 'logs' not found"
    );
    assert_eq!(
        format!(
            "{}",
            CloudSearchError::DocumentNotFound("doc-123".to_string())
        ),
        "document 'doc-123' not found"
    );
}

#[test]
fn test_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let cs_err: CloudSearchError = io_err.into();
    assert!(matches!(cs_err, CloudSearchError::Io(_)));
}

#[test]
fn test_error_from_serde_error() {
    let serde_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
    let cs_err: CloudSearchError = serde_err.into();
    assert!(matches!(cs_err, CloudSearchError::Serde(_)));
}

// IndexMetadata::new

#[test]
fn test_index_metadata_new_generates_uuid_and_timestamps() {
    let before = chrono::Utc::now();
    let meta = IndexMetadata::new("test-index", IndexSettings::default());
    let after = chrono::Utc::now();

    assert_eq!(meta.name, "test-index");
    assert!(meta.created_at >= before && meta.created_at <= after);
    assert!(meta.updated_at >= before && meta.updated_at <= after);
    assert_ne!(meta.id, Uuid::nil());
    assert!(meta.mappings.is_empty());
}

// UpdateSettingsRequest and RestoreResponse

#[test]
fn test_update_settings_request_roundtrip() {
    round_trip(&UpdateSettingsRequest {
        retention_secs: Some(86400),
    });
}

#[test]
fn test_update_settings_request_none() {
    round_trip(&UpdateSettingsRequest {
        retention_secs: None,
    });
}

#[test]
fn test_restore_response() {
    let original = RestoreResponse {
        result: "restored".to_string(),
        restored_documents: 42,
        sequence_number: 100,
    };
    round_trip(&original);
}

// BoolQuery with should and filter

#[test]
fn test_bool_query_with_should_and_filter() {
    round_trip(&SearchQuery::Bool(BoolQuery {
        must: vec![],
        should: vec![SearchQuery::Term(TermQuery {
            field: "tag".to_string(),
            value: serde_json::json!("featured"),
            fuzziness: None,
            boost: None,
        })],
        filter: vec![SearchQuery::Range(RangeQuery {
            field: "price".to_string(),
            gte: Some(serde_json::json!(10)),
            gt: None,
            lte: None,
            lt: None,
        })],
        must_not: vec![],
        minimum_should_match: None,
    }));
}

// SearchHit highlight field

#[test]
fn test_search_hit_with_highlight() {
    use std::collections::BTreeMap;
    round_trip(&SearchHit {
        id: "doc1".to_string(),
        source: serde_json::json!({"message": "hello world"}),
        score: Some(1.5),
        highlight: Some(BTreeMap::from([(
            "message".to_string(),
            vec!["<em>hello</em> world".to_string()],
        )])),
        sort_values: None,
    });
}

#[test]
fn test_skip_serializing_none_for_highlight() {
    // Verify highlight is omitted when None using the static_str helper
    let hit = SearchHit {
        id: "doc1".to_string(),
        source: serde_json::json!({"x": 1}),
        score: None,
        highlight: None,
        sort_values: None,
    };
    round_trip_static_str(&hit, |value| {
        // highlight field should not appear in JSON when None
        assert!(
            value.get("highlight").is_none(),
            "highlight should be omitted when None"
        );
    });
}

// PrefixQuery, WildcardQuery, MatchQuery, PhraseQuery

#[test]
fn test_prefix_query_roundtrip() {
    round_trip(&SearchQuery::Prefix(PrefixQuery {
        field: "name".to_string(),
        value: "pref".to_string(),
    }));
}

#[test]
fn test_wildcard_query_roundtrip() {
    round_trip(&SearchQuery::Wildcard(WildcardQuery {
        field: "name".to_string(),
        value: "foo*bar?".to_string(),
    }));
}

#[test]
fn test_match_query_roundtrip() {
    round_trip(&SearchQuery::Match(MatchQuery {
        field: "message".to_string(),
        value: "hello world".to_string(),
    }));
}

#[test]
fn test_phrase_query_roundtrip() {
    round_trip(&SearchQuery::Phrase(PhraseQuery {
        field: "message".to_string(),
        value: "hello world".to_string(),
    }));
}
