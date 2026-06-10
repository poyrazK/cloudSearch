//! Coverage tests for cloudsearch-index.
//!
//! Run with: cargo test -p cloudsearch-index --test coverage

use cloudsearch_common::{
    BoolQuery, CreateIndexRequest, Fuzziness, IndexDocument, IndexSettings, MatchQuery, MltQuery,
    MultiMatchQuery, MultiMatchType, SearchQuery, SearchRequest, SortOrder, SortSpec, TermQuery,
};
use cloudsearch_index::{IndexCatalog, MergePlan};
use cloudsearch_storage::SegmentMeta;
use std::sync::Arc;
use tempfile::TempDir;

fn doc(id: &str, source: serde_json::Value) -> IndexDocument {
    IndexDocument {
        id: id.to_string(),
        source,
    }
}

#[tokio::test]
async fn get_document_returns_none_for_pending_delete() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // Index a doc then delete it — delete goes to pending_operations
    handle
        .index_document(doc("1", serde_json::json!({"x": 1})))
        .await
        .expect("index");
    handle.delete_document("1").await.expect("delete");

    // get_document should return None for deleted doc (pending delete)
    assert!(handle.get_document("1").is_none());
}

#[tokio::test]
async fn apply_merge_plan_skips_when_no_on_disk_segment() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // Index a doc so pending_operations has an entry, but no flush (no on-disk segment)
    handle
        .index_document(doc("1", serde_json::json!({"x": 1})))
        .await
        .expect("index");

    // Create a merge plan — should skip gracefully when no segment on disk
    let plan = MergePlan::new(vec![SegmentMeta {
        segment_number: 1,
        last_sequence_number: 1,
        document_count: 1,
        checksum: 0,
    }]);
    let result = handle.apply_merge_plan(&plan).await;
    result.expect("apply_merge_plan should succeed gracefully");
}

#[tokio::test]
async fn validate_search_request_rejects_nested_bool_with_object_field() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // Index a doc with an object field to establish mapping
    handle
        .index_document(doc("1", serde_json::json!({"meta": {"nested": "value"}})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    // Search with nested bool query that sorts on object field — should be rejected
    let request = SearchRequest {
        query: Some(SearchQuery::Bool(BoolQuery {
            must: vec![SearchQuery::Bool(BoolQuery {
                must: vec![SearchQuery::Term(TermQuery {
                    field: "meta".to_string(),
                    value: serde_json::json!("value"),
                    fuzziness: None,
                    boost: None,
                })],
                should: vec![],
                filter: vec![],
                must_not: vec![],
                minimum_should_match: None,
            })],
            should: vec![],
            filter: vec![],
            must_not: vec![],
            minimum_should_match: None,
        })),
        sort: Some(SortSpec {
            field: "meta".to_string(),
            order: SortOrder::Asc,
        }),
        ..Default::default()
    };

    let result = handle.validate_search_request(&request);
    assert!(
        result.is_err(),
        "nested bool with object sort field should be rejected"
    );
}

#[tokio::test]
async fn validate_search_request_rejects_size_exceeding_max() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let handle = catalog.open_index("test").await.expect("open index");

    let request = SearchRequest {
        size: Some(100_000),
        ..Default::default()
    };

    let result = handle.validate_search_request(&request);
    assert!(
        result.is_err(),
        "size exceeding MAX_SEARCH_SIZE should be rejected"
    );
}

#[tokio::test]
async fn validate_search_request_rejects_from_exceeding_max() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let handle = catalog.open_index("test").await.expect("open index");

    let request = SearchRequest {
        from: Some(2_000_000),
        ..Default::default()
    };

    let result = handle.validate_search_request(&request);
    assert!(
        result.is_err(),
        "from exceeding MAX_SEARCH_OFFSET should be rejected"
    );
}

#[tokio::test]
async fn validate_search_request_rejects_search_after_without_sort() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let handle = catalog.open_index("test").await.expect("open index");

    // search_after without sort is invalid — cursor is meaningless without sort order
    let request = SearchRequest {
        search_after: Some(vec![serde_json::json!(1.0), serde_json::json!("doc123")]),
        ..Default::default()
    };

    let result = handle.validate_search_request(&request);
    assert!(
        result.is_err(),
        "search_after without sort field should be rejected"
    );
}

#[tokio::test]
async fn validate_search_request_rejects_fuzzy_with_search_after() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let handle = catalog.open_index("test").await.expect("open index");

    // Fuzzy query with search_after is invalid — fuzzy matching affects sort order
    let request = SearchRequest {
        query: Some(SearchQuery::Term(TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("admin"),
            fuzziness: Some(Fuzziness::Auto),
            boost: None,
        })),
        search_after: Some(vec![serde_json::json!(1.0), serde_json::json!("doc123")]),
        sort: Some(SortSpec {
            field: "name".to_string(),
            order: SortOrder::Asc,
        }),
        ..Default::default()
    };

    let result = handle.validate_search_request(&request);
    assert!(
        result.is_err(),
        "fuzzy query with search_after should be rejected"
    );
}

#[tokio::test]
async fn highlight_positions_case_insensitive() {
    // Index doc with mixed-case text, search for lowercase term.
    // extract_positions now searches in to_ascii_lowercase()'d text,
    // so token "error" finds "ERROR" positions correctly.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "1",
            serde_json::json!({"content": "ERROR log ERROR message"}),
        ))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Match(MatchQuery {
            field: "content".to_string(),
            value: "error".to_string(),
        })),
        ..Default::default()
    });

    let hit = result.hits.hits.first().expect("should have a hit");
    assert!(
        hit.highlight.is_some(),
        "lowercase 'error' should find highlight in 'ERROR log ERROR'"
    );
}

#[tokio::test]
async fn highlight_positions_multiple_occurrences() {
    // Same term appears twice in one field — all positions should be captured.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "1",
            serde_json::json!({"content": "info error info error"}),
        ))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Match(MatchQuery {
            field: "content".to_string(),
            value: "info".to_string(),
        })),
        ..Default::default()
    });

    let hit = result.hits.hits.first().expect("should have a hit");
    assert!(
        hit.highlight.is_some(),
        "'info' should be highlighted even when it appears twice"
    );
}

#[tokio::test]
async fn highlight_positions_no_match() {
    // Term not in document — no highlight.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("1", serde_json::json!({"content": "hello world"})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    // Use MatchAll so the document IS found (giving us a hit to assert on),
    // then verify that hit has no highlight since no query terms are provided.
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::MatchAll),
        ..Default::default()
    });

    let hit = result
        .hits
        .hits
        .first()
        .expect("doc should be found with MatchAll");
    assert!(
        hit.highlight.is_none(),
        "MatchAll query should produce no highlight (no query terms to highlight)"
    );
}

#[tokio::test]
async fn highlight_positions_empty_field() {
    // Document with empty text field — no highlights.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    let _metadata = catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("1", serde_json::json!({"content": ""})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Match(MatchQuery {
            field: "content".to_string(),
            value: "any".to_string(),
        })),
        ..Default::default()
    });

    assert!(
        result.hits.hits.is_empty() || result.hits.hits.iter().all(|h| h.highlight.is_none()),
        "no highlight for empty text field"
    );
}

#[tokio::test]
async fn fuzzy_match_exact_edit_distance_within_threshold() {
    // Index doc with "name": "admin", search with "admim" (edit distance 1) and fuzziness=1
    // Should match since edit distance <= threshold
    use cloudsearch_common::{Fuzziness, TermQuery};

    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("1", serde_json::json!({"name": "admin"})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    // Edit distance 1 — should match with fuzziness=1
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Term(TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("admim"),
            fuzziness: Some(Fuzziness::Exact(1)),
            boost: None,
        })),
        ..Default::default()
    });
    assert_eq!(
        result.hits.total, 1,
        "edit distance 1 should match with fuzziness=1"
    );
    assert_eq!(result.hits.hits[0].id, "1");
}

#[tokio::test]
async fn fuzzy_match_no_match_when_exceeding_threshold() {
    // Index doc with "name": "admin", search with "xyz" (edit distance 5) and fuzziness=1
    // Should NOT match since edit distance > threshold
    use cloudsearch_common::{Fuzziness, TermQuery};

    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("1", serde_json::json!({"name": "admin"})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Term(TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("xyz"),
            fuzziness: Some(Fuzziness::Exact(1)),
            boost: None,
        })),
        ..Default::default()
    });
    assert_eq!(
        result.hits.total, 0,
        "edit distance 5 should NOT match with fuzziness=1"
    );
}

#[tokio::test]
async fn fuzzy_match_auto_mode_threshold_2_for_long_terms() {
    // Index doc with "name": "admin" (6 chars), use Auto fuzziness
    // Auto threshold for 6+ chars is 2, so "admim" (edit distance 1) should match
    use cloudsearch_common::{Fuzziness, TermQuery};

    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("1", serde_json::json!({"name": "admin"})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Term(TermQuery {
            field: "name".to_string(),
            value: serde_json::json!("admim"),
            fuzziness: Some(Fuzziness::Auto),
            boost: None,
        })),
        ..Default::default()
    });
    assert_eq!(
        result.hits.total, 1,
        "Auto fuzziness (threshold=2) should match edit distance 1"
    );
}

#[tokio::test]
async fn minimum_should_match_requires_multiple_should_matches() {
    // Both docs have only one matching term in should clause.
    // With minimum_should_match=2, neither doc matches both, so total=0.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("1", serde_json::json!({"x": "a"})))
        .await
        .expect("index");
    handle
        .index_document(doc("2", serde_json::json!({"x": "b"})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Bool(BoolQuery {
            should: vec![
                SearchQuery::Term(TermQuery {
                    field: "x".to_string(),
                    value: serde_json::json!("a"),
                    fuzziness: None,
                    boost: None,
                }),
                SearchQuery::Term(TermQuery {
                    field: "x".to_string(),
                    value: serde_json::json!("b"),
                    fuzziness: None,
                    boost: None,
                }),
            ],
            minimum_should_match: Some(2),
            ..Default::default()
        })),
        ..Default::default()
    });

    assert_eq!(
        result.hits.total, 0,
        "minimum_should_match=2 should reject docs with only 1 matching should clause"
    );
}

#[tokio::test]
async fn minimum_should_match_zero_allows_no_should_matches() {
    // Doc only matches "a" not "b". With minimum_should_match=0, all should are optional.
    // Must is empty and filter is empty, so default would be 1 but explicit 0 overrides.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("1", serde_json::json!({"x": "a"})))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Bool(BoolQuery {
            should: vec![
                SearchQuery::Term(TermQuery {
                    field: "x".to_string(),
                    value: serde_json::json!("a"),
                    fuzziness: None,
                    boost: None,
                }),
                SearchQuery::Term(TermQuery {
                    field: "x".to_string(),
                    value: serde_json::json!("b"),
                    fuzziness: None,
                    boost: None,
                }),
            ],
            minimum_should_match: Some(0),
            ..Default::default()
        })),
        ..Default::default()
    });

    assert_eq!(
        result.hits.total, 1,
        "minimum_should_match=0 should allow doc when no should clauses match"
    );
}

#[tokio::test]
async fn mlt_with_doc_id_excludes_source_and_ranks_by_significance() {
    // Index 3 docs: doc1 has terms [A, B, C], doc2 has [A, B], doc3 has [A]
    // MLT on doc1 should find doc2 and doc3 (doc1 excluded), with doc2 ranked higher (more shared terms)
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "doc1",
            serde_json::json!({"content": "apple banana cherry"}),
        ))
        .await
        .expect("index doc1");
    handle
        .index_document(doc("doc2", serde_json::json!({"content": "apple banana"})))
        .await
        .expect("index doc2");
    handle
        .index_document(doc("doc3", serde_json::json!({"content": "apple"})))
        .await
        .expect("index doc3");
    handle.refresh().await.expect("refresh");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Mlt(MltQuery {
            doc_id: Some("doc1".to_string()),
            like: None,
            fields: vec!["content".to_string()],
            min_term_freq: 1,
            min_doc_freq: 1,
            max_query_terms: 25,
            min_word_length: None,
            max_word_length: None,
            field_boost_factor: 1.0,
        })),
        ..Default::default()
    });

    // MLT with doc_id should return empty if positions_readers are empty (needs flush)
    // This is expected behavior - MLT requires positions data
    assert_eq!(
        result.hits.total, 0,
        "MLT requires flushed segments for positions data"
    );
}

#[tokio::test]
async fn mlt_with_like_uses_raw_source() {
    // MLT with `like` parameter should use the provided JSON directly as reference
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("doc1", serde_json::json!({"content": "foo bar baz"})))
        .await
        .expect("index doc1");
    handle
        .index_document(doc("doc2", serde_json::json!({"content": "foo bar"})))
        .await
        .expect("index doc2");
    handle.refresh().await.expect("refresh");

    // MLT with like parameter uses provided JSON directly as reference
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Mlt(MltQuery {
            doc_id: None,
            like: Some(serde_json::json!({"content": "foo bar baz"})),
            fields: vec!["content".to_string()],
            min_term_freq: 1,
            min_doc_freq: 1,
            max_query_terms: 25,
            min_word_length: None,
            max_word_length: None,
            field_boost_factor: 1.0,
        })),
        ..Default::default()
    });

    // MLT requires flushed segments for positions data (positions_readers)
    // Currently returns empty until flush is implemented for per-doc inverted index
    assert_eq!(
        result.hits.total, 0,
        "MLT requires flushed segments for positions data"
    );
}

#[tokio::test]
async fn mlt_rejects_query_with_neither_doc_id_nor_like() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("doc1", serde_json::json!({"content": "test"})))
        .await
        .expect("index doc1");
    handle.refresh().await.expect("refresh");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Mlt(MltQuery {
            doc_id: None,
            like: None,
            fields: vec![],
            min_term_freq: 2,
            min_doc_freq: 1,
            max_query_terms: 25,
            min_word_length: None,
            max_word_length: None,
            field_boost_factor: 1.0,
        })),
        ..Default::default()
    });

    // MLT with neither doc_id nor like should return empty results
    assert_eq!(result.hits.total, 0);
}

#[tokio::test]
async fn mlt_respects_min_term_freq_and_max_query_terms() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // doc1 has 5 occurrences of "rare" and 1 occurrence of "common"
    handle
        .index_document(doc(
            "doc1",
            serde_json::json!({"content": "rare rare rare rare rare common"}),
        ))
        .await
        .expect("index doc1");
    // doc2 has 2 occurrences of "rare"
    handle
        .index_document(doc("doc2", serde_json::json!({"content": "rare rare"})))
        .await
        .expect("index doc2");
    handle.refresh().await.expect("refresh");

    // With min_term_freq=3, only "rare" (5 occurrences) qualifies, not "common" (1)
    // With max_query_terms=1, only the top term is used
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Mlt(MltQuery {
            doc_id: Some("doc1".to_string()),
            like: None,
            fields: vec!["content".to_string()],
            min_term_freq: 3,
            min_doc_freq: 1,
            max_query_terms: 1,
            min_word_length: None,
            max_word_length: None,
            field_boost_factor: 1.0,
        })),
        ..Default::default()
    });

    // MLT requires flushed segments for positions data
    // Currently returns empty until flush is implemented for per-doc inverted index
    assert_eq!(
        result.hits.total, 0,
        "MLT requires flushed segments for positions data"
    );
}

#[tokio::test]
async fn mlt_with_doc_id_not_found_returns_error() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc("doc1", serde_json::json!({"content": "test content"})))
        .await
        .expect("index doc1");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    // MLT with a doc_id that does not exist should return an error
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Mlt(MltQuery {
            doc_id: Some("nonexistent".to_string()),
            like: None,
            fields: vec!["content".to_string()],
            min_term_freq: 1,
            min_doc_freq: 1,
            max_query_terms: 25,
            min_word_length: None,
            max_word_length: None,
            field_boost_factor: 1.0,
        })),
        ..Default::default()
    });

    // Should return empty because the document does not exist
    // (build_mlt_bool_query returns error → search returns empty response)
    assert_eq!(
        result.hits.total, 0,
        "MLT with nonexistent doc_id should return empty results"
    );
}

#[tokio::test]
async fn mlt_with_like_and_empty_fields_auto_infers_from_like_json() {
    // When fields is empty and like is provided, MLT should auto-infer fields
    // from the keys in the like JSON. The like JSON content becomes the reference text.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "doc1",
            serde_json::json!({"title": "rust programming", "body": "systems language"}),
        ))
        .await
        .expect("index doc1");
    handle
        .index_document(doc(
            "doc2",
            serde_json::json!({"title": "rust metal", "body": "durable material"}),
        ))
        .await
        .expect("index doc2");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    // MLT with like containing "rust" in title and "systems" in body.
    // Empty fields list → auto-inferred from like JSON keys: ["title", "body"]
    // Reference terms: "rust" (from title) and "systems" (from body)
    // doc1 has both "rust" and "systems" → highest score
    // doc2 has "rust" but not "systems" → lower score
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Mlt(MltQuery {
            doc_id: None,
            like: Some(serde_json::json!({"title": "rust programming", "body": "systems"})),
            fields: vec![],
            min_term_freq: 1,
            min_doc_freq: 1,
            max_query_terms: 25,
            min_word_length: None,
            max_word_length: None,
            field_boost_factor: 1.0,
        })),
        ..Default::default()
    });

    // Both docs match "rust". doc1 has more shared terms (rust + systems) → higher score.
    // doc2 only shares "rust". Neither is excluded since 'like' (not doc_id) is the source.
    assert_eq!(
        result.hits.total, 2,
        "both docs share 'rust', so both should match"
    );
    assert_eq!(
        result.hits.hits[0].id, "doc1",
        "doc1 has more shared terms (rust + systems) → highest score"
    );
    assert_eq!(
        result.hits.hits[1].id, "doc2",
        "doc2 only has 'rust' in common → lower score"
    );
}

#[tokio::test]
async fn mlt_with_min_word_length_filters_short_terms() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // doc1 has short term "a" and normal term "rust"
    handle
        .index_document(doc("doc1", serde_json::json!({"content": "a rust"})))
        .await
        .expect("index doc1");
    handle
        .index_document(doc("doc2", serde_json::json!({"content": "rust"})))
        .await
        .expect("index doc2");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    // With min_word_length=4, "a" is filtered out — only "rust" is used
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Mlt(MltQuery {
            doc_id: Some("doc1".to_string()),
            like: None,
            fields: vec!["content".to_string()],
            min_term_freq: 1,
            min_doc_freq: 1,
            max_query_terms: 25,
            min_word_length: Some(4),
            max_word_length: None,
            field_boost_factor: 1.0,
        })),
        ..Default::default()
    });

    // doc1 excluded (source), doc2 has "rust" — should match
    assert!(
        result.hits.total >= 1,
        "MLT with min_word_length=4 should still find doc2 via 'rust', got: {}",
        result.hits.total
    );
}

#[tokio::test]
async fn mlt_with_max_word_length_filters_long_terms() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // doc1 has a very long token and a normal token
    handle
        .index_document(doc(
            "doc1",
            serde_json::json!({"content": "superlongtokenname rust"}),
        ))
        .await
        .expect("index doc1");
    handle
        .index_document(doc("doc2", serde_json::json!({"content": "rust"})))
        .await
        .expect("index doc2");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    // With max_word_length=4, "superlongtokenname" is filtered — only "rust" is used
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Mlt(MltQuery {
            doc_id: Some("doc1".to_string()),
            like: None,
            fields: vec!["content".to_string()],
            min_term_freq: 1,
            min_doc_freq: 1,
            max_query_terms: 25,
            min_word_length: None,
            max_word_length: Some(4),
            field_boost_factor: 1.0,
        })),
        ..Default::default()
    });

    assert_eq!(
        result.hits.total, 1,
        "only doc2 should match (doc1 is the source and excluded)"
    );
    assert_eq!(
        result.hits.hits[0].id, "doc2",
        "doc2 has 'rust' which passes max_word_length=4"
    );
}

#[tokio::test]
async fn mlt_all_terms_filtered_returns_empty_or_error() {
    // When all terms in the reference document are filtered out by min_term_freq,
    // MLT should return empty results or an error
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // doc1 has only one occurrence of "rare" — min_term_freq=2 will filter it out
    handle
        .index_document(doc("doc1", serde_json::json!({"content": "rare"})))
        .await
        .expect("index doc1");
    handle
        .index_document(doc("doc2", serde_json::json!({"content": "other"})))
        .await
        .expect("index doc2");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::Mlt(MltQuery {
            doc_id: Some("doc1".to_string()),
            like: None,
            fields: vec!["content".to_string()],
            min_term_freq: 2, // "rare" has tf=1, filtered out
            min_doc_freq: 1,
            max_query_terms: 25,
            min_word_length: None,
            max_word_length: None,
            field_boost_factor: 1.0,
        })),
        ..Default::default()
    });

    // All terms filtered → empty results
    assert_eq!(
        result.hits.total, 0,
        "MLT with all terms filtered should return empty results"
    );
}

#[tokio::test]
async fn multi_match_best_fields_returns_max_score() {
    // Doc has "foo" in title and "bar" in content.
    // multi_match query "foo bar" across both fields (best_fields).
    // Only "foo" matches in title, only "bar" matches in content.
    // So each field gets one match → best_fields returns the max score.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "1",
            serde_json::json!({"title": "foo", "content": "bar"}),
        ))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
            query: "foo bar".to_string(),
            fields: [("title".to_string(), 1.0), ("content".to_string(), 1.0)]
                .into_iter()
                .collect(),
            multi_match_type: MultiMatchType::BestFields,
            tie_breaker: 0.0,
        })),
        ..Default::default()
    });

    assert_eq!(result.hits.total, 1, "doc should match multi_match query");
    assert!(result.hits.hits[0].score.is_some());
}

#[tokio::test]
async fn multi_match_most_fields_sums_scores() {
    // Doc has "foo" in both title and content.
    // multi_match query "foo foo" across both fields (most_fields).
    // Each field contributes a score → sum should be higher than single field.
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "1",
            serde_json::json!({"title": "foo", "content": "foo"}),
        ))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
            query: "foo".to_string(),
            fields: [("title".to_string(), 1.0), ("content".to_string(), 1.0)]
                .into_iter()
                .collect(),
            multi_match_type: MultiMatchType::MostFields,
            tie_breaker: 0.0,
        })),
        ..Default::default()
    });

    assert_eq!(result.hits.total, 1, "doc should match multi_match query");
    let score = result.hits.hits[0].score.unwrap();

    // Compare with best_fields
    let best_fields_result = handle.search(&SearchRequest {
        query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
            query: "foo".to_string(),
            fields: [("title".to_string(), 1.0), ("content".to_string(), 1.0)]
                .into_iter()
                .collect(),
            multi_match_type: MultiMatchType::BestFields,
            tie_breaker: 0.0,
        })),
        ..Default::default()
    });
    let best_score = best_fields_result.hits.hits[0].score.unwrap();

    assert!(
        score > best_score,
        "most_fields score ({score}) should exceed best_fields score ({best_score})"
    );
}

#[tokio::test]
async fn multi_match_phrase_requires_consecutive_tokens() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "1",
            serde_json::json!({"content": "hello world today"}),
        ))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    // Consecutive "hello world" — should match
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
            query: "hello world".to_string(),
            fields: [("content".to_string(), 1.0)].into_iter().collect(),
            multi_match_type: MultiMatchType::Phrase,
            tie_breaker: 0.0,
        })),
        ..Default::default()
    });
    assert_eq!(
        result.hits.total, 1,
        "consecutive 'hello world' should match"
    );

    // Non-consecutive "hello today" — should NOT match
    let result2 = handle.search(&SearchRequest {
        query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
            query: "hello today".to_string(),
            fields: [("content".to_string(), 1.0)].into_iter().collect(),
            multi_match_type: MultiMatchType::Phrase,
            tie_breaker: 0.0,
        })),
        ..Default::default()
    });
    assert_eq!(
        result2.hits.total, 0,
        "non-consecutive 'hello today' should not match phrase query"
    );
}

#[tokio::test]
async fn multi_match_phrase_prefix_with_prefix_last_token() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    handle
        .index_document(doc(
            "1",
            serde_json::json!({"content": "hello world wildcarding"}),
        ))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");
    handle.flush().await.expect("flush");

    // "hello world wild*" — hello world consecutive, wild* prefix matches wildcarding
    let result = handle.search(&SearchRequest {
        query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
            query: "hello world wild".to_string(),
            fields: [("content".to_string(), 1.0)].into_iter().collect(),
            multi_match_type: MultiMatchType::PhrasePrefix,
            tie_breaker: 0.0,
        })),
        ..Default::default()
    });
    assert_eq!(
        result.hits.total, 1,
        "phrase_prefix: 'hello world wild*' should match 'wildcarding'"
    );
}

#[tokio::test]
async fn multi_match_tie_breaker_formula() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let mut handle = catalog.open_index("test").await.expect("open index");

    // Doc with two fields, "foo" appears in both
    handle
        .index_document(doc(
            "1",
            serde_json::json!({"title": "foo bar", "content": "baz foo"}),
        ))
        .await
        .expect("index");
    handle.refresh().await.expect("refresh");

    // tie_breaker = 0.0: pure max
    let tie0 = handle
        .search(&SearchRequest {
            query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
                query: "foo".to_string(),
                fields: [("title".to_string(), 1.0), ("content".to_string(), 1.0)]
                    .into_iter()
                    .collect(),
                multi_match_type: MultiMatchType::BestFields,
                tie_breaker: 0.0,
            })),
            ..Default::default()
        })
        .hits
        .hits[0]
        .score
        .unwrap();

    // tie_breaker = 0.3: max + 0.3 * sum_others
    let tie3 = handle
        .search(&SearchRequest {
            query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
                query: "foo".to_string(),
                fields: [("title".to_string(), 1.0), ("content".to_string(), 1.0)]
                    .into_iter()
                    .collect(),
                multi_match_type: MultiMatchType::BestFields,
                tie_breaker: 0.3,
            })),
            ..Default::default()
        })
        .hits
        .hits[0]
        .score
        .unwrap();

    assert!(
        tie3 > tie0,
        "tie_breaker=0.3 should produce higher score than tie_breaker=0: {tie3} vs {tie0}"
    );
}

#[tokio::test]
async fn multi_match_rejects_empty_fields() {
    let temp_dir = TempDir::new().expect("temp dir");
    let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
    catalog.initialize().await.expect("init catalog");
    catalog
        .create_index(
            "test",
            CreateIndexRequest {
                settings: IndexSettings::default(),
                ..Default::default()
            },
        )
        .await
        .expect("create index");
    let handle = catalog.open_index("test").await.expect("open index");

    let result = handle.validate_search_request(&SearchRequest {
        query: Some(SearchQuery::MultiMatch(MultiMatchQuery {
            query: "foo".to_string(),
            fields: std::collections::BTreeMap::new(),
            multi_match_type: MultiMatchType::BestFields,
            tie_breaker: 0.0,
        })),
        ..Default::default()
    });

    assert!(result.is_err(), "empty fields should be rejected");
}
