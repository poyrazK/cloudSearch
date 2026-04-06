use chrono::{DateTime, Timelike, Utc};
use cloudsearch_common::{
    AggregationRequest, AggregationResult, BoolQuery, BulkItem, BulkItemResult, BulkOperation,
    BulkRequest, BulkResponse, CloudSearchError, CreateIndexRequest,
    DateHistogramAggregationResult, DateHistogramBucket, DateHistogramInterval, FieldMapping,
    FieldType, FlushResponse, HitsMetadata, IndexDocument, IndexMetadata, MappingMode, RangeQuery,
    Result, SearchHit, SearchQuery, SearchRequest, SearchResponse, SortOrder, SortSpec,
    StatsAggregationResult, TermsAggregationResult, TermsBucket, TermsQuery,
};
use cloudsearch_storage::{
    SegmentSnapshot, WalManager, WalRecord, read_segment_snapshot, write_segment_snapshot,
};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs,
    sync::{Mutex, RwLock},
};

const MAX_FIELDS_PER_INDEX: usize = 1000;

#[derive(Debug, Clone)]
pub struct IndexCatalog {
    root_dir: PathBuf,
    lifecycle_lock: Arc<RwLock<()>>,
}

#[derive(Debug, Clone)]
pub struct IndexRegistry {
    catalog: Arc<IndexCatalog>,
    handles: Arc<Mutex<HashMap<String, Arc<Mutex<IndexHandle>>>>>,
    lifecycle_lock: Arc<RwLock<()>>,
}

impl IndexCatalog {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            lifecycle_lock: Arc::new(RwLock::new(())),
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        fs::create_dir_all(self.indexes_dir()).await?;
        Ok(())
    }

    pub async fn create_index(
        &self,
        name: &str,
        request: CreateIndexRequest,
    ) -> Result<IndexMetadata> {
        let _guard = self.lifecycle_lock.write().await;
        validate_index_name(name)?;

        let index_dir = self.index_dir(name);
        let metadata_path = self.metadata_path(name);

        if fs::try_exists(&index_dir).await? {
            return Err(CloudSearchError::IndexAlreadyExists(name.to_string()));
        }

        fs::create_dir_all(index_dir.join("wal")).await?;
        fs::create_dir_all(index_dir.join("segments")).await?;

        let metadata = IndexMetadata::new(name, request.settings);
        let json = serde_json::to_vec_pretty(&metadata)?;
        fs::write(metadata_path, json).await?;

        Ok(metadata)
    }

    pub async fn get_index(&self, name: &str) -> Result<IndexMetadata> {
        validate_index_name(name)?;

        let metadata_path = self.metadata_path(name);
        if !fs::try_exists(&metadata_path).await? {
            return Err(CloudSearchError::IndexNotFound(name.to_string()));
        }

        let bytes = fs::read(metadata_path).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn delete_index(&self, name: &str) -> Result<()> {
        let _guard = self.lifecycle_lock.write().await;
        validate_index_name(name)?;

        let index_dir = self.index_dir(name);
        if !fs::try_exists(&index_dir).await? {
            return Err(CloudSearchError::IndexNotFound(name.to_string()));
        }

        fs::remove_dir_all(index_dir).await?;
        Ok(())
    }

    pub async fn open_index(&self, name: &str) -> Result<IndexHandle> {
        let metadata = self.get_index(name).await?;
        let metadata_path = self.metadata_path(name);
        let segments_dir = self.index_dir(name).join("segments");
        let wal = WalManager::open(self.index_dir(name).join("wal")).await?;
        let snapshot = read_segment_snapshot(&segments_dir).await?;

        let mut searchable_documents = snapshot
            .as_ref()
            .map(|snapshot| {
                snapshot
                    .documents
                    .iter()
                    .cloned()
                    .map(|document| (document.id.clone(), document))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let mut last_sequence_number = snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_sequence_number)
            .unwrap_or(0);

        let entries = wal.replay_from(last_sequence_number).await?;

        for entry in entries {
            last_sequence_number = entry.sequence_number;

            match entry.record {
                WalRecord::IndexDocument { document } => {
                    searchable_documents.insert(document.id.clone(), document);
                }
                WalRecord::DeleteDocument { document_id } => {
                    searchable_documents.remove(&document_id);
                }
                WalRecord::MappingUpdate { .. } => {}
            }
        }

        Ok(IndexHandle {
            metadata,
            metadata_path,
            wal,
            segments_dir,
            searchable_documents,
            pending_operations: BTreeMap::new(),
            last_sequence_number,
        })
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    fn indexes_dir(&self) -> PathBuf {
        self.root_dir.join("indexes")
    }

    fn index_dir(&self, name: &str) -> PathBuf {
        self.indexes_dir().join(name)
    }

    fn metadata_path(&self, name: &str) -> PathBuf {
        self.index_dir(name).join("metadata.json")
    }
}

impl IndexRegistry {
    pub fn new(catalog: Arc<IndexCatalog>) -> Self {
        Self {
            catalog,
            handles: Arc::new(Mutex::new(HashMap::new())),
            lifecycle_lock: Arc::new(RwLock::new(())),
        }
    }

    pub async fn create_index(
        &self,
        name: &str,
        request: CreateIndexRequest,
    ) -> Result<IndexMetadata> {
        let _guard = self.lifecycle_lock.write().await;
        let metadata = self.catalog.create_index(name, request).await?;

        let handle = Arc::new(Mutex::new(self.catalog.open_index(name).await?));
        self.handles.lock().await.insert(name.to_string(), handle);

        Ok(metadata)
    }

    pub async fn get_index(&self, name: &str) -> Result<IndexMetadata> {
        self.catalog.get_index(name).await
    }

    pub async fn delete_index(&self, name: &str) -> Result<()> {
        let _guard = self.lifecycle_lock.write().await;
        self.catalog.delete_index(name).await?;
        self.handles.lock().await.remove(name);
        Ok(())
    }

    pub async fn index_handle(&self, name: &str) -> Result<Arc<Mutex<IndexHandle>>> {
        {
            let handles = self.handles.lock().await;
            if let Some(handle) = handles.get(name) {
                return Ok(handle.clone());
            }
        }

        let _guard = self.lifecycle_lock.read().await;

        {
            let handles = self.handles.lock().await;
            if let Some(handle) = handles.get(name) {
                return Ok(handle.clone());
            }
        }

        let opened = Arc::new(Mutex::new(self.catalog.open_index(name).await?));

        let mut handles = self.handles.lock().await;
        if let Some(handle) = handles.get(name) {
            return Ok(handle.clone());
        }

        handles.insert(name.to_string(), opened.clone());
        Ok(opened)
    }

    pub async fn cached_handle_count(&self) -> usize {
        self.handles.lock().await.len()
    }

    pub async fn cached_handles(&self) -> Vec<Arc<Mutex<IndexHandle>>> {
        self.handles.lock().await.values().cloned().collect()
    }
}

#[derive(Debug)]
pub struct IndexHandle {
    metadata: IndexMetadata,
    metadata_path: PathBuf,
    wal: WalManager,
    segments_dir: PathBuf,
    searchable_documents: BTreeMap<String, IndexDocument>,
    pending_operations: BTreeMap<String, PendingOperation>,
    last_sequence_number: u64,
}

#[derive(Debug, Clone)]
enum PendingOperation {
    Upsert(IndexDocument),
    Delete,
}

impl IndexHandle {
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    pub fn documents(&self) -> &BTreeMap<String, IndexDocument> {
        &self.searchable_documents
    }

    pub fn get_document(&self, document_id: &str) -> Option<&IndexDocument> {
        match self.pending_operations.get(document_id) {
            Some(PendingOperation::Upsert(document)) => Some(document),
            Some(PendingOperation::Delete) => None,
            None => self.searchable_documents.get(document_id),
        }
    }

    pub fn search(&self, request: &SearchRequest) -> SearchResponse {
        let query = request.query.as_ref().unwrap_or(&SearchQuery::MatchAll);
        let matching_documents = self
            .searchable_documents
            .values()
            .filter(|document| matches_query(document, query))
            .cloned()
            .collect::<Vec<_>>();

        let mut hits = matching_documents
            .iter()
            .map(|document| SearchHit {
                id: document.id.clone(),
                source: document.source.clone(),
            })
            .collect::<Vec<_>>();

        let total = hits.len();
        let aggregations = compute_aggregations(&matching_documents, request.aggs.as_ref());

        if let Some(sort) = &request.sort {
            hits.sort_by(|left, right| compare_hits(left, right, sort));
        }

        let from = request.from.unwrap_or(0);
        let size = request.size.unwrap_or(total);
        let hits = hits.into_iter().skip(from).take(size).collect::<Vec<_>>();

        SearchResponse {
            hits: HitsMetadata { total, hits },
            aggregations,
        }
    }

    pub fn validate_search_request(&self, request: &SearchRequest) -> Result<()> {
        if let Some(query) = &request.query {
            self.validate_query(query)?;
        }

        if let Some(sort) = &request.sort
            && let Some(mapping) = self.metadata.mappings.get(&sort.field)
            && matches!(mapping.field_type, FieldType::Object)
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{}' cannot be used for sorting",
                sort.field
            )));
        }

        if let Some(aggs) = &request.aggs {
            for (name, agg) in aggs {
                match agg {
                    AggregationRequest::Terms(terms) => {
                        self.ensure_scalar_field(&terms.field, name)?;
                    }
                    AggregationRequest::Stats(stats) => {
                        self.ensure_numeric_field(&stats.field, name)?;
                    }
                    AggregationRequest::DateHistogram(histogram) => {
                        self.ensure_timestamp_field(&histogram.field, name)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn index_document(&mut self, document: IndexDocument) -> Result<u64> {
        self.validate_and_update_mappings(&document.source).await?;

        let sequence_number = self.last_sequence_number + 1;
        self.wal
            .append(
                sequence_number,
                WalRecord::IndexDocument {
                    document: document.clone(),
                },
            )
            .await?;
        self.pending_operations
            .insert(document.id.clone(), PendingOperation::Upsert(document));
        self.last_sequence_number = sequence_number;
        Ok(sequence_number)
    }

    pub async fn delete_document(&mut self, document_id: &str) -> Result<u64> {
        let sequence_number = self.last_sequence_number + 1;
        self.wal
            .append(
                sequence_number,
                WalRecord::DeleteDocument {
                    document_id: document_id.to_string(),
                },
            )
            .await?;
        self.pending_operations
            .insert(document_id.to_string(), PendingOperation::Delete);
        self.last_sequence_number = sequence_number;
        Ok(sequence_number)
    }

    pub async fn bulk_apply(&mut self, request: BulkRequest) -> Result<BulkResponse> {
        let mut items = Vec::with_capacity(request.operations.len());

        for operation in request.operations {
            match operation {
                BulkOperation::Index(index) => {
                    let sequence_number = self
                        .index_document(IndexDocument {
                            id: index.id.clone(),
                            source: index.source,
                        })
                        .await?;

                    items.push(BulkItem::Index(BulkItemResult {
                        id: index.id,
                        result: "created".to_string(),
                        sequence_number,
                    }));
                }
                BulkOperation::Delete(delete) => {
                    let sequence_number = self.delete_document(&delete.id).await?;

                    items.push(BulkItem::Delete(BulkItemResult {
                        id: delete.id,
                        result: "deleted".to_string(),
                        sequence_number,
                    }));
                }
            }
        }

        Ok(BulkResponse {
            errors: false,
            items,
        })
    }

    pub async fn refresh(&mut self) -> Result<usize> {
        let refreshed_documents = self.pending_operations.len();

        for (document_id, operation) in std::mem::take(&mut self.pending_operations) {
            match operation {
                PendingOperation::Upsert(document) => {
                    self.searchable_documents.insert(document_id, document);
                }
                PendingOperation::Delete => {
                    self.searchable_documents.remove(&document_id);
                }
            }
        }

        self.metadata.updated_at = Utc::now();

        Ok(refreshed_documents)
    }

    pub async fn flush(&mut self) -> Result<FlushResponse> {
        let snapshot = SegmentSnapshot {
            last_sequence_number: self.last_sequence_number,
            documents: self.searchable_documents.values().cloned().collect(),
        };

        write_segment_snapshot(&self.segments_dir, &snapshot).await?;
        self.wal.rollover().await?;
        self.wal.trim_through(snapshot.last_sequence_number).await?;

        Ok(FlushResponse {
            result: "flushed",
            flushed_documents: snapshot.documents.len(),
            sequence_number: snapshot.last_sequence_number,
        })
    }

    async fn validate_and_update_mappings(&mut self, source: &serde_json::Value) -> Result<()> {
        let object = source.as_object().ok_or_else(|| {
            CloudSearchError::MappingConflict("document source must be a JSON object".to_string())
        })?;

        let mut new_mappings = Vec::new();

        for (field, value) in object {
            let Some(inferred_type) = infer_field_type(field, value)? else {
                continue;
            };

            match self.metadata.mappings.get(field) {
                Some(existing) if existing.field_type != inferred_type => {
                    return Err(CloudSearchError::MappingConflict(format!(
                        "field '{}' expected {:?} but received {:?}",
                        field, existing.field_type, inferred_type
                    )));
                }
                Some(_) => {}
                None => match self.metadata.settings.mapping_mode {
                    MappingMode::Strict => {
                        return Err(CloudSearchError::UnknownFieldRejected(field.clone()));
                    }
                    MappingMode::ControlledDynamic => {
                        new_mappings.push((
                            field.clone(),
                            FieldMapping {
                                field_type: inferred_type,
                            },
                        ));
                    }
                },
            }
        }

        if self.metadata.mappings.len() + new_mappings.len() > MAX_FIELDS_PER_INDEX {
            return Err(CloudSearchError::MappingLimitExceeded(format!(
                "index '{}' exceeded maximum field count of {}",
                self.metadata.name, MAX_FIELDS_PER_INDEX
            )));
        }

        if !new_mappings.is_empty() {
            for (field, mapping) in new_mappings {
                self.metadata.mappings.insert(field, mapping);
            }
            self.metadata.updated_at = Utc::now();
            self.persist_metadata().await?;
        }

        Ok(())
    }

    async fn persist_metadata(&self) -> Result<()> {
        let json = serde_json::to_vec_pretty(&self.metadata)?;
        fs::write(&self.metadata_path, json).await?;
        Ok(())
    }

    fn validate_query(&self, query: &SearchQuery) -> Result<()> {
        match query {
            SearchQuery::MatchAll => Ok(()),
            SearchQuery::Term(term) => self.ensure_scalar_field(&term.field, &term.field),
            SearchQuery::Terms(terms) => self.ensure_scalar_field(&terms.field, &terms.field),
            SearchQuery::Range(range) => self.ensure_range_field(&range.field),
            SearchQuery::Bool(boolean) => boolean
                .must
                .iter()
                .chain(boolean.should.iter())
                .chain(boolean.filter.iter())
                .chain(boolean.must_not.iter())
                .try_for_each(|query| self.validate_query(query)),
        }
    }

    fn ensure_scalar_field(&self, field: &str, context: &str) -> Result<()> {
        if let Some(mapping) = self.metadata.mappings.get(field)
            && matches!(mapping.field_type, FieldType::Object)
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{}' cannot be used as a scalar in '{}'",
                field, context
            )));
        }

        Ok(())
    }

    fn ensure_numeric_field(&self, field: &str, context: &str) -> Result<()> {
        if let Some(mapping) = self.metadata.mappings.get(field)
            && !matches!(
                mapping.field_type,
                FieldType::Integer | FieldType::Long | FieldType::Double
            )
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{}' is not numeric for '{}'",
                field, context
            )));
        }

        Ok(())
    }

    fn ensure_timestamp_field(&self, field: &str, context: &str) -> Result<()> {
        if let Some(mapping) = self.metadata.mappings.get(field)
            && mapping.field_type != FieldType::Timestamp
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{}' is not a timestamp for '{}'",
                field, context
            )));
        }

        Ok(())
    }

    fn ensure_range_field(&self, field: &str) -> Result<()> {
        if let Some(mapping) = self.metadata.mappings.get(field)
            && !matches!(
                mapping.field_type,
                FieldType::Integer | FieldType::Long | FieldType::Double | FieldType::Timestamp
            )
        {
            return Err(CloudSearchError::InvalidSearchRequest(format!(
                "field '{}' does not support range queries",
                field
            )));
        }

        Ok(())
    }
}

fn infer_field_type(field: &str, value: &serde_json::Value) -> Result<Option<FieldType>> {
    Ok(match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(_) => Some(FieldType::Boolean),
        serde_json::Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                if i32::try_from(integer).is_ok() {
                    Some(FieldType::Integer)
                } else {
                    Some(FieldType::Long)
                }
            } else if let Some(integer) = number.as_u64() {
                if i32::try_from(integer).is_ok() {
                    Some(FieldType::Integer)
                } else if i64::try_from(integer).is_ok() {
                    Some(FieldType::Long)
                } else {
                    Some(FieldType::Double)
                }
            } else {
                Some(FieldType::Double)
            }
        }
        serde_json::Value::String(raw) => {
            if DateTime::parse_from_rfc3339(raw).is_ok() {
                Some(FieldType::Timestamp)
            } else {
                Some(FieldType::Keyword)
            }
        }
        serde_json::Value::Array(_) => {
            return Err(CloudSearchError::UnsupportedArrayField(field.to_string()));
        }
        serde_json::Value::Object(_) => Some(FieldType::Object),
    })
}

fn matches_query(document: &IndexDocument, query: &SearchQuery) -> bool {
    match query {
        SearchQuery::MatchAll => true,
        SearchQuery::Term(term) => document
            .source
            .get(&term.field)
            .is_some_and(|value| value == &term.value),
        SearchQuery::Terms(terms) => matches_terms_query(document, terms),
        SearchQuery::Range(range) => matches_range_query(document, range),
        SearchQuery::Bool(bool_query) => matches_bool_query(document, bool_query),
    }
}

fn matches_bool_query(document: &IndexDocument, bool_query: &BoolQuery) -> bool {
    let must_matches = bool_query
        .must
        .iter()
        .all(|query| matches_query(document, query));
    let filter_matches = bool_query
        .filter
        .iter()
        .all(|query| matches_query(document, query));
    let must_not_matches = bool_query
        .must_not
        .iter()
        .all(|query| !matches_query(document, query));
    let should_matches = bool_query
        .should
        .iter()
        .any(|query| matches_query(document, query));
    let should_required =
        bool_query.must.is_empty() && bool_query.filter.is_empty() && !bool_query.should.is_empty();

    must_matches && filter_matches && must_not_matches && (!should_required || should_matches)
}

fn matches_terms_query(document: &IndexDocument, terms: &TermsQuery) -> bool {
    document
        .source
        .get(&terms.field)
        .is_some_and(|value| terms.values.iter().any(|candidate| candidate == value))
}

fn compute_aggregations(
    documents: &[IndexDocument],
    requests: Option<&BTreeMap<String, AggregationRequest>>,
) -> BTreeMap<String, AggregationResult> {
    let mut aggregations = BTreeMap::new();

    let Some(requests) = requests else {
        return aggregations;
    };

    for (name, request) in requests {
        let result = match request {
            AggregationRequest::Terms(terms) => {
                AggregationResult::Terms(compute_terms_aggregation(documents, &terms.field))
            }
            AggregationRequest::Stats(stats) => {
                AggregationResult::Stats(compute_stats_aggregation(documents, &stats.field))
            }
            AggregationRequest::DateHistogram(histogram) => {
                AggregationResult::DateHistogram(compute_date_histogram_aggregation(
                    documents,
                    &histogram.field,
                    &histogram.interval,
                ))
            }
        };

        aggregations.insert(name.clone(), result);
    }

    aggregations
}

fn compute_terms_aggregation(documents: &[IndexDocument], field: &str) -> TermsAggregationResult {
    let mut counts: HashMap<String, (serde_json::Value, usize)> = HashMap::new();

    for document in documents {
        let Some(value) = document.source.get(field) else {
            continue;
        };

        if !matches!(
            value,
            serde_json::Value::String(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Bool(_)
        ) {
            continue;
        }

        let key = value.to_string();
        let entry = counts.entry(key).or_insert_with(|| (value.clone(), 0));
        entry.1 += 1;
    }

    let mut buckets = counts
        .into_values()
        .map(|(key, doc_count)| TermsBucket { key, doc_count })
        .collect::<Vec<_>>();

    buckets.sort_by(|left, right| {
        right
            .doc_count
            .cmp(&left.doc_count)
            .then_with(|| left.key.to_string().cmp(&right.key.to_string()))
    });

    TermsAggregationResult { buckets }
}

fn compute_stats_aggregation(documents: &[IndexDocument], field: &str) -> StatsAggregationResult {
    let values = documents
        .iter()
        .filter_map(|document| document.source.get(field))
        .filter_map(serde_json::Value::as_f64)
        .collect::<Vec<_>>();

    let count = values.len();
    let sum = values.iter().sum::<f64>();
    let min = values.iter().copied().reduce(f64::min);
    let max = values.iter().copied().reduce(f64::max);
    let avg = if count > 0 {
        Some(sum / count as f64)
    } else {
        None
    };

    StatsAggregationResult {
        count,
        min,
        max,
        avg,
        sum,
    }
}

fn compute_date_histogram_aggregation(
    documents: &[IndexDocument],
    field: &str,
    interval: &DateHistogramInterval,
) -> DateHistogramAggregationResult {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();

    for document in documents {
        let Some(raw) = document
            .source
            .get(field)
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };

        let Ok(timestamp) = DateTime::parse_from_rfc3339(raw) else {
            continue;
        };

        let bucket_key = truncate_timestamp(timestamp.with_timezone(&Utc), interval)
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        *counts.entry(bucket_key).or_default() += 1;
    }

    DateHistogramAggregationResult {
        buckets: counts
            .into_iter()
            .map(|(key, doc_count)| DateHistogramBucket { key, doc_count })
            .collect(),
    }
}

fn truncate_timestamp(timestamp: DateTime<Utc>, interval: &DateHistogramInterval) -> DateTime<Utc> {
    match interval {
        DateHistogramInterval::Minute => timestamp
            .with_second(0)
            .and_then(|ts| ts.with_nanosecond(0))
            .expect("valid minute truncation"),
        DateHistogramInterval::Hour => timestamp
            .with_minute(0)
            .and_then(|ts| ts.with_second(0))
            .and_then(|ts| ts.with_nanosecond(0))
            .expect("valid hour truncation"),
        DateHistogramInterval::Day => timestamp
            .with_hour(0)
            .and_then(|ts| ts.with_minute(0))
            .and_then(|ts| ts.with_second(0))
            .and_then(|ts| ts.with_nanosecond(0))
            .expect("valid day truncation"),
    }
}

fn matches_range_query(document: &IndexDocument, range: &RangeQuery) -> bool {
    let Some(value) = document.source.get(&range.field) else {
        return false;
    };

    match comparable_value(value) {
        Some(ComparableValue::Number(number)) => matches_numeric_range(number, range),
        Some(ComparableValue::Timestamp(timestamp)) => matches_timestamp_range(timestamp, range),
        Some(ComparableValue::String(_)) | Some(ComparableValue::Boolean(_)) => false,
        None => false,
    }
}

enum ComparableValue {
    Number(f64),
    Timestamp(DateTime<Utc>),
    String(String),
    Boolean(bool),
}

fn comparable_value(value: &serde_json::Value) -> Option<ComparableValue> {
    if let Some(number) = value.as_f64() {
        return Some(ComparableValue::Number(number));
    }

    if let Some(boolean) = value.as_bool() {
        return Some(ComparableValue::Boolean(boolean));
    }

    if let Some(raw) = value.as_str() {
        if let Ok(timestamp) = DateTime::parse_from_rfc3339(raw) {
            return Some(ComparableValue::Timestamp(timestamp.with_timezone(&Utc)));
        }

        return Some(ComparableValue::String(raw.to_string()));
    }

    None
}

fn compare_hits(left: &SearchHit, right: &SearchHit, sort: &SortSpec) -> std::cmp::Ordering {
    let left_value = left.source.get(&sort.field).and_then(comparable_value);
    let right_value = right.source.get(&sort.field).and_then(comparable_value);

    let ordering = match (left_value, right_value) {
        (Some(left), Some(right)) => compare_sort_values(&left, &right, sort),
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, None) => left.id.cmp(&right.id),
    };

    if ordering == std::cmp::Ordering::Equal {
        left.id.cmp(&right.id)
    } else {
        ordering
    }
}

fn compare_sort_values(
    left: &ComparableValue,
    right: &ComparableValue,
    sort: &SortSpec,
) -> std::cmp::Ordering {
    let ordering = match (left, right) {
        (ComparableValue::Number(left), ComparableValue::Number(right)) => left.total_cmp(right),
        (ComparableValue::Timestamp(left), ComparableValue::Timestamp(right)) => left.cmp(right),
        (ComparableValue::String(left), ComparableValue::String(right)) => left.cmp(right),
        (ComparableValue::Boolean(left), ComparableValue::Boolean(right)) => left.cmp(right),
        _ => std::cmp::Ordering::Equal,
    };

    match sort.order {
        SortOrder::Asc => ordering,
        SortOrder::Desc => ordering.reverse(),
    }
}

fn matches_numeric_range(number: f64, range: &RangeQuery) -> bool {
    compare_numeric_bound(number, range.gte.as_ref(), |lhs, rhs| lhs >= rhs)
        && compare_numeric_bound(number, range.gt.as_ref(), |lhs, rhs| lhs > rhs)
        && compare_numeric_bound(number, range.lte.as_ref(), |lhs, rhs| lhs <= rhs)
        && compare_numeric_bound(number, range.lt.as_ref(), |lhs, rhs| lhs < rhs)
}

fn compare_numeric_bound(
    number: f64,
    bound: Option<&serde_json::Value>,
    predicate: impl Fn(f64, f64) -> bool,
) -> bool {
    match bound {
        Some(value) => value.as_f64().is_some_and(|bound| predicate(number, bound)),
        None => true,
    }
}

fn matches_timestamp_range(timestamp: DateTime<Utc>, range: &RangeQuery) -> bool {
    compare_timestamp_bound(timestamp, range.gte.as_ref(), |lhs, rhs| lhs >= rhs)
        && compare_timestamp_bound(timestamp, range.gt.as_ref(), |lhs, rhs| lhs > rhs)
        && compare_timestamp_bound(timestamp, range.lte.as_ref(), |lhs, rhs| lhs <= rhs)
        && compare_timestamp_bound(timestamp, range.lt.as_ref(), |lhs, rhs| lhs < rhs)
}

fn compare_timestamp_bound(
    timestamp: DateTime<Utc>,
    bound: Option<&serde_json::Value>,
    predicate: impl Fn(DateTime<Utc>, DateTime<Utc>) -> bool,
) -> bool {
    match bound {
        Some(value) => value
            .as_str()
            .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
            .map(|parsed| parsed.with_timezone(&Utc))
            .is_some_and(|bound| predicate(timestamp, bound)),
        None => true,
    }
}

fn validate_index_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 255
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');

    if valid {
        Ok(())
    } else {
        Err(CloudSearchError::InvalidIndexName(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudsearch_common::{
        AggregationRequest, AggregationResult, BoolQuery, BulkDeleteOperation, BulkIndexOperation,
        BulkOperation, BulkRequest, CreateIndexRequest, DateHistogramAggregationRequest,
        DateHistogramInterval, FieldType, IndexSettings, MappingMode, RangeQuery, SearchQuery,
        SearchRequest, SortOrder, SortSpec, StatsAggregationRequest, TermQuery,
        TermsAggregationRequest, TermsQuery,
    };
    use tempfile::TempDir;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn creates_and_loads_index_metadata() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let metadata = catalog
            .create_index(
                "logs_v1",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::ControlledDynamic,
                        primary_time_field: Some("@timestamp".to_string()),
                    },
                },
            )
            .await
            .expect("create index");

        let loaded = catalog.get_index("logs_v1").await.expect("load index");

        assert_eq!(loaded.name, metadata.name);
        assert_eq!(
            loaded.settings.primary_time_field.as_deref(),
            Some("@timestamp")
        );
    }

    #[tokio::test]
    async fn rejects_duplicate_index_creation() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let error = catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect_err("duplicate should fail");

        assert!(matches!(error, CloudSearchError::IndexAlreadyExists(_)));
    }

    #[tokio::test]
    async fn delete_index_removes_storage_and_allows_recreate() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let index_dir = temp_dir.path().join("indexes").join("logs");
        assert!(index_dir.exists());

        catalog.delete_index("logs").await.expect("delete index");
        assert!(!index_dir.exists());

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("recreate index");

        assert!(index_dir.exists());
    }

    #[tokio::test]
    async fn delete_missing_index_returns_not_found() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        let error = catalog
            .delete_index("missing")
            .await
            .expect_err("missing index should fail");

        assert!(matches!(error, CloudSearchError::IndexNotFound(_)));
    }

    #[tokio::test]
    async fn registry_reuses_and_evicted_handles_correctly() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = Arc::new(IndexCatalog::new(temp_dir.path()));
        catalog.initialize().await.expect("init catalog");
        let registry = Arc::new(IndexRegistry::new(catalog));

        registry
            .catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let barrier = Arc::new(Barrier::new(2));
        let first_task = {
            let registry = registry.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.index_handle("logs").await.expect("first handle")
            })
        };
        let second_task = {
            let registry = registry.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.index_handle("logs").await.expect("second handle")
            })
        };

        let first = first_task.await.expect("join first task");
        let second = second_task.await.expect("join second task");
        assert!(Arc::ptr_eq(&first, &second));

        registry.delete_index("logs").await.expect("delete index");

        registry
            .catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("recreate index");

        let barrier = Arc::new(Barrier::new(2));
        let third_task = {
            let registry = registry.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.index_handle("logs").await.expect("third handle")
            })
        };
        let fourth_task = {
            let registry = registry.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                registry.index_handle("logs").await.expect("fourth handle")
            })
        };

        let third = third_task.await.expect("join third task");
        let fourth = fourth_task.await.expect("join fourth task");
        assert!(Arc::ptr_eq(&third, &fourth));
        assert!(!Arc::ptr_eq(&first, &third));
    }

    #[tokio::test]
    async fn infers_and_persists_mappings_across_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({
                    "service": "billing",
                    "latency": 42,
                    "active": true,
                    "timestamp": "2026-03-14T10:00:00Z"
                }),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(
            reopened.metadata().mappings["service"].field_type,
            FieldType::Keyword
        );
        assert_eq!(
            reopened.metadata().mappings["latency"].field_type,
            FieldType::Integer
        );
        assert_eq!(
            reopened.metadata().mappings["active"].field_type,
            FieldType::Boolean
        );
        assert_eq!(
            reopened.metadata().mappings["timestamp"].field_type,
            FieldType::Timestamp
        );
    }

    #[tokio::test]
    async fn strict_mode_rejects_unknown_fields_and_arrays() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: IndexSettings {
                        mapping_mode: MappingMode::Strict,
                        primary_time_field: None,
                    },
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");

        let unknown = handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing"}),
            })
            .await
            .expect_err("strict mode should reject unknown field");
        assert!(matches!(unknown, CloudSearchError::UnknownFieldRejected(_)));

        let array = handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"services": ["billing", "search"]}),
            })
            .await
            .expect_err("arrays should be rejected");
        assert!(matches!(array, CloudSearchError::UnsupportedArrayField(_)));
    }

    #[tokio::test]
    async fn rejects_mapping_conflicts_and_invalid_query_usage() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "meta": {"host": "a"}}),
            })
            .await
            .expect("index doc");

        let conflict = handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"meta": "not-an-object"}),
            })
            .await
            .expect_err("conflict should fail");
        assert!(matches!(conflict, CloudSearchError::MappingConflict(_)));

        let invalid_query = handle.validate_search_request(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "service".to_string(),
                gte: Some(serde_json::json!("a")),
                gt: None,
                lte: None,
                lt: None,
            })),
            ..Default::default()
        });
        assert!(matches!(
            invalid_query,
            Err(CloudSearchError::InvalidSearchRequest(_))
        ));
    }

    #[tokio::test]
    async fn replays_documents_from_wal() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "world"}),
            })
            .await
            .expect("index doc");
        handle.delete_document("doc-1").await.expect("delete doc");

        let recovered = catalog.open_index("logs").await.expect("recover index");

        assert_eq!(recovered.documents().len(), 1);
        assert!(recovered.documents().contains_key("doc-2"));
        assert!(!recovered.documents().contains_key("doc-1"));
    }

    #[tokio::test]
    async fn gets_document_by_id_after_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-42".to_string(),
                source: serde_json::json!({"message": "persist me"}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        let document = reopened.get_document("doc-42").expect("document exists");

        assert_eq!(document.id, "doc-42");
        assert_eq!(document.source["message"], "persist me");
        assert!(reopened.get_document("missing").is_none());
    }

    #[tokio::test]
    async fn writes_are_searchable_only_after_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing"}),
            })
            .await
            .expect("index doc");

        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 0);
        assert!(handle.get_document("doc-1").is_some());

        let refreshed = handle.refresh().await.expect("refresh");
        assert_eq!(refreshed, 1);
        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 1);
    }

    #[tokio::test]
    async fn delete_becomes_search_visible_after_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");
        handle.delete_document("doc-1").await.expect("delete doc");

        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 1);

        handle.refresh().await.expect("refresh");
        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 0);
    }

    #[tokio::test]
    async fn flush_persists_searchable_state_but_not_pending_writes() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "visible"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");
        handle.flush().await.expect("flush");

        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "pending"}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(reopened.search(&SearchRequest::default()).hits.total, 2);
        assert_eq!(
            reopened.get_document("doc-1").unwrap().source["message"],
            "visible"
        );
        assert_eq!(
            reopened.get_document("doc-2").unwrap().source["message"],
            "pending"
        );
    }

    #[tokio::test]
    async fn flush_response_reports_sequence_and_document_count() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        let flushed = handle.flush().await.expect("flush");

        assert_eq!(flushed.result, "flushed");
        assert_eq!(flushed.flushed_documents, 1);
        assert_eq!(flushed.sequence_number, 1);
    }

    #[tokio::test]
    async fn flush_rolls_over_and_trims_covered_wal_generations() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");
        handle.flush().await.expect("flush");

        let wal_dir = temp_dir.path().join("indexes").join("logs").join("wal");
        assert!(!wal_dir.join("000001.log").exists());

        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "tail"}),
            })
            .await
            .expect("index doc");

        assert!(wal_dir.join("000002.log").exists());
    }

    #[tokio::test]
    async fn searches_match_all_and_term_queries_after_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "level": "info"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "search", "level": "info"}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");

        let match_all = reopened.search(&SearchRequest::default());
        assert_eq!(match_all.hits.total, 2);

        let term = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "service".to_string(),
                value: serde_json::json!("billing"),
            })),
            ..Default::default()
        });
        assert_eq!(term.hits.total, 1);
        assert_eq!(term.hits.hits[0].id, "doc-1");

        let filtered = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![SearchQuery::Term(TermQuery {
                    field: "level".to_string(),
                    value: serde_json::json!("info"),
                })],
                ..Default::default()
            })),
            ..Default::default()
        });
        assert_eq!(filtered.hits.total, 2);
    }

    #[tokio::test]
    async fn latest_document_version_wins_after_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "first"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "second"}),
            })
            .await
            .expect("overwrite doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        let document = reopened.get_document("doc-1").expect("document exists");

        assert_eq!(document.source["message"], "second");
    }

    #[tokio::test]
    async fn search_handles_empty_indexes_missing_fields_and_multiple_filters() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let empty = catalog.open_index("logs").await.expect("open empty index");
        assert_eq!(empty.search(&SearchRequest::default()).hits.total, 0);

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "level": "info", "active": true}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"service": "billing", "level": "error", "active": false}),
            })
            .await
            .expect("index doc");
        handle.delete_document("doc-2").await.expect("delete doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");

        let missing_field = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "missing".to_string(),
                value: serde_json::json!("nope"),
            })),
            ..Default::default()
        });
        assert_eq!(missing_field.hits.total, 0);

        let multiple_filters = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![
                    SearchQuery::Term(TermQuery {
                        field: "service".to_string(),
                        value: serde_json::json!("billing"),
                    }),
                    SearchQuery::Term(TermQuery {
                        field: "active".to_string(),
                        value: serde_json::json!(true),
                    }),
                ],
                ..Default::default()
            })),
            ..Default::default()
        });
        assert_eq!(multiple_filters.hits.total, 1);
        assert_eq!(multiple_filters.hits.hits[0].id, "doc-1");
    }

    #[tokio::test]
    async fn repeated_reopen_preserves_overwrite_and_delete_sequence() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut first = catalog.open_index("logs").await.expect("open index");
        first
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "first"}),
            })
            .await
            .expect("index doc");

        let mut second = catalog.open_index("logs").await.expect("reopen index");
        second
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "second"}),
            })
            .await
            .expect("overwrite doc");
        second.delete_document("doc-1").await.expect("delete doc");
        second
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "survivor"}),
            })
            .await
            .expect("index doc");

        let third = catalog.open_index("logs").await.expect("reopen index");
        assert!(third.get_document("doc-1").is_none());
        assert_eq!(third.search(&SearchRequest::default()).hits.total, 1);
        assert_eq!(
            third.get_document("doc-2").unwrap().source["message"],
            "survivor"
        );
    }

    #[tokio::test]
    async fn deleting_missing_document_is_safe_across_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        let sequence = handle
            .delete_document("missing-doc")
            .await
            .expect("delete missing doc");
        assert_eq!(sequence, 1);

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(reopened.search(&SearchRequest::default()).hits.total, 0);
        assert!(reopened.get_document("missing-doc").is_none());
    }

    #[tokio::test]
    async fn boolean_and_numeric_term_queries_match_exact_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"active": true, "latency": 42}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");

        let bool_query = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "active".to_string(),
                value: serde_json::json!(true),
            })),
            ..Default::default()
        });
        assert_eq!(bool_query.hits.total, 1);

        let numeric_query = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "latency".to_string(),
                value: serde_json::json!(42),
            })),
            ..Default::default()
        });
        assert_eq!(numeric_query.hits.total, 1);

        let wrong_type = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "latency".to_string(),
                value: serde_json::json!("42"),
            })),
            ..Default::default()
        });
        assert_eq!(wrong_type.hits.total, 0);
    }

    #[tokio::test]
    async fn range_queries_match_numeric_and_timestamp_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({
                    "latency": 42,
                    "timestamp": "2026-03-14T10:00:00Z"
                }),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({
                    "latency": 7,
                    "timestamp": "2026-03-14T12:00:00Z"
                }),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        let numeric = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "latency".to_string(),
                gte: Some(serde_json::json!(10)),
                gt: None,
                lte: Some(serde_json::json!(50)),
                lt: None,
            })),
            ..Default::default()
        });
        assert_eq!(numeric.hits.total, 1);
        assert_eq!(numeric.hits.hits[0].id, "doc-1");

        let timestamp = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "timestamp".to_string(),
                gte: Some(serde_json::json!("2026-03-14T11:00:00Z")),
                gt: None,
                lte: None,
                lt: Some(serde_json::json!("2026-03-14T13:00:00Z")),
            })),
            ..Default::default()
        });
        assert_eq!(timestamp.hits.total, 1);
        assert_eq!(timestamp.hits.hits[0].id, "doc-2");

        let missing = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "missing".to_string(),
                gte: Some(serde_json::json!(1)),
                gt: None,
                lte: None,
                lt: None,
            })),
            ..Default::default()
        });
        assert_eq!(missing.hits.total, 0);
    }

    #[tokio::test]
    async fn deleted_documents_do_not_appear_in_match_all_after_reopen() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "remove me"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "keep me"}),
            })
            .await
            .expect("index doc");
        handle.delete_document("doc-1").await.expect("delete doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        let all = reopened.search(&SearchRequest::default());

        assert_eq!(all.hits.total, 1);
        assert_eq!(all.hits.hits[0].id, "doc-2");
    }

    #[tokio::test]
    async fn bool_filter_with_no_clauses_matches_all_documents() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "a"}),
            })
            .await
            .expect("index doc");
        handle
            .index_document(IndexDocument {
                id: "doc-2".to_string(),
                source: serde_json::json!({"message": "b"}),
            })
            .await
            .expect("index doc");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        let result = reopened.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![],
                ..Default::default()
            })),
            ..Default::default()
        });

        assert_eq!(result.hits.total, 2);
    }

    #[tokio::test]
    async fn expanded_bool_query_supports_must_should_and_must_not() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            (
                "doc-1",
                serde_json::json!({"service": "billing", "level": "info"}),
            ),
            (
                "doc-2",
                serde_json::json!({"service": "billing", "level": "error"}),
            ),
            (
                "doc-3",
                serde_json::json!({"service": "search", "level": "info"}),
            ),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let must = handle.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                must: vec![SearchQuery::Term(TermQuery {
                    field: "service".to_string(),
                    value: serde_json::json!("billing"),
                })],
                ..Default::default()
            })),
            ..Default::default()
        });
        assert_eq!(must.hits.total, 2);

        let should = handle.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                should: vec![
                    SearchQuery::Term(TermQuery {
                        field: "service".to_string(),
                        value: serde_json::json!("billing"),
                    }),
                    SearchQuery::Term(TermQuery {
                        field: "service".to_string(),
                        value: serde_json::json!("search"),
                    }),
                ],
                ..Default::default()
            })),
            ..Default::default()
        });
        assert_eq!(should.hits.total, 3);

        let must_not = handle.search(&SearchRequest {
            query: Some(SearchQuery::Bool(BoolQuery {
                filter: vec![SearchQuery::Term(TermQuery {
                    field: "service".to_string(),
                    value: serde_json::json!("billing"),
                })],
                must_not: vec![SearchQuery::Term(TermQuery {
                    field: "level".to_string(),
                    value: serde_json::json!("error"),
                })],
                ..Default::default()
            })),
            ..Default::default()
        });
        assert_eq!(must_not.hits.total, 1);
        assert_eq!(must_not.hits.hits[0].id, "doc-1");
    }

    #[tokio::test]
    async fn bulk_apply_orders_updates_and_requires_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        let response = handle
            .bulk_apply(BulkRequest {
                operations: vec![
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "first"}),
                    }),
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"message": "second"}),
                    }),
                    BulkOperation::Delete(BulkDeleteOperation {
                        id: "doc-2".to_string(),
                    }),
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-2".to_string(),
                        source: serde_json::json!({"message": "survivor"}),
                    }),
                ],
            })
            .await
            .expect("bulk apply");

        assert!(!response.errors);
        assert_eq!(response.items.len(), 4);
        assert_eq!(handle.search(&SearchRequest::default()).hits.total, 0);

        handle.refresh().await.expect("refresh");

        let all = handle.search(&SearchRequest::default());
        assert_eq!(all.hits.total, 2);
        assert_eq!(
            handle.get_document("doc-1").unwrap().source["message"],
            "second"
        );
        assert_eq!(
            handle.get_document("doc-2").unwrap().source["message"],
            "survivor"
        );
    }

    #[tokio::test]
    async fn bulk_state_survives_reopen_after_refresh() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .bulk_apply(BulkRequest {
                operations: vec![
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-1".to_string(),
                        source: serde_json::json!({"service": "billing"}),
                    }),
                    BulkOperation::Index(BulkIndexOperation {
                        id: "doc-2".to_string(),
                        source: serde_json::json!({"service": "search"}),
                    }),
                    BulkOperation::Delete(BulkDeleteOperation {
                        id: "doc-1".to_string(),
                    }),
                ],
            })
            .await
            .expect("bulk apply");
        handle.refresh().await.expect("refresh");

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(reopened.search(&SearchRequest::default()).hits.total, 1);
        assert!(reopened.get_document("doc-1").is_none());
        assert_eq!(
            reopened.get_document("doc-2").unwrap().source["service"],
            "search"
        );
    }

    #[tokio::test]
    async fn terms_query_matches_multiple_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, service) in [("doc-1", "billing"), ("doc-2", "search"), ("doc-3", "auth")] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source: serde_json::json!({"service": service}),
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let response = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "service".to_string(),
                values: vec![serde_json::json!("billing"), serde_json::json!("auth")],
            })),
            ..Default::default()
        });

        assert_eq!(response.hits.total, 2);
        assert_eq!(response.hits.hits[0].id, "doc-1");
        assert_eq!(response.hits.hits[1].id, "doc-3");
    }

    #[tokio::test]
    async fn terms_query_handles_empty_duplicate_numeric_and_boolean_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            (
                "doc-1",
                serde_json::json!({"service": "billing", "latency": 30, "active": true}),
            ),
            (
                "doc-2",
                serde_json::json!({"service": "search", "latency": 10, "active": false}),
            ),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let empty = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "service".to_string(),
                values: vec![],
            })),
            ..Default::default()
        });
        assert_eq!(empty.hits.total, 0);

        let duplicates = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "service".to_string(),
                values: vec![serde_json::json!("billing"), serde_json::json!("billing")],
            })),
            ..Default::default()
        });
        assert_eq!(duplicates.hits.total, 1);
        assert_eq!(duplicates.hits.hits[0].id, "doc-1");

        let numeric = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "latency".to_string(),
                values: vec![serde_json::json!(10), serde_json::json!(99)],
            })),
            ..Default::default()
        });
        assert_eq!(numeric.hits.total, 1);
        assert_eq!(numeric.hits.hits[0].id, "doc-2");

        let booleans = handle.search(&SearchRequest {
            query: Some(SearchQuery::Terms(TermsQuery {
                field: "active".to_string(),
                values: vec![serde_json::json!(true)],
            })),
            ..Default::default()
        });
        assert_eq!(booleans.hits.total, 1);
        assert_eq!(booleans.hits.hits[0].id, "doc-1");
    }

    #[tokio::test]
    async fn search_supports_pagination_and_sorting() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, latency, service, active, timestamp) in [
            ("doc-1", 30, "billing", true, "2026-03-14T10:00:00Z"),
            ("doc-2", 10, "search", false, "2026-03-14T11:00:00Z"),
            ("doc-3", 20, "auth", true, "2026-03-14T12:00:00Z"),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source: serde_json::json!({
                        "latency": latency,
                        "service": service,
                        "active": active,
                        "timestamp": timestamp,
                    }),
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let paged = handle.search(&SearchRequest {
            query: None,
            from: Some(1),
            size: Some(1),
            sort: Some(SortSpec {
                field: "latency".to_string(),
                order: SortOrder::Asc,
            }),
            ..Default::default()
        });
        assert_eq!(paged.hits.total, 3);
        assert_eq!(paged.hits.hits.len(), 1);
        assert_eq!(paged.hits.hits[0].id, "doc-3");

        let strings = handle.search(&SearchRequest {
            query: None,
            size: Some(2),
            sort: Some(SortSpec {
                field: "service".to_string(),
                order: SortOrder::Desc,
            }),
            ..Default::default()
        });
        assert_eq!(strings.hits.hits[0].id, "doc-2");
        assert_eq!(strings.hits.hits[1].id, "doc-1");

        let booleans = handle.search(&SearchRequest {
            query: None,
            sort: Some(SortSpec {
                field: "active".to_string(),
                order: SortOrder::Asc,
            }),
            ..Default::default()
        });
        assert_eq!(booleans.hits.hits[0].id, "doc-2");

        let timestamps = handle.search(&SearchRequest {
            query: None,
            sort: Some(SortSpec {
                field: "timestamp".to_string(),
                order: SortOrder::Desc,
            }),
            ..Default::default()
        });
        assert_eq!(timestamps.hits.hits[0].id, "doc-3");
    }

    #[tokio::test]
    async fn terms_and_stats_aggregations_respect_query_and_ignore_pagination() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            (
                "doc-1",
                serde_json::json!({"service": "billing", "level": "info", "latency": 10}),
            ),
            (
                "doc-2",
                serde_json::json!({"service": "billing", "level": "error", "latency": 20}),
            ),
            (
                "doc-3",
                serde_json::json!({"service": "search", "level": "info", "latency": 30}),
            ),
            (
                "doc-4",
                serde_json::json!({"service": "search", "level": "info"}),
            ),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let response = handle.search(&SearchRequest {
            query: Some(SearchQuery::Term(TermQuery {
                field: "level".to_string(),
                value: serde_json::json!("info"),
            })),
            from: Some(0),
            size: Some(1),
            aggs: Some(BTreeMap::from([
                (
                    "services".to_string(),
                    AggregationRequest::Terms(TermsAggregationRequest {
                        field: "service".to_string(),
                    }),
                ),
                (
                    "latency_stats".to_string(),
                    AggregationRequest::Stats(StatsAggregationRequest {
                        field: "latency".to_string(),
                    }),
                ),
            ])),
            ..Default::default()
        });

        assert_eq!(response.hits.total, 3);
        assert_eq!(response.hits.hits.len(), 1);

        match response.aggregations.get("services") {
            Some(AggregationResult::Terms(terms)) => {
                assert_eq!(terms.buckets.len(), 2);
                assert_eq!(terms.buckets[0].key, serde_json::json!("search"));
                assert_eq!(terms.buckets[0].doc_count, 2);
                assert_eq!(terms.buckets[1].key, serde_json::json!("billing"));
                assert_eq!(terms.buckets[1].doc_count, 1);
            }
            other => panic!("unexpected aggregation result: {other:?}"),
        }

        match response.aggregations.get("latency_stats") {
            Some(AggregationResult::Stats(stats)) => {
                assert_eq!(stats.count, 2);
                assert_eq!(stats.min, Some(10.0));
                assert_eq!(stats.max, Some(30.0));
                assert_eq!(stats.avg, Some(20.0));
                assert_eq!(stats.sum, 40.0);
            }
            other => panic!("unexpected aggregation result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn date_histogram_buckets_by_hour_and_ignores_missing_values() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            (
                "doc-1",
                serde_json::json!({"timestamp": "2026-03-14T10:05:00Z", "service": "billing"}),
            ),
            (
                "doc-2",
                serde_json::json!({"timestamp": "2026-03-14T10:45:00Z", "service": "billing"}),
            ),
            (
                "doc-3",
                serde_json::json!({"timestamp": "2026-03-14T11:15:00Z", "service": "billing"}),
            ),
            ("doc-4", serde_json::json!({"service": "billing"})),
            ("doc-5", serde_json::json!({"service": "billing"})),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let response = handle.search(&SearchRequest {
            aggs: Some(BTreeMap::from([(
                "events_over_time".to_string(),
                AggregationRequest::DateHistogram(DateHistogramAggregationRequest {
                    field: "timestamp".to_string(),
                    interval: DateHistogramInterval::Hour,
                }),
            )])),
            ..Default::default()
        });

        match response.aggregations.get("events_over_time") {
            Some(AggregationResult::DateHistogram(histogram)) => {
                assert_eq!(histogram.buckets.len(), 2);
                assert_eq!(histogram.buckets[0].key, "2026-03-14T10:00:00Z");
                assert_eq!(histogram.buckets[0].doc_count, 2);
                assert_eq!(histogram.buckets[1].key, "2026-03-14T11:00:00Z");
                assert_eq!(histogram.buckets[1].doc_count, 1);
            }
            other => panic!("unexpected aggregation result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn pagination_edges_and_missing_sort_field_are_stable() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        for (id, source) in [
            ("doc-1", serde_json::json!({"latency": 30})),
            ("doc-2", serde_json::json!({"latency": 10})),
            ("doc-3", serde_json::json!({"message": "missing-latency"})),
        ] {
            handle
                .index_document(IndexDocument {
                    id: id.to_string(),
                    source,
                })
                .await
                .expect("index doc");
        }
        handle.refresh().await.expect("refresh");

        let zero_size = handle.search(&SearchRequest {
            size: Some(0),
            ..Default::default()
        });
        assert_eq!(zero_size.hits.total, 3);
        assert!(zero_size.hits.hits.is_empty());

        let exact_end = handle.search(&SearchRequest {
            from: Some(3),
            size: Some(2),
            ..Default::default()
        });
        assert_eq!(exact_end.hits.total, 3);
        assert!(exact_end.hits.hits.is_empty());

        let missing_last = handle.search(&SearchRequest {
            sort: Some(SortSpec {
                field: "latency".to_string(),
                order: SortOrder::Asc,
            }),
            ..Default::default()
        });
        assert_eq!(missing_last.hits.hits[0].id, "doc-2");
        assert_eq!(missing_last.hits.hits[1].id, "doc-1");
        assert_eq!(missing_last.hits.hits[2].id, "doc-3");
    }

    #[tokio::test]
    async fn range_query_rejects_string_and_boolean_fields() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"service": "billing", "active": true}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        let string_range = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "service".to_string(),
                gte: Some(serde_json::json!("a")),
                gt: None,
                lte: None,
                lt: None,
            })),
            ..Default::default()
        });
        assert_eq!(string_range.hits.total, 0);

        let bool_range = handle.search(&SearchRequest {
            query: Some(SearchQuery::Range(RangeQuery {
                field: "active".to_string(),
                gte: Some(serde_json::json!(true)),
                gt: None,
                lte: None,
                lt: None,
            })),
            ..Default::default()
        });
        assert_eq!(bool_range.hits.total, 0);
    }

    #[tokio::test]
    async fn repeated_flushes_without_new_writes_are_stable() {
        let temp_dir = TempDir::new().expect("temp dir");
        let catalog = IndexCatalog::new(temp_dir.path());
        catalog.initialize().await.expect("init catalog");

        catalog
            .create_index(
                "logs",
                CreateIndexRequest {
                    settings: Default::default(),
                },
            )
            .await
            .expect("create index");

        let mut handle = catalog.open_index("logs").await.expect("open index");
        handle
            .index_document(IndexDocument {
                id: "doc-1".to_string(),
                source: serde_json::json!({"message": "hello"}),
            })
            .await
            .expect("index doc");
        handle.refresh().await.expect("refresh");

        let first = handle.flush().await.expect("first flush");
        let second = handle.flush().await.expect("second flush");

        assert_eq!(first.flushed_documents, 1);
        assert_eq!(second.flushed_documents, 1);

        let reopened = catalog.open_index("logs").await.expect("reopen index");
        assert_eq!(reopened.search(&SearchRequest::default()).hits.total, 1);
    }
}
