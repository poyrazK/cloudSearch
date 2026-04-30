use reqwest::Client;
use tempfile::TempDir;

pub mod helpers {
    include!("helpers.rs");
}

use helpers::{TestNode, reserve_port, spawn_node, stop_node, wait_for_health};

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn health_endpoint_returns_200() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .get(format!("{base_url}/_health"))
        .send()
        .await
        .expect("health request");
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["status"], "ok");

    stop_node(&mut child);
}

#[tokio::test]
async fn create_and_query_index() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    let resp = client
        .get(format!("{base_url}/test"))
        .send()
        .await
        .expect("get index request")
        .error_for_status()
        .expect("get index status");

    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["name"], "test");
    assert_eq!(body["settings"]["mapping_mode"], "controlled_dynamic");

    stop_node(&mut child);
}

#[tokio::test]
async fn index_and_fetch_document() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .put(format!("{base_url}/test/_doc"))
        .json(&serde_json::json!({
            "id": "doc-1",
            "source": {"service": "billing", "message": "hello"}
        }))
        .send()
        .await
        .expect("index doc request")
        .error_for_status()
        .expect("index doc status");

    let resp = client
        .get(format!("{base_url}/test/_doc/doc-1"))
        .send()
        .await
        .expect("get doc request")
        .error_for_status()
        .expect("get doc status");

    let doc: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(doc["_id"], "doc-1");
    assert_eq!(doc["_source"]["message"], "hello");

    stop_node(&mut child);
}

#[tokio::test]
async fn search_returns_documents_after_refresh() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .put(format!("{base_url}/test/_doc"))
        .json(&serde_json::json!({"id": "doc-1", "source": {"service": "billing"}}))
        .send()
        .await
        .expect("index doc 1")
        .error_for_status()
        .expect("index doc 1 status");

    client
        .put(format!("{base_url}/test/_doc"))
        .json(&serde_json::json!({"id": "doc-2", "source": {"service": "search"}}))
        .send()
        .await
        .expect("index doc 2")
        .error_for_status()
        .expect("index doc 2 status");

    client
        .post(format!("{base_url}/test/_refresh"))
        .send()
        .await
        .expect("refresh request")
        .error_for_status()
        .expect("refresh status");

    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"term": {"field": "service", "value": "billing"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");

    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 1);
    assert_eq!(body["hits"]["hits"][0]["_id"], "doc-1");

    stop_node(&mut child);
}

#[tokio::test]
async fn bulk_operations() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    let resp = client
        .post(format!("{base_url}/test/_bulk"))
        .json(&serde_json::json!({
            "operations": [
                {"index": {"id": "doc-1", "source": {"service": "billing", "latency": 30}}},
                {"index": {"id": "doc-2", "source": {"service": "search", "latency": 10}}}
            ]
        }))
        .send()
        .await
        .expect("bulk request")
        .error_for_status()
        .expect("bulk status");

    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["errors"], false);
    assert_eq!(body["items"].as_array().unwrap().len(), 2);

    stop_node(&mut child);
}

#[tokio::test]
async fn overwrite_document_by_id() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .put(format!("{base_url}/test/_doc"))
        .json(&serde_json::json!({"id": "doc-1", "source": {"service": "billing", "message": "first"}}))
        .send()
        .await
        .expect("index doc request")
        .error_for_status()
        .expect("index doc status");

    client
        .put(format!("{base_url}/test/_doc"))
        .json(&serde_json::json!({"id": "doc-1", "source": {"service": "billing", "message": "updated"}}))
        .send()
        .await
        .expect("overwrite doc request")
        .error_for_status()
        .expect("overwrite doc status");

    client
        .post(format!("{base_url}/test/_refresh"))
        .send()
        .await
        .expect("refresh request")
        .error_for_status()
        .expect("refresh status");

    let resp = client
        .get(format!("{base_url}/test/_doc/doc-1"))
        .send()
        .await
        .expect("get doc request")
        .error_for_status()
        .expect("get doc status");

    let doc: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(doc["_source"]["message"], "updated");

    stop_node(&mut child);
}

#[tokio::test]
async fn delete_and_recreate_index() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .delete(format!("{base_url}/test"))
        .send()
        .await
        .expect("delete index request")
        .error_for_status()
        .expect("delete index status");

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("recreate index request")
        .error_for_status()
        .expect("recreate index status");

    stop_node(&mut child);
}

#[tokio::test]
async fn flush_endpoint() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .put(format!("{base_url}/test/_doc"))
        .json(&serde_json::json!({"id": "doc-1", "source": {"service": "billing"}}))
        .send()
        .await
        .expect("index doc request")
        .error_for_status()
        .expect("index doc status");

    let resp = client
        .post(format!("{base_url}/test/_flush"))
        .send()
        .await
        .expect("flush request")
        .error_for_status()
        .expect("flush status");

    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["result"], "flushed");
    assert!(body["flushed_documents"].is_number());

    stop_node(&mut child);
}

#[tokio::test]
async fn metrics_endpoint() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    let resp = client
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .expect("metrics request")
        .error_for_status()
        .expect("metrics status");

    let body: String = resp.text().await.expect("parse body");
    assert!(body.contains("cloudsearch_open_indexes"));

    stop_node(&mut child);
}

#[tokio::test]
async fn bulk_delete_removes_document() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    client
        .post(format!("{base_url}/test/_bulk"))
        .json(&serde_json::json!({
            "operations": [
                {"index": {"id": "doc-1", "source": {"msg": "one"}}},
                {"index": {"id": "doc-2", "source": {"msg": "two"}}},
                {"index": {"id": "doc-3", "source": {"msg": "three"}}}
            ]
        }))
        .send()
        .await
        .expect("bulk request")
        .error_for_status()
        .expect("bulk status");

    client
        .post(format!("{base_url}/test/_bulk"))
        .json(&serde_json::json!({
            "operations": [{"delete": {"id": "doc-2"}}]
        }))
        .send()
        .await
        .expect("bulk delete")
        .error_for_status()
        .expect("bulk delete status");

    client
        .post(format!("{base_url}/test/_refresh"))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({"query": {"match_all": {}}}))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 2);

    stop_node(&mut child);
}

#[tokio::test]
async fn put_index_returns_409_if_already_exists() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    let resp = client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("second create index request");

    assert_eq!(resp.status(), 409);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn get_index_returns_404_if_missing() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .get(format!("{base_url}/missing"))
        .send()
        .await
        .expect("get missing index request");

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn delete_index_returns_404_if_missing() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .delete(format!("{base_url}/missing"))
        .send()
        .await
        .expect("delete missing index request");

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn put_doc_returns_404_if_index_missing() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .put(format!("{base_url}/missing/_doc"))
        .json(&serde_json::json!({"id": "doc-1", "source": {"x": 1}}))
        .send()
        .await
        .expect("put doc to missing index request");

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn get_doc_returns_404_if_index_missing() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .get(format!("{base_url}/missing/_doc/doc-1"))
        .send()
        .await
        .expect("get doc from missing index request");

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn search_returns_404_if_index_missing() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .post(format!("{base_url}/missing/_search"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("search missing index request");

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn bulk_returns_404_if_index_missing() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .post(format!("{base_url}/missing/_bulk"))
        .json(&serde_json::json!({"operations": [{"index": {"id": "doc-1", "source": {"x": 1}}}]}))
        .send()
        .await
        .expect("bulk to missing index request");

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn bulk_with_invalid_item_fails_request() {
    // A bulk request with malformed JSON body returns 400.
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    let resp = client
        .post(format!("{base_url}/test/_bulk"))
        .header("content-type", "application/json")
        .body("not valid json{{{")
        .send()
        .await
        .expect("bulk request");
    assert_eq!(resp.status(), 400, "malformed JSON body should return 400");

    stop_node(&mut child);
}

#[tokio::test]
async fn refresh_returns_404_if_index_missing() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .post(format!("{base_url}/missing/_refresh"))
        .send()
        .await
        .expect("refresh missing index request");

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn flush_returns_404_if_index_missing() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .post(format!("{base_url}/missing/_flush"))
        .send()
        .await
        .expect("flush missing index request");

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn merge_returns_404_if_index_missing() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let resp = client
        .post(format!("{base_url}/missing/_merge"))
        .send()
        .await
        .expect("merge missing index request");

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn bool_query_must_and_should_combined() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    for (id, svc, level) in [
        ("doc-1", "billing", "error"),
        ("doc-2", "search", "info"),
        ("doc-3", "billing", "info"),
    ] {
        client
            .put(format!("{base_url}/test/_doc"))
            .json(&serde_json::json!({"id": id, "source": {"service": svc, "level": level}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    client
        .post(format!("{base_url}/test/_refresh"))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {
                "bool": {
                    "must": [{"term": {"field": "service", "value": "billing"}}],
                    "should": [{"term": {"field": "level", "value": "error"}}]
                }
            }
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");

    // Both billing docs match must; doc-1 (error) scores higher due to should match.
    assert_eq!(body["hits"]["total"]["value"], 2);
    assert_eq!(body["hits"]["hits"][0]["_id"], "doc-1");
    assert_eq!(body["hits"]["hits"][1]["_id"], "doc-3");

    stop_node(&mut child);
}

#[tokio::test]
async fn search_returns_400_for_unsupported_query_type() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"unsupported_clause": {"field": "x", "value": 1}}
        }))
        .send()
        .await
        .expect("search request");

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert!(body.get("error").is_some());

    stop_node(&mut child);
}

#[tokio::test]
async fn range_query_filters_by_numeric_field() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    for (id, latency) in [("a", 10), ("b", 30), ("c", 20)] {
        client
            .put(format!("{base_url}/test/_doc"))
            .json(&serde_json::json!({"id": id, "source": {"latency": latency}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    client
        .post(format!("{base_url}/test/_refresh"))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {
                "range": {
                    "field": "latency",
                    "gte": 15,
                    "lte": 25
                }
            }
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");

    assert_eq!(body["hits"]["total"]["value"], 1);
    assert_eq!(body["hits"]["hits"][0]["_id"], "c");

    stop_node(&mut child);
}

#[tokio::test]
async fn get_missing_document_returns_404() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    let resp = client
        .get(format!("{base_url}/test/_doc/nonexistent"))
        .send()
        .await
        .expect("get request");
    assert_eq!(resp.status(), 404, "get non-existent doc should return 404");

    stop_node(&mut child);
}

#[tokio::test]
async fn range_on_string_field_returns_400() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    client
        .put(format!("{base_url}/test/_doc"))
        .json(&serde_json::json!({"id": "doc-1", "source": {"service": "billing"}}))
        .send()
        .await
        .expect("index doc")
        .error_for_status()
        .expect("index status");

    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {
                "range": {
                    "field": "service",
                    "gte": "a",
                    "lte": "z"
                }
            }
        }))
        .send()
        .await
        .expect("search request");
    assert_eq!(
        resp.status(),
        400,
        "range on string field should return 400"
    );

    stop_node(&mut child);
}

#[tokio::test]
async fn prefix_query_matches_string_prefixes() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    for (id, svc) in [
        ("d1", "auth-service"),
        ("d2", "auth-worker"),
        ("d3", "billing-api"),
    ] {
        client
            .put(format!("{base_url}/test/_doc"))
            .json(&serde_json::json!({"id": id, "source": {"service": svc}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    client
        .post(format!("{base_url}/test/_refresh"))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    // Prefix "auth-" matches d1 and d2
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"prefix": {"field": "service", "value": "auth-"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 2);

    // Shorthand form: prefix "auth-" matches d1 and d2
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"prefix": {"service": "auth-"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 2);

    // Prefix "auth-worker" matches d2 only
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"prefix": {"field": "service", "value": "auth-worker"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 1);
    assert_eq!(body["hits"]["hits"][0]["_id"], "d2");

    // Prefix "xyz" matches nothing
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"prefix": {"field": "service", "value": "xyz"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 0);

    stop_node(&mut child);
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn wildcard_query_matches_string_patterns() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    for (id, svc) in [
        ("d1", "auth-service"),
        ("d2", "auth-worker"),
        ("d3", "billing-api"),
        ("d4", "search-service"),
    ] {
        client
            .put(format!("{base_url}/test/_doc"))
            .json(&serde_json::json!({"id": id, "source": {"service": svc}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    client
        .post(format!("{base_url}/test/_refresh"))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    // Wildcard "auth-*" matches d1 and d2
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"wildcard": {"field": "service", "value": "auth-*"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 2);

    // Shorthand form: wildcard "*-service" matches d1 and d4
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"wildcard": {"service": "*-service"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 2);

    // Wildcard "*-api" matches d3 only
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"wildcard": {"field": "service", "value": "*-api"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 1);

    // Wildcard "xyz*" matches nothing
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"wildcard": {"field": "service", "value": "xyz*"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 0);

    // Wildcard "?illing-api" matches billing-api (? = single char)
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"wildcard": {"field": "service", "value": "?illing-api"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 1);

    // Wildcard "?xyz*" matches nothing (? must match exactly one char)
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"wildcard": {"field": "service", "value": "?xyz*"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 0);

    stop_node(&mut child);
}

#[tokio::test]
async fn match_query_finds_text_in_documents() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/test"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    for (id, msg) in [
        ("d1", "hello world"),
        ("d2", "hello there world"),
        ("d3", "foo bar"),
    ] {
        client
            .put(format!("{base_url}/test/_doc"))
            .json(&serde_json::json!({"id": id, "source": {"message": msg}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    client
        .post(format!("{base_url}/test/_refresh"))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    // Match "hello" finds d1 and d2
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"match": {"field": "message", "value": "hello"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 2);

    // Shorthand form: match "world"
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"match": {"message": "world"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 2);

    // Scores are exposed
    assert!(body["hits"]["hits"][0]["_score"].is_number());

    // Match "xyz" finds nothing
    let resp = client
        .post(format!("{base_url}/test/_search"))
        .json(&serde_json::json!({
            "query": {"match": {"field": "message", "value": "xyz"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["hits"]["total"]["value"], 0);

    stop_node(&mut child);
}

#[tokio::test]
async fn multi_search_returns_results_from_multiple_indexes() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    // Create two indexes
    client
        .put(format!("{base_url}/index-a"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index-a")
        .error_for_status()
        .expect("create index-a status");

    client
        .put(format!("{base_url}/index-b"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index-b")
        .error_for_status()
        .expect("create index-b status");

    // Index docs
    client
        .put(format!("{base_url}/index-a/_doc"))
        .json(&serde_json::json!({"id": "doc1", "source": {"content": "hello world"}}))
        .send()
        .await
        .expect("index doc1")
        .error_for_status()
        .expect("index doc1 status");

    client
        .put(format!("{base_url}/index-b/_doc"))
        .json(&serde_json::json!({"id": "doc2", "source": {"content": "foo bar"}}))
        .send()
        .await
        .expect("index doc2")
        .error_for_status()
        .expect("index doc2 status");

    // Refresh both
    client
        .post(format!("{base_url}/index-a/_refresh"))
        .send()
        .await
        .expect("refresh index-a")
        .error_for_status()
        .expect("refresh index-a status");

    client
        .post(format!("{base_url}/index-b/_refresh"))
        .send()
        .await
        .expect("refresh index-b")
        .error_for_status()
        .expect("refresh index-b status");

    // Call _msearch
    let resp = client
        .post(format!("{base_url}/_msearch"))
        .json(&serde_json::json!({
            "searches": [
                { "index": "index-a", "request": { "query": { "match": { "field": "content", "value": "hello" } } } },
                { "index": "index-b", "request": { "query": { "match": { "field": "content", "value": "foo" } } } }
            ]
        }))
        .send()
        .await
        .expect("msearch request")
        .error_for_status()
        .expect("msearch status");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    let responses = body["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["index"], "index-a");
    assert_eq!(responses[1]["index"], "index-b");
    assert_eq!(responses[0]["status"], 200);
    assert_eq!(responses[1]["status"], 200);

    // First search found doc1
    let hits_a = &responses[0]["response"]["hits"]["hits"];
    assert_eq!(hits_a.as_array().expect("hits array").len(), 1);
    assert_eq!(hits_a[0]["id"], "doc1");

    // Second search found doc2
    let hits_b = &responses[1]["response"]["hits"]["hits"];
    assert_eq!(hits_b.as_array().expect("hits array").len(), 1);
    assert_eq!(hits_b[0]["id"], "doc2");

    stop_node(&mut child);
}

#[tokio::test]
async fn multi_search_handles_missing_index_gracefully() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    // Create only index-a
    client
        .put(format!("{base_url}/index-a"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index-a")
        .error_for_status()
        .expect("create index-a status");

    let resp = client
        .post(format!("{base_url}/_msearch"))
        .json(&serde_json::json!({
            "searches": [
                { "index": "index-a", "request": { "query": null } },
                { "index": "nonexistent", "request": { "query": null } }
            ]
        }))
        .send()
        .await
        .expect("msearch request")
        .error_for_status()
        .expect("msearch status");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("parse body");
    let responses = body["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["index"], "index-a");
    assert_eq!(responses[0]["status"], 200);
    assert_eq!(responses[1]["index"], "nonexistent");
    assert_eq!(responses[1]["status"], 404);
    assert!(responses[1]["error"].is_string());

    stop_node(&mut child);
}

#[tokio::test]
async fn phrase_query_matches_consecutive_terms() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/test", node.base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create index status");

    // Index two docs with similar content but different ordering
    node.client
        .put(format!("{}/test/_doc", node.base_url))
        .json(&serde_json::json!({"id": "doc1", "source": {"message": "the quick brown fox"}}))
        .send()
        .await
        .expect("index doc1")
        .error_for_status()
        .expect("index doc1 status");

    node.client
        .put(format!("{}/test/_doc", node.base_url))
        .json(&serde_json::json!({"id": "doc2", "source": {"message": "the quick red fox"}}))
        .send()
        .await
        .expect("index doc2")
        .error_for_status()
        .expect("index doc2 status");

    node.client
        .post(format!("{}/test/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    // Flush to ensure positions are written to disk
    node.client
        .post(format!("{}/test/_flush", node.base_url))
        .send()
        .await
        .expect("flush")
        .error_for_status()
        .expect("flush status");

    // Exact phrase "quick brown fox" — should match doc1 only
    let resp = node
        .client
        .post(format!("{}/test/_search", node.base_url))
        .json(&serde_json::json!({
            "query": { "phrase": { "field": "message", "value": "quick brown fox" } }
        }))
        .send()
        .await
        .expect("phrase search request")
        .error_for_status()
        .expect("phrase search status");

    let body: serde_json::Value = resp.json().await.expect("parse body");
    let hits = body["hits"]["hits"].as_array().expect("hits array");
    assert_eq!(
        hits.len(),
        1,
        "expected exactly 1 hit for exact phrase match"
    );
    assert_eq!(hits[0]["_id"], "doc1");

    node.stop();
}

#[tokio::test]
async fn phrase_query_with_no_consecutive_match() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/test", node.base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create index status");

    // Index a doc where "brown" comes before "quick"
    node.client
        .put(format!("{}/test/_doc", node.base_url))
        .json(&serde_json::json!({"id": "doc1", "source": {"message": "the brown quick fox"}}))
        .send()
        .await
        .expect("index doc1")
        .error_for_status()
        .expect("index doc1 status");

    node.client
        .post(format!("{}/test/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    // Search for "quick brown fox" (consecutive in source) — should NOT match
    let resp = node
        .client
        .post(format!("{}/test/_search", node.base_url))
        .json(&serde_json::json!({
            "query": { "phrase": { "field": "message", "value": "quick brown fox" } }
        }))
        .send()
        .await
        .expect("phrase search request")
        .error_for_status()
        .expect("phrase search status");

    let body: serde_json::Value = resp.json().await.expect("parse body");
    let hits = body["hits"]["hits"].as_array().expect("hits array");
    assert_eq!(
        hits.len(),
        0,
        "expected 0 hits when terms are not consecutive"
    );

    node.stop();
}

#[tokio::test]
async fn create_and_list_snapshots() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/test", node.base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create index status");

    for i in 1..=3 {
        node.client
            .put(format!("{}/test/_doc", node.base_url))
            .json(&serde_json::json!({
                "id": format!("doc-{i}"),
                "source": {"field": format!("value{i}")}
            }))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    node.client
        .post(format!("{}/test/_snapshot/snap1", node.base_url))
        .send()
        .await
        .expect("create snapshot")
        .error_for_status()
        .expect("create snapshot status");

    let resp = node
        .client
        .get(format!("{}/test/_snapshot", node.base_url))
        .send()
        .await
        .expect("list snapshots")
        .error_for_status()
        .expect("list snapshots status");

    let body: serde_json::Value = resp.json().await.expect("parse body");
    let snapshots = body["snapshots"].as_array().expect("snapshots array");
    assert_eq!(snapshots.len(), 1, "expected 1 snapshot");
    assert_eq!(snapshots[0]["name"], "snap1");

    node.stop();
}

#[tokio::test]
async fn create_and_get_snapshot() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/test", node.base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create index status");

    node.client
        .put(format!("{}/test/_doc", node.base_url))
        .json(&serde_json::json!({
            "id": "doc-1",
            "source": {"field": "value1"}
        }))
        .send()
        .await
        .expect("index doc")
        .error_for_status()
        .expect("index status");

    node.client
        .post(format!("{}/test/_snapshot/mysnap", node.base_url))
        .send()
        .await
        .expect("create snapshot")
        .error_for_status()
        .expect("create snapshot status");

    let resp = node
        .client
        .get(format!("{}/test/_snapshot/mysnap", node.base_url))
        .send()
        .await
        .expect("get snapshot")
        .error_for_status()
        .expect("get snapshot status");

    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["name"], "mysnap");

    node.stop();
}

#[tokio::test]
async fn delete_snapshot_removes_from_list() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/test", node.base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create index status");

    node.client
        .post(format!("{}/test/_snapshot/to-delete", node.base_url))
        .send()
        .await
        .expect("create snapshot")
        .error_for_status()
        .expect("create snapshot status");

    let resp = node
        .client
        .get(format!("{}/test/_snapshot", node.base_url))
        .send()
        .await
        .expect("list snapshots")
        .error_for_status()
        .expect("list snapshots status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(body["snapshots"].as_array().unwrap().len(), 1);

    node.client
        .delete(format!("{}/test/_snapshot/to-delete", node.base_url))
        .send()
        .await
        .expect("delete snapshot")
        .error_for_status()
        .expect("delete snapshot status");

    let resp = node
        .client
        .get(format!("{}/test/_snapshot", node.base_url))
        .send()
        .await
        .expect("list snapshots")
        .error_for_status()
        .expect("list snapshots status");
    let body: serde_json::Value = resp.json().await.expect("parse body");
    assert_eq!(
        body["snapshots"].as_array().unwrap().len(),
        0,
        "expected 0 snapshots after delete"
    );

    node.stop();
}

#[tokio::test]
async fn restore_snapshot_recovers_documents() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.create_index("test")
        .await
        .error_for_status()
        .expect("create index status");

    // Index documents
    for i in 1..=2 {
        node.index_doc(
            "test",
            &format!("doc-{i}"),
            serde_json::json!({"field": format!("value{i}")}),
        )
        .await
        .error_for_status()
        .expect("index doc status");
    }

    node.refresh("test")
        .await
        .error_for_status()
        .expect("refresh status");
    node.client
        .post(format!("{}/test/_snapshot/backup1", node.base_url))
        .send()
        .await
        .expect("create snapshot")
        .error_for_status()
        .expect("create snapshot status");

    // Delete all documents via bulk
    node.bulk(
        "test",
        serde_json::json!([
            {"delete": {"id": "doc-1"}},
            {"delete": {"id": "doc-2"}}
        ]),
    )
    .await
    .error_for_status()
    .expect("bulk delete status");

    node.refresh("test")
        .await
        .error_for_status()
        .expect("refresh status");

    // Verify docs are gone
    let body = node
        .search("test", serde_json::json!({"query": {"match_all": {}}}))
        .await;
    assert_eq!(
        TestNode::hits_total(&body),
        0,
        "expected 0 docs after delete"
    );

    // Restore snapshot
    node.client
        .post(format!("{}/test/_snapshot/backup1/_restore", node.base_url))
        .send()
        .await
        .expect("restore snapshot")
        .error_for_status()
        .expect("restore status");

    node.refresh("test")
        .await
        .error_for_status()
        .expect("refresh status");

    // Verify docs are back
    let body = node
        .search("test", serde_json::json!({"query": {"match_all": {}}}))
        .await;
    assert_eq!(
        TestNode::hits_total(&body),
        2,
        "expected 2 docs after restore"
    );

    node.stop();
}

#[tokio::test]
async fn snapshot_returns_404_for_missing_index() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    let resp = node
        .client
        .post(format!("{}/nonexistent/_snapshot/snap1", node.base_url))
        .send()
        .await
        .expect("create snapshot request");

    assert_eq!(resp.status(), 404, "expected 404 for missing index");

    node.stop();
}
