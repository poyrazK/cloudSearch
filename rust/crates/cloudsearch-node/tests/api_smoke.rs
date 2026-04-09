use reqwest::Client;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

pub mod helpers {
    include!("helpers.rs");
}

use helpers::{reserve_port, spawn_node, stop_node};

async fn wait_for_health(client: &Client, base_url: &str) {
    let mut last_err = String::new();
    for _ in 0..50 {
        let url = format!("{base_url}/_health");
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => {
                last_err = response.status().to_string();
            }
            Err(err) => {
                last_err = format!("{:?}", err);
            }
        }
        sleep(Duration::from_millis(100)).await;
    }

    panic!("node did not become healthy in time: {last_err}");
}

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
