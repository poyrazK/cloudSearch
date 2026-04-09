use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

pub mod helpers {
    include!("helpers.rs");
}

use helpers::{
    TestNode, reserve_port, spawn_node, spawn_node_with_all_intervals, spawn_node_with_intervals,
    stop_node,
};

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
async fn preserves_documents_and_search_results_across_node_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut first = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/logs"))
        .json(&json!({
            "settings": {
                "mapping_mode": "controlled_dynamic",
                "primary_time_field": null
            }
        }))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .put(format!("{base_url}/logs/_doc"))
        .json(&json!({
            "id": "doc-1",
            "source": {"service": "billing", "message": "hello"}
        }))
        .send()
        .await
        .expect("index doc request")
        .error_for_status()
        .expect("index doc status");

    stop_node(&mut first);

    let mut second = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let document = client
        .get(format!("{base_url}/logs/_doc/doc-1"))
        .send()
        .await
        .expect("get doc request")
        .error_for_status()
        .expect("get doc status")
        .json::<serde_json::Value>()
        .await
        .expect("document body");

    assert_eq!(document["_id"], "doc-1");
    assert_eq!(document["_source"]["message"], "hello");

    let search = client
        .post(format!("{base_url}/logs/_search"))
        .json(&json!({
            "query": {
                "term": {
                    "field": "service",
                    "value": "billing"
                }
            }
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("search body");

    assert_eq!(search["hits"]["total"]["value"], 1);
    assert_eq!(search["hits"]["hits"][0]["_id"], "doc-1");

    stop_node(&mut second);
}

#[tokio::test]
async fn preserves_flushed_documents_and_replays_wal_tail_across_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut first = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/logs"))
        .json(&json!({
            "settings": {
                "mapping_mode": "controlled_dynamic",
                "primary_time_field": null
            }
        }))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .put(format!("{base_url}/logs/_doc"))
        .json(&json!({
            "id": "doc-1",
            "source": {"service": "billing", "message": "flushed"}
        }))
        .send()
        .await
        .expect("index doc request")
        .error_for_status()
        .expect("index doc status");

    client
        .post(format!("{base_url}/logs/_refresh"))
        .send()
        .await
        .expect("refresh request")
        .error_for_status()
        .expect("refresh status");

    client
        .post(format!("{base_url}/logs/_flush"))
        .send()
        .await
        .expect("flush request")
        .error_for_status()
        .expect("flush status");

    client
        .put(format!("{base_url}/logs/_doc"))
        .json(&json!({
            "id": "doc-2",
            "source": {"service": "search", "message": "wal-tail"}
        }))
        .send()
        .await
        .expect("index doc request")
        .error_for_status()
        .expect("index doc status");

    stop_node(&mut first);

    let mut second = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let search = client
        .post(format!("{base_url}/logs/_search"))
        .json(&json!({}))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("search body");

    assert_eq!(search["hits"]["total"]["value"], 2);

    stop_node(&mut second);
}

#[tokio::test]
async fn preserves_bulk_flushed_state_and_sorted_search_after_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut first = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/logs"))
        .json(&json!({
            "settings": {
                "mapping_mode": "controlled_dynamic",
                "primary_time_field": null
            }
        }))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .post(format!("{base_url}/logs/_bulk"))
        .json(&json!({
            "operations": [
                {"index": {"id": "doc-1", "source": {"service": "billing", "latency": 30}}},
                {"index": {"id": "doc-2", "source": {"service": "search", "latency": 10}}},
                {"index": {"id": "doc-3", "source": {"service": "auth", "latency": 20}}}
            ]
        }))
        .send()
        .await
        .expect("bulk request")
        .error_for_status()
        .expect("bulk status");

    client
        .post(format!("{base_url}/logs/_refresh"))
        .send()
        .await
        .expect("refresh request")
        .error_for_status()
        .expect("refresh status");

    client
        .post(format!("{base_url}/logs/_flush"))
        .send()
        .await
        .expect("flush request")
        .error_for_status()
        .expect("flush status");

    client
        .put(format!("{base_url}/logs/_doc"))
        .json(&json!({
            "id": "doc-4",
            "source": {"service": "billing", "latency": 15}
        }))
        .send()
        .await
        .expect("index doc request")
        .error_for_status()
        .expect("index doc status");

    client
        .post(format!("{base_url}/logs/_refresh"))
        .send()
        .await
        .expect("refresh request")
        .error_for_status()
        .expect("refresh status");

    stop_node(&mut first);

    let mut second = spawn_node(temp_dir.path(), port);
    wait_for_health(&client, &base_url).await;

    let search = client
        .post(format!("{base_url}/logs/_search"))
        .json(&json!({
            "query": {
                "terms": {
                    "field": "service",
                    "values": ["billing", "auth"]
                }
            },
            "sort": {
                "field": "latency",
                "order": "asc"
            },
            "from": 0,
            "size": 3
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("search body");

    assert_eq!(search["hits"]["total"]["value"], 3);
    assert_eq!(search["hits"]["hits"][0]["_id"], "doc-4");
    assert_eq!(search["hits"]["hits"][1]["_id"], "doc-3");
    assert_eq!(search["hits"]["hits"][2]["_id"], "doc-1");

    stop_node(&mut second);
}

#[tokio::test]
async fn automatic_refresh_makes_documents_searchable_without_manual_refresh() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node_with_intervals(temp_dir.path(), port, 1, 60);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/logs"))
        .json(&json!({
            "settings": {
                "mapping_mode": "controlled_dynamic",
                "primary_time_field": null
            }
        }))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .put(format!("{base_url}/logs/_doc"))
        .json(&json!({
            "id": "doc-1",
            "source": {"service": "billing"}
        }))
        .send()
        .await
        .expect("index request")
        .error_for_status()
        .expect("index status");

    for _ in 0..20 {
        let response = client
            .post(format!("{base_url}/logs/_search"))
            .json(&json!({}))
            .send()
            .await
            .expect("search request")
            .error_for_status()
            .expect("search status")
            .json::<serde_json::Value>()
            .await
            .expect("search body");

        if response["hits"]["total"]["value"] == 1 {
            stop_node(&mut child);
            return;
        }

        sleep(Duration::from_millis(200)).await;
    }

    stop_node(&mut child);
    panic!("document did not become searchable after automatic refresh");
}

#[tokio::test]
async fn automatic_flush_persists_searchable_state_without_manual_flush() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut first = spawn_node_with_intervals(temp_dir.path(), port, 1, 2);
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/logs"))
        .json(&json!({
            "settings": {
                "mapping_mode": "controlled_dynamic",
                "primary_time_field": null
            }
        }))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    client
        .put(format!("{base_url}/logs/_doc"))
        .json(&json!({
            "id": "doc-1",
            "source": {"service": "billing", "message": "persisted"}
        }))
        .send()
        .await
        .expect("index request")
        .error_for_status()
        .expect("index status");

    sleep(Duration::from_secs(4)).await;
    stop_node(&mut first);

    let mut second = spawn_node_with_intervals(temp_dir.path(), port, 1, 2);
    wait_for_health(&client, &base_url).await;

    let response = client
        .post(format!("{base_url}/logs/_search"))
        .json(&json!({}))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("search body");

    assert_eq!(response["hits"]["total"]["value"], 1);
    assert_eq!(
        response["hits"]["hits"][0]["_source"]["message"],
        "persisted"
    );

    stop_node(&mut second);
}

#[tokio::test]
async fn automatic_merge_compacts_segments_without_manual_call() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = Client::new();

    let mut child = spawn_node_with_all_intervals(temp_dir.path(), port, 1, 2, Some(2));
    wait_for_health(&client, &base_url).await;

    client
        .put(format!("{base_url}/logs"))
        .json(&json!({
            "settings": {
                "mapping_mode": "controlled_dynamic",
                "primary_time_field": null
            }
        }))
        .send()
        .await
        .expect("create index request")
        .error_for_status()
        .expect("create index status");

    for i in 0..5 {
        client
            .put(format!("{base_url}/logs/_doc"))
            .json(&json!({
                "id": format!("doc-{}", i),
                "source": {"service": "billing", "index": i}
            }))
            .send()
            .await
            .expect("index request")
            .error_for_status()
            .expect("index status");
    }

    sleep(Duration::from_secs(4)).await;

    let response = client
        .post(format!("{base_url}/logs/_search"))
        .json(&json!({}))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("search body");

    assert_eq!(response["hits"]["total"]["value"], 5);

    stop_node(&mut child);
}

#[tokio::test]
async fn bulk_index_survives_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/logs", node.base_url))
        .json(&json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    node.client
        .post(format!("{}/logs/_bulk", node.base_url))
        .json(&serde_json::json!({
            "operations": [
                {"index": {"id": "doc-1", "source": {"service": "billing", "msg": "one"}}},
                {"index": {"id": "doc-2", "source": {"service": "search", "msg": "two"}}},
                {"index": {"id": "doc-3", "source": {"service": "auth", "msg": "three"}}},
                {"index": {"id": "doc-4", "source": {"service": "billing", "msg": "four"}}},
                {"index": {"id": "doc-5", "source": {"service": "search", "msg": "five"}}}
            ]
        }))
        .send()
        .await
        .expect("bulk request")
        .error_for_status()
        .expect("bulk status");

    node.client
        .post(format!("{}/logs/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    node.restart().await;

    let search = node
        .client
        .post(format!("{}/logs/_search", node.base_url))
        .json(&serde_json::json!({"query": {"match_all": {}}}))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("search body");

    assert_eq!(search["hits"]["total"]["value"], 5);

    node.stop();
}

#[tokio::test]
async fn sorted_search_order_preserved_after_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/logs", node.base_url))
        .json(&json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    for (id, latency) in [("a", 10), ("b", 30), ("c", 20)] {
        node.client
            .put(format!("{}/logs/_doc", node.base_url))
            .json(&serde_json::json!({"id": id, "source": {"latency": latency}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    node.client
        .post(format!("{}/logs/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    node.restart().await;

    let search = node
        .client
        .post(format!("{}/logs/_search", node.base_url))
        .json(&serde_json::json!({
            "sort": {"field": "latency", "order": "desc"}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("search body");

    assert_eq!(search["hits"]["hits"][0]["_id"], "b");
    assert_eq!(search["hits"]["hits"][1]["_id"], "c");
    assert_eq!(search["hits"]["hits"][2]["_id"], "a");

    node.stop();
}

#[tokio::test]
async fn multi_index_survives_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    for idx in ["logs", "events"] {
        node.client
            .put(format!("{}/{idx}", node.base_url))
            .json(&json!({}))
            .send()
            .await
            .expect("create index")
            .error_for_status()
            .expect("create status");

        node.client
            .put(format!("{}/{idx}/_doc", node.base_url))
            .json(&serde_json::json!({"id": "doc-1", "source": {"x": 1}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    for idx in ["logs", "events"] {
        node.client
            .post(format!("{}/{idx}/_refresh", node.base_url))
            .send()
            .await
            .expect("refresh")
            .error_for_status()
            .expect("refresh status");
    }

    node.restart().await;

    for idx in ["logs", "events"] {
        let search = node
            .client
            .post(format!("{}/{idx}/_search", node.base_url))
            .json(&serde_json::json!({"query": {"match_all": {}}}))
            .send()
            .await
            .expect("search request")
            .error_for_status()
            .expect("search status")
            .json::<serde_json::Value>()
            .await
            .expect("search body");

        assert_eq!(
            search["hits"]["total"]["value"], 1,
            "index {idx} should have 1 doc"
        );
    }

    node.stop();
}

#[tokio::test]
async fn delete_document_survives_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/logs", node.base_url))
        .json(&json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    for id in ["doc-1", "doc-2"] {
        node.client
            .put(format!("{}/logs/_doc", node.base_url))
            .json(&serde_json::json!({"id": id, "source": {"x": 1}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    node.client
        .post(format!("{}/logs/_bulk", node.base_url))
        .json(&serde_json::json!({"operations": [{"delete": {"id": "doc-1"}}]}))
        .send()
        .await
        .expect("delete doc")
        .error_for_status()
        .expect("delete status");

    node.client
        .post(format!("{}/logs/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    node.restart().await;

    let search = node
        .client
        .post(format!("{}/logs/_search", node.base_url))
        .json(&serde_json::json!({"query": {"match_all": {}}}))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("search body");

    assert_eq!(search["hits"]["total"]["value"], 1);
    assert_eq!(search["hits"]["hits"][0]["_id"], "doc-2");

    node.stop();
}
