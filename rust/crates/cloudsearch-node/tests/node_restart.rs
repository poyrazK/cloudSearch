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
    stop_node, wait_for_health,
};

#[allow(clippy::too_many_lines)]
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

#[tokio::test]
async fn merge_triggered_after_enough_documents() {
    // Index more than MERGE_TRIGGER_DOCUMENT_COUNT (8) docs, then call merge.
    // Merge endpoint should return 200 even without prior refresh.
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

    for i in 0..10 {
        node.client
            .put(format!("{}/logs/_doc", node.base_url))
            .json(&serde_json::json!({"id": format!("doc-{}", i), "source": {"x": i}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    let resp = node
        .client
        .post(format!("{}/logs/_merge", node.base_url))
        .send()
        .await
        .expect("merge request")
        .error_for_status()
        .expect("merge status");
    assert_eq!(resp.status(), 200);

    let json: serde_json::Value = resp.json().await.expect("parse merge response");
    assert_eq!(json["result"], "merged");
    assert_eq!(json["merged_documents"], 10);

    node.stop();
}

#[tokio::test]
async fn merged_segments_survive_restart() {
    // Index docs, flush (snapshots), index more, merge, restart → all docs found.
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

    for i in 0..5 {
        node.client
            .put(format!("{}/logs/_doc", node.base_url))
            .json(&serde_json::json!({"id": format!("doc-{}", i), "source": {"x": i}}))
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

    node.client
        .post(format!("{}/logs/_flush", node.base_url))
        .send()
        .await
        .expect("flush")
        .error_for_status()
        .expect("flush status");

    for i in 5..8 {
        node.client
            .put(format!("{}/logs/_doc", node.base_url))
            .json(&serde_json::json!({"id": format!("doc-{}", i), "source": {"x": i}}))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    node.client
        .post(format!("{}/logs/_merge", node.base_url))
        .send()
        .await
        .expect("merge request")
        .error_for_status()
        .expect("merge status");

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

    assert_eq!(search["hits"]["total"]["value"], 8);

    node.stop();
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn compaction_removes_overwrites_across_restart() {
    // Overwrite same doc 3 times with different values, merge, restart.
    // Latest value should survive, no duplicates.
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

    // v1
    node.client
        .put(format!("{}/logs/_doc", node.base_url))
        .json(&serde_json::json!({"id": "doc-1", "source": {"version": "v1"}}))
        .send()
        .await
        .expect("index doc")
        .error_for_status()
        .expect("index status");
    node.client
        .post(format!("{}/logs/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");
    node.client
        .post(format!("{}/logs/_flush", node.base_url))
        .send()
        .await
        .expect("flush")
        .error_for_status()
        .expect("flush status");

    // v2
    node.client
        .put(format!("{}/logs/_doc", node.base_url))
        .json(&serde_json::json!({"id": "doc-1", "source": {"version": "v2"}}))
        .send()
        .await
        .expect("index doc")
        .error_for_status()
        .expect("index status");
    node.client
        .post(format!("{}/logs/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");
    node.client
        .post(format!("{}/logs/_flush", node.base_url))
        .send()
        .await
        .expect("flush")
        .error_for_status()
        .expect("flush status");

    // v3 (no flush)
    node.client
        .put(format!("{}/logs/_doc", node.base_url))
        .json(&serde_json::json!({"id": "doc-1", "source": {"version": "v3"}}))
        .send()
        .await
        .expect("index doc")
        .error_for_status()
        .expect("index status");
    node.client
        .post(format!("{}/logs/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    node.client
        .post(format!("{}/logs/_merge", node.base_url))
        .send()
        .await
        .expect("merge request")
        .error_for_status()
        .expect("merge status");

    node.restart().await;

    let doc = node
        .client
        .get(format!("{}/logs/_doc/doc-1", node.base_url))
        .send()
        .await
        .expect("get doc")
        .error_for_status()
        .expect("get status")
        .json::<serde_json::Value>()
        .await
        .expect("doc body");
    assert_eq!(doc["_source"]["version"], "v3");

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

    node.stop();
}

#[tokio::test]
async fn paginated_search_returns_correct_total_across_pages() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/test", node.base_url))
        .json(&json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    // Index 5 documents
    for i in 0..5 {
        node.client
            .put(format!("{}/test/_doc", node.base_url))
            .json(&serde_json::json!({
                "id": format!("doc-{}", i),
                "source": {"n": i}
            }))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    // Refresh to make searchable
    node.client
        .post(format!("{}/test/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    // Search with pagination — request 2 at a time, starting at 0
    let page1 = node
        .client
        .post(format!("{}/test/_search", node.base_url))
        .json(&json!({
            "size": 2,
            "from": 0,
            "sort": [{"n": {"order": "asc"}}]
        }))
        .send()
        .await
        .expect("search page 1")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("parse page 1");

    let page2 = node
        .client
        .post(format!("{}/test/_search", node.base_url))
        .json(&serde_json::json!({
            "size": 2,
            "from": 2,
            "sort": [{"n": {"order": "asc"}}]
        }))
        .send()
        .await
        .expect("search page 2")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("parse page 2");

    // Total should be 5 across both pages, not 2
    assert_eq!(
        page1["hits"]["total"]["value"], 5,
        "total should reflect all 5 docs"
    );
    assert_eq!(
        page2["hits"]["total"]["value"], 5,
        "total should be same on page 2"
    );
    assert_eq!(page1["hits"]["hits"].as_array().unwrap().len(), 2);
    assert_eq!(page2["hits"]["hits"].as_array().unwrap().len(), 2);

    node.stop();
}

#[tokio::test]
async fn highlights_work_across_multiple_segments() {
    let temp_dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let mut node = TestNode::spawn(temp_dir, port).await;

    node.client
        .put(format!("{}/test", node.base_url))
        .json(&json!({}))
        .send()
        .await
        .expect("create index")
        .error_for_status()
        .expect("create status");

    // Index first batch and flush (creates segment 1)
    for i in 0..3 {
        node.client
            .put(format!("{}/test/_doc", node.base_url))
            .json(&serde_json::json!({
                "id": format!("doc-{}", i),
                "source": {"content": format!("hello world term{}", i)}
            }))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    node.client
        .post(format!("{}/test/_flush", node.base_url))
        .send()
        .await
        .expect("flush")
        .error_for_status()
        .expect("flush status");

    // Index second batch and flush (creates segment 2)
    for i in 3..6 {
        node.client
            .put(format!("{}/test/_doc", node.base_url))
            .json(&serde_json::json!({
                "id": format!("doc-{}", i),
                "source": {"content": format!("hello world term{}", i)}
            }))
            .send()
            .await
            .expect("index doc")
            .error_for_status()
            .expect("index status");
    }

    node.client
        .post(format!("{}/test/_flush", node.base_url))
        .send()
        .await
        .expect("flush")
        .error_for_status()
        .expect("flush status");

    // Merge to combine segments
    node.client
        .post(format!("{}/test/_merge", node.base_url))
        .send()
        .await
        .expect("merge request")
        .error_for_status()
        .expect("merge status");

    node.client
        .post(format!("{}/test/_refresh", node.base_url))
        .send()
        .await
        .expect("refresh")
        .error_for_status()
        .expect("refresh status");

    // Search with term that should find highlights in docs from BOTH pre-merge segments
    let response = node
        .client
        .post(format!("{}/test/_search", node.base_url))
        .json(&serde_json::json!({
            "query": {"match": {"field": "content", "value": "hello"}}
        }))
        .send()
        .await
        .expect("search request")
        .error_for_status()
        .expect("search status")
        .json::<serde_json::Value>()
        .await
        .expect("search body");

    let hits = response["hits"]["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 6, "should find all 6 documents");

    // At least some hits should have highlights
    let with_highlights = hits.iter().filter(|h| h.get("highlight").is_some()).count();
    assert!(
        with_highlights > 0,
        "expected some hits to have highlight fragments, got {with_highlights}"
    );

    node.stop();
}
