use reqwest::Client;
use serde_json::json;
use std::{
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tempfile::TempDir;
use tokio::time::sleep;

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

fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind random port");
    listener.local_addr().expect("local addr").port()
}

fn spawn_node(data_dir: &Path, port: u16) -> Child {
    spawn_node_with_intervals(data_dir, port, 1, 30)
}

fn spawn_node_with_intervals(
    data_dir: &Path,
    port: u16,
    refresh_interval_secs: u64,
    flush_interval_secs: u64,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_cloudsearch-node"))
        .env("CLOUDSEARCH_BIND", format!("127.0.0.1:{port}"))
        .env("CLOUDSEARCH_DATA_DIR", data_dir)
        .env(
            "CLOUDSEARCH_REFRESH_INTERVAL_SECS",
            refresh_interval_secs.to_string(),
        )
        .env(
            "CLOUDSEARCH_FLUSH_INTERVAL_SECS",
            flush_interval_secs.to_string(),
        )
        .env("CLOUDSEARCH_MERGE_INTERVAL_SECS", "60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn node")
}

fn spawn_node_with_all_intervals(
    data_dir: &Path,
    port: u16,
    refresh_interval_secs: u64,
    flush_interval_secs: u64,
    merge_interval_secs: u64,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_cloudsearch-node"))
        .env("CLOUDSEARCH_BIND", format!("127.0.0.1:{port}"))
        .env("CLOUDSEARCH_DATA_DIR", data_dir)
        .env(
            "CLOUDSEARCH_REFRESH_INTERVAL_SECS",
            refresh_interval_secs.to_string(),
        )
        .env(
            "CLOUDSEARCH_FLUSH_INTERVAL_SECS",
            flush_interval_secs.to_string(),
        )
        .env(
            "CLOUDSEARCH_MERGE_INTERVAL_SECS",
            merge_interval_secs.to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn node")
}

fn stop_node(child: &mut Child) {
    child.kill().expect("kill node");
    child.wait().expect("wait for node exit");
}

async fn wait_for_health(client: &Client, base_url: &str) {
    for _ in 0..50 {
        if let Ok(response) = client.get(format!("{base_url}/_health")).send().await
            && response.status().is_success()
        {
            return;
        }

        sleep(Duration::from_millis(100)).await;
    }

    panic!("node did not become healthy in time");
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

    let mut child = spawn_node_with_all_intervals(temp_dir.path(), port, 1, 2, 2);
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
