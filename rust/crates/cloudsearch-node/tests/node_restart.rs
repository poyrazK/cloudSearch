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

    assert_eq!(document["id"], "doc-1");
    assert_eq!(document["source"]["message"], "hello");

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

    assert_eq!(search["hits"]["total"], 1);
    assert_eq!(search["hits"]["hits"][0]["id"], "doc-1");

    stop_node(&mut second);
}

fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind random port");
    listener.local_addr().expect("local addr").port()
}

fn spawn_node(data_dir: &Path, port: u16) -> Child {
    Command::new(env!("CARGO_BIN_EXE_cloudsearch-node"))
        .env("CLOUDSEARCH_BIND", format!("127.0.0.1:{port}"))
        .env("CLOUDSEARCH_DATA_DIR", data_dir)
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
