/// Shared helpers for cloudsearch-node integration tests.
use reqwest::Client;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

pub async fn wait_for_health(client: &Client, base_url: &str) {
    let mut last_err = String::new();
    for _ in 0..50 {
        let url = format!("{base_url}/_health");
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => {
                last_err = response.status().to_string();
            }
            Err(err) => {
                last_err = format!("{err:?}");
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("node did not become healthy in time: {last_err}");
}

/// A test harness that manages a cloudsearch-node process lifecycle.
///
/// Provides spawn/restart/stop with guaranteed cleanup via Drop.
pub struct TestNode {
    pub temp_dir: TempDir,
    pub port: u16,
    pub base_url: String,
    pub client: Client,
    child: Option<Child>,
}

impl TestNode {
    /// Spawns a new node, waiting until it is healthy.
    pub async fn spawn(temp_dir: TempDir, port: u16) -> Self {
        let base_url = format!("http://127.0.0.1:{port}");
        let client = Client::new();
        let child = spawn_node_process(temp_dir.path(), port);
        wait_for_health(&client, &base_url).await;
        Self {
            temp_dir,
            port,
            base_url,
            client,
            child: Some(child),
        }
    }

    /// Stops the current node and spawns a fresh one on the same port/data directory.
    pub async fn restart(&mut self) {
        if let Some(mut child) = self.child.take() {
            stop_node(&mut child);
        }
        self.child = Some(spawn_node_process(self.temp_dir.path(), self.port));
        wait_for_health(&self.client, &self.base_url).await;
    }

    /// Stops the managed node process.
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            stop_node(&mut child);
        }
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Returns a port that is available for binding.
#[must_use]
pub fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind random port");
    listener.local_addr().expect("local addr").port()
}

fn spawn_node_process(data_dir: &Path, port: u16) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cloudsearch-node"));
    cmd.env("CLOUDSEARCH_BIND", format!("127.0.0.1:{port}"))
        .env("CLOUDSEARCH_DATA_DIR", data_dir)
        .env("CLOUDSEARCH_REFRESH_INTERVAL_SECS", "1")
        .env("CLOUDSEARCH_FLUSH_INTERVAL_SECS", "30")
        .stdout(if std::env::var("CLOUDSEARCH_TEST_DEBUG").is_ok() {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .stderr(if std::env::var("CLOUDSEARCH_TEST_DEBUG").is_ok() {
            Stdio::inherit()
        } else {
            Stdio::null()
        });
    cmd.spawn().expect("spawn node")
}

#[must_use]
pub fn spawn_node(data_dir: &Path, port: u16) -> Child {
    spawn_node_with_intervals(data_dir, port, 1, 30)
}

#[must_use]
pub fn spawn_node_with_intervals(
    data_dir: &Path,
    port: u16,
    refresh_interval_secs: u64,
    flush_interval_secs: u64,
) -> Child {
    spawn_node_with_all_intervals(
        data_dir,
        port,
        refresh_interval_secs,
        flush_interval_secs,
        None,
    )
}

#[must_use]
pub fn spawn_node_with_all_intervals(
    data_dir: &Path,
    port: u16,
    refresh_interval_secs: u64,
    flush_interval_secs: u64,
    merge_interval_secs: Option<u64>,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cloudsearch-node"));
    cmd.env("CLOUDSEARCH_BIND", format!("127.0.0.1:{port}"))
        .env("CLOUDSEARCH_DATA_DIR", data_dir)
        .env(
            "CLOUDSEARCH_REFRESH_INTERVAL_SECS",
            refresh_interval_secs.to_string(),
        )
        .env(
            "CLOUDSEARCH_FLUSH_INTERVAL_SECS",
            flush_interval_secs.to_string(),
        )
        .stdout(if std::env::var("CLOUDSEARCH_TEST_DEBUG").is_ok() {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .stderr(if std::env::var("CLOUDSEARCH_TEST_DEBUG").is_ok() {
            Stdio::inherit()
        } else {
            Stdio::null()
        });

    if let Some(merge_secs) = merge_interval_secs {
        cmd.env("CLOUDSEARCH_MERGE_INTERVAL_SECS", merge_secs.to_string());
    }

    cmd.spawn().expect("spawn node")
}

pub fn stop_node(child: &mut Child) {
    child.kill().expect("kill node");
    child.wait().expect("wait for node exit");
}
