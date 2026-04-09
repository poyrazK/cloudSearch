/// Shared helpers for cloudsearch-node integration tests.
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};

/// Returns a port that is available for binding.
pub fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind random port");
    listener.local_addr().expect("local addr").port()
}

pub fn spawn_node(data_dir: &Path, port: u16) -> Child {
    spawn_node_with_intervals(data_dir, port, 1, 30)
}

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
