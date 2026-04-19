use cloudsearch_api::router_with_registry;
use cloudsearch_common::AppConfig;
use cloudsearch_index::{IndexCatalog, IndexRegistry};
use std::{env, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, broadcast},
    time::sleep,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

struct ShutdownHandle {
    sender: broadcast::Sender<()>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cloudsearch=info,warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let mut config = AppConfig::default();
    config.bind_addr = env::var("CLOUDSEARCH_BIND").unwrap_or(config.bind_addr);
    config.data_dir = env::var("CLOUDSEARCH_DATA_DIR")
        .map(Into::into)
        .unwrap_or(config.data_dir);
    config.refresh_interval_secs = parse_interval_env(
        "CLOUDSEARCH_REFRESH_INTERVAL_SECS",
        config.refresh_interval_secs,
    );
    config.flush_interval_secs = parse_interval_env(
        "CLOUDSEARCH_FLUSH_INTERVAL_SECS",
        config.flush_interval_secs,
    );
    config.merge_interval_secs = parse_interval_env(
        "CLOUDSEARCH_MERGE_INTERVAL_SECS",
        config.merge_interval_secs,
    );
    config.retention_interval_secs = parse_interval_env(
        "CLOUDSEARCH_RETENTION_INTERVAL_SECS",
        config.retention_interval_secs,
    );
    config.normalize_intervals();

    let catalog = Arc::new(IndexCatalog::new(config.data_dir.clone()));
    catalog.initialize().await?;
    let registry = Arc::new(IndexRegistry::new(catalog));

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let shutdown = ShutdownHandle {
        sender: shutdown_tx,
    };

    // Semaphore to limit concurrent background operations across all indexes
    // Use MAX_PERMITS as "unlimited" when not configured (Semaphore permits max)
    const MAX_PERMITS: usize = 2305843009213693951;
    let bg_semaphore = Arc::new(Semaphore::new(
        config.max_concurrent_background_ops.unwrap_or(MAX_PERMITS),
    ));

    spawn_refresh_loop(
        registry.clone(),
        Duration::from_secs(config.refresh_interval_secs),
        shutdown_rx.resubscribe(),
        Arc::clone(&bg_semaphore),
    );
    spawn_flush_loop(
        registry.clone(),
        Duration::from_secs(config.flush_interval_secs),
        shutdown_rx.resubscribe(),
        Arc::clone(&bg_semaphore),
    );
    spawn_merge_loop(
        registry.clone(),
        Duration::from_secs(config.merge_interval_secs),
        shutdown_rx.resubscribe(),
        Arc::clone(&bg_semaphore),
    );
    spawn_retention_loop(
        registry.clone(),
        Duration::from_secs(config.retention_interval_secs),
        shutdown_rx.resubscribe(),
        Arc::clone(&bg_semaphore),
    );

    let app = router_with_registry(registry);
    let listener = TcpListener::bind(&config.bind_addr).await?;

    println!("cloudSearch node listening on {}", config.bind_addr);
    tracing::info!("cloudSearch node listening on {}", config.bind_addr);

    // Wait for shutdown signal (SIGINT/SIGTERM) then broadcast shutdown to all loops
    let shutdown_tx = shutdown.sender;
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown signal received, stopping background tasks...");
        let _ = shutdown_tx.send(());
    });

    // Graceful shutdown: stop accepting new connections when shutdown signal is received
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}

fn spawn_refresh_loop(
    registry: Arc<IndexRegistry>,
    interval: Duration,
    mut shutdown: broadcast::Receiver<()>,
    sem: Arc<Semaphore>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = sleep(interval) => {}
                _ = shutdown.recv() => {
                    tracing::debug!("refresh loop received shutdown signal, stopping");
                    break;
                }
            }
            let handles = registry.cached_handles_with_names().await;
            for (index_name, handle) in handles {
                let handle = handle.clone();
                let sem = Arc::clone(&sem);
                tokio::spawn(async move {
                    // Acquire permit inside the task so it stays alive with the task
                    let _permit = sem.acquire().await;
                    let result = async {
                        let mut guard = handle.lock().await;
                        guard.refresh().await
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::warn!(index = %index_name, "background refresh failed: {}", error);
                    }
                });
            }
        }
    });
}

fn spawn_flush_loop(
    registry: Arc<IndexRegistry>,
    interval: Duration,
    mut shutdown: broadcast::Receiver<()>,
    sem: Arc<Semaphore>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = sleep(interval) => {}
                _ = shutdown.recv() => {
                    tracing::debug!("flush loop received shutdown signal, stopping");
                    break;
                }
            }
            let handles = registry.cached_handles_with_names().await;
            for (index_name, handle) in handles {
                let handle = handle.clone();
                let sem = Arc::clone(&sem);
                tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    let result = async {
                        let mut guard = handle.lock().await;
                        guard.flush().await
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::warn!(index = %index_name, "background flush failed: {}", error);
                    }
                });
            }
        }
    });
}

fn spawn_merge_loop(
    registry: Arc<IndexRegistry>,
    interval: Duration,
    mut shutdown: broadcast::Receiver<()>,
    sem: Arc<Semaphore>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = sleep(interval) => {}
                _ = shutdown.recv() => {
                    tracing::debug!("merge loop received shutdown signal, stopping");
                    break;
                }
            }
            let handles = registry.cached_handles_with_names().await;
            for (index_name, handle) in handles {
                let handle = handle.clone();
                let sem = Arc::clone(&sem);
                tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    let result = async {
                        let mut guard = handle.lock().await;
                        guard.merge().await
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::warn!(index = %index_name, "background merge failed: {}", error);
                    }
                });
            }
        }
    });
}

fn spawn_retention_loop(
    registry: Arc<IndexRegistry>,
    interval: Duration,
    mut shutdown: broadcast::Receiver<()>,
    sem: Arc<Semaphore>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = sleep(interval) => {}
                _ = shutdown.recv() => {
                    tracing::debug!("retention loop received shutdown signal, stopping");
                    break;
                }
            }
            let handles = registry.cached_handles_with_names().await;
            for (index_name, handle) in handles {
                let handle = handle.clone();
                let sem = Arc::clone(&sem);
                tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    let result = async {
                        let mut guard = handle.lock().await;
                        guard.evict_expired_documents().await
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::warn!(index = %index_name, "background retention eviction failed: {}", error);
                    }
                });
            }
        }
    });
}

fn parse_interval_env(name: &str, default: u64) -> u64 {
    match env::var(name) {
        Ok(value) => match value.parse::<u64>() {
            Ok(parsed) => parsed,
            Err(_) => {
                eprintln!(
                    "cloudSearch ignored invalid value '{value}' for {name}; using default {default}"
                );
                default
            }
        },
        Err(_) => default,
    }
}
