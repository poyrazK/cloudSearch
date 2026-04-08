use cloudsearch_api::router_with_registry;
use cloudsearch_common::AppConfig;
use cloudsearch_index::{IndexCatalog, IndexRegistry};
use std::{env, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
    config.normalize_intervals();

    let catalog = Arc::new(IndexCatalog::new(config.data_dir.clone()));
    catalog.initialize().await?;
    let registry = Arc::new(IndexRegistry::new(catalog));

    spawn_refresh_loop(
        registry.clone(),
        Duration::from_secs(config.refresh_interval_secs),
    );
    spawn_flush_loop(
        registry.clone(),
        Duration::from_secs(config.flush_interval_secs),
    );
    spawn_merge_loop(
        registry.clone(),
        Duration::from_secs(config.merge_interval_secs),
    );

    let app = router_with_registry(registry);
    let listener = TcpListener::bind(&config.bind_addr).await?;

    println!("cloudSearch node listening on {}", config.bind_addr);
    tracing::info!("cloudSearch node listening on {}", config.bind_addr);

    axum::serve(listener, app).await?;

    Ok(())
}

fn spawn_refresh_loop(registry: Arc<IndexRegistry>, interval: Duration) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let handles = registry.cached_handles_with_names().await;
            for (index_name, handle) in handles {
                let handle = handle.clone();
                tokio::spawn(async move {
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

fn spawn_flush_loop(registry: Arc<IndexRegistry>, interval: Duration) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let handles = registry.cached_handles_with_names().await;
            for (index_name, handle) in handles {
                let handle = handle.clone();
                tokio::spawn(async move {
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

fn spawn_merge_loop(registry: Arc<IndexRegistry>, interval: Duration) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let handles = registry.cached_handles_with_names().await;
            for (index_name, handle) in handles {
                let handle = handle.clone();
                tokio::spawn(async move {
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

fn parse_interval_env(name: &str, default: u64) -> u64 {
    match env::var(name) {
        Ok(value) => match value.parse::<u64>() {
            Ok(parsed) => parsed,
            Err(_) => {
                eprintln!(
                    "cloudSearch ignored invalid value '{}' for {}; using default {}",
                    value, name, default
                );
                default
            }
        },
        Err(_) => default,
    }
}
