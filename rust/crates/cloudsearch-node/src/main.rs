use cloudsearch_api::router_with_registry;
use cloudsearch_index::{IndexCatalog, IndexRegistry};
use std::{env, sync::Arc};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env::var("CLOUDSEARCH_BIND").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let data_dir = env::var("CLOUDSEARCH_DATA_DIR").unwrap_or_else(|_| "./data".to_string());

    let catalog = Arc::new(IndexCatalog::new(data_dir));
    catalog.initialize().await?;
    let registry = Arc::new(IndexRegistry::new(catalog));

    let app = router_with_registry(registry);
    let listener = TcpListener::bind(&bind_addr).await?;

    println!("cloudSearch node listening on {bind_addr}");

    axum::serve(listener, app).await?;

    Ok(())
}
