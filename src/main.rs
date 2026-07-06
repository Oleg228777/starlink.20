use prometheus::{Encoder, Registry, TextEncoder};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::{info, warn, error};
use tokio::time::{sleep, Duration};

use crate::{error::Error, metrics::Metrics};
use starlink::proto::space_x::api::device::{
    device_client::DeviceClient,
    request,
    response,
    GetDeviceInfoRequest,
    Request,
};

mod error;
mod metrics;

use std::fs::OpenOptions;
use std::io::Write;
use chrono::Local;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let starlink_address = dotenv::var("STARLINK_ADDRESS").unwrap_or("http://dishy.starlink.com:9200".to_string());

    info!("Connecting to Starlink device on {}", &starlink_address);

    let mut labels = HashMap::new();
    let mut connected = false;

    // Initial connection loop
    while !connected {
        match DeviceClient::connect(starlink_address.clone()).await {
            Ok(mut client) => {
                let req = tonic::Request::new(Request {
                    request: Some(request::Request::GetDeviceInfo(GetDeviceInfoRequest {})),
                    ..Default::default()
                });
                
                match client.handle(req).await {
                    Ok(res) => {
                        let res = res.into_inner();
                        if let Some(response::Response::GetDeviceInfo(r)) = res.response {
                            if let Some(device_info) = r.device_info {
                                if let Some(id) = device_info.id {
                                    info!("Registry label id = {}", &id);
                                    labels.insert("id".to_string(), id);
                                }
                                if let Some(hardware_version) = device_info.hardware_version {
                                    info!("Registry label hardware_version = {}", &hardware_version);
                                    labels.insert("hardware_version".to_string(), hardware_version);
                                }
                            }
                        }
                        connected = true;
                        info!("Successfully connected to Starlink device");
                    }
                    Err(e) => {
                        warn!("Failed to get device info: {}. Retrying in 5 seconds...", e);
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
            Err(e) => {
                warn!("Connection failed: {}. Retrying in 5 seconds...", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }

    let registry = Registry::new_custom(Some("starlink".to_string()), Some(labels))?;

    let metrics = Metrics::new()?;
    metrics.register(&registry)?;
    let metrics = Arc::new(Mutex::new(metrics));

    info!("Starting background logger (no web server). Writing to starlink_metrics_detailed.log");

    // Main loop with reconnection logic
    loop {
        let mut metrics_lock = metrics.lock().await;
        
        match metrics_lock.update(starlink_address.clone()).await {
            Ok(_) => {
                let encoder = TextEncoder::new();
                let metric_families = registry.gather();
                let mut buffer = vec![];
                
                if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
                    error!("Encode error: {}", e);
                } else {
                    let metrics_str = String::from_utf8_lossy(&buffer);
                    if let Err(e) = log_metrics_to_file(&metrics_str) {
                        error!("Log write error: {}", e);
                    } else {
                        info!("Logged metrics at {}", Local::now().format("%H:%M:%S"));
                    }
                }
            }
            Err(e) => {
                error!("Connection lost: {}", e);
                warn!("Attempting to reconnect to Starlink device...");
                
                // Release the lock before reconnecting
                drop(metrics_lock);
                
                // Reconnection loop
                loop {
                    match DeviceClient::connect(starlink_address.clone()).await {
                        Ok(mut client) => {
                            let req = tonic::Request::new(Request {
                                request: Some(request::Request::GetDeviceInfo(GetDeviceInfoRequest {})),
                                ..Default::default()
                            });
                            
                            match client.handle(req).await {
                                Ok(_) => {
                                    info!("Successfully reconnected to Starlink device. Resuming telemetry collection.");
                                    break; // Exit reconnection loop
                                }
                                Err(e) => {
                                    warn!("Reconnection attempt failed: {}. Retrying in 5 seconds...", e);
                                    sleep(Duration::from_secs(5)).await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Reconnection attempt failed: {}. Retrying in 5 seconds...", e);
                            sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
                // After successful reconnection, continue to next iteration
                // where we'll acquire a fresh lock
                continue;
            }
        }

        // Drop metrics_lock before sleeping
        drop(metrics_lock);
        sleep(Duration::from_secs(5)).await;
    }
}

fn log_metrics_to_file(metrics_text: &str) -> std::io::Result<()> {
    let filename = "starlink_metrics_detailed.log";
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)?;

    writeln!(file, "=")?;
    writeln!(file, "[{}] --- LOG ENTRY ---", timestamp)?;
    writeln!(file, "{}", metrics_text)?;
    writeln!(file, "=")?;
    writeln!(file)?;
    
    file.sync_all()?;

    Ok(())
}