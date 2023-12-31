mod app_config;
mod error_handling;
mod protobuf;
mod shared_types;
mod stream_producer;
mod ws_connection;
mod ws_feed_consumer;

use dotenv;
use signal_hook::consts::signal::SIGTERM;
use signal_hook::iterator::Signals;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use tokio::time::{sleep, Duration};

use crate::app_config::AppConfig;
use crate::error_handling::ProcessError;
use crate::ws_connection::remove_active_connections;
use crate::ws_feed_consumer::run_one_feed;

fn load_app_config() -> Result<AppConfig, ProcessError> {
    AppConfig::new().map_err(|e| ProcessError::ConfigError(e.to_string()))
}

fn setup_sigterm_handler(running: Arc<AtomicBool>) {
    thread::spawn(move || {
        let mut signals = Signals::new(&[SIGTERM]).unwrap();
        for _ in signals.forever() {
            running.store(false, Ordering::SeqCst);
        }
    });
}

async fn run_app(app_config: &AppConfig, running: Arc<AtomicBool>) {
    let feeds = app_config.feeds.clone();
    let mut tasks = Vec::new();

    for feed in feeds {
        let task = tokio::spawn(run_one_feed(feed, 5));
        tasks.push(task);
    }

    while running.load(Ordering::SeqCst) && !tasks.is_empty() {
        tasks.retain(|task| !task.is_finished());

        if tasks.is_empty() {
            eprintln!("All feeds have stopped. Shutting down the application.");
            running.store(false, Ordering::SeqCst);
            break;
        }

        sleep(Duration::from_secs(5)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), ProcessError> {
    dotenv::dotenv().ok();

    let running = Arc::new(AtomicBool::new(true));

    setup_sigterm_handler(running.clone());

    let app_config = load_app_config()?;
    while running.load(Ordering::SeqCst) {
        run_app(&app_config, running.clone()).await;
    }

    println!("Application shutting down gracefully.");
    remove_active_connections().await;
    Ok(())
}
