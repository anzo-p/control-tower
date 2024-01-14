use futures_util::{stream::StreamExt, SinkExt};
use lazy_static::lazy_static;
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::errors::ProcessError;
use crate::load_app_config;
use crate::ws_feed_consumer::{AuthMessage, SubMessage};

const INITIAL_BACKOFF: u64 = 1;
const MAX_BACKOFF: u64 = 64;
const BACKOFF_FACTOR: u64 = 2;

type MyWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

lazy_static! {
    static ref WEBSOCKET_CONNECTIONS: Mutex<HashMap<String, Arc<Mutex<MyWebSocketStream>>>> =
        Mutex::new(HashMap::new());
}

pub async fn acquire_connection(url_str: &str) -> Result<(), ProcessError> {
    let mut retry_count = 0;
    let mut backoff = INITIAL_BACKOFF;
    let max_retries = 5;

    while retry_count <= max_retries {
        match connect_to_stream(url_str).await {
            Ok(_) => {
                auth_and_sub_by_url(url_str).await?;
                return Ok(());
            }
            Err(e) => {
                retry_count += 1;
                eprintln!(
                    "{} - Error connecting/reconnecting to {}: {}. Attempt {} of {}.",
                    chrono::Local::now(),
                    url_str,
                    e,
                    retry_count + 1,
                    max_retries
                );
                sleep(Duration::from_secs(backoff)).await;
                backoff = std::cmp::min(backoff * BACKOFF_FACTOR, MAX_BACKOFF);
            }
        }
    }

    Err(ProcessError::MaxRetriesReached(format!(
        "Failed to connect to {} after {} attempts",
        url_str, max_retries
    )))
}

pub async fn read_from_connection(url_str: &str) -> Result<Option<Message>, ProcessError> {
    let ws_arc = {
        let connections = WEBSOCKET_CONNECTIONS.lock().await;
        connections
            .get(url_str)
            .cloned()
            .ok_or(ProcessError::ConnectionNotFound((&*url_str).to_string()))?
    };

    let mut ws: MutexGuard<_> = ws_arc.lock().await;
    match ws.next().await {
        Some(Ok(message)) => return Ok(Some(message)),

        other => {
            let error_message = if let Some(Err(e)) = other {
                format!("Websocket read error: {:?}, ", e)
            } else {
                String::from("Websocket connection closed, ")
            };

            eprintln!("{} - {}attempting to reconnect...", chrono::Local::now(), error_message,);
            remove_connection(url_str).await;
            return attempt_reconnect(url_str).await;
        }
    }
}

async fn attempt_reconnect(url_str: &str) -> Result<Option<Message>, ProcessError> {
    match acquire_connection(url_str).await {
        Ok(_) => Ok(None),
        Err(e) => Err(e),
    }
}

async fn send_message<T: Serialize>(url_str: &str, message: &T) -> Result<(), ProcessError> {
    let content = serde_json::to_string(message)?;
    let ws_message = Message::Text(content).to_string();

    let connections = WEBSOCKET_CONNECTIONS.lock().await;
    if let Some(ws_arc) = connections.get(url_str) {
        let mut ws = ws_arc.lock().await;
        ws.send(Message::from(ws_message)).await?;
    }
    Ok(())
}

async fn connect_to_stream(url_str: &str) -> Result<(), ProcessError> {
    let mut connections = WEBSOCKET_CONNECTIONS.lock().await;

    if connections.contains_key(url_str) {
        eprintln!("{} - Connection to {} already exists", chrono::Local::now(), url_str);
        return Ok(());
    }

    let url = Url::parse(url_str).map_err(|e| ProcessError::UrlParseError(e))?;
    eprintln!("{} - Connecting to {}", chrono::Local::now(), url_str);
    let (ws_stream, _) = connect_async(&url)
        .await
        .map_err(|e| ProcessError::WebSocketConnectionError {
            url: url.to_string(),
            source: e,
        })?;

    eprintln!("{} - Successfully connected to {}", chrono::Local::now(), url_str);
    connections.insert(url_str.to_string(), Arc::new(Mutex::new(ws_stream)));
    Ok(())
}

async fn auth_and_sub_by_url(url_str: &str) -> Result<(), ProcessError> {
    let app_config = load_app_config()?;
    if let Some(feed) = app_config.feeds.iter().find(|f| f.url == url_str) {
        auth_and_sub_by_url_and_symbols(url_str, &feed.symbols).await
    } else {
        eprintln!(
            "{} - No such feed: '{}' to connect to in app config",
            chrono::Local::now(),
            url_str
        );
        Ok(())
    }
}

async fn auth_and_sub_by_url_and_symbols(url_str: &str, trading_symbols: &[String]) -> Result<(), ProcessError> {
    if let Err(e) = send_auth_message(url_str).await {
        return Err(e);
    }
    if let Err(e) = send_sub_message(url_str, trading_symbols).await {
        return Err(e);
    }
    Ok(())
}

async fn send_sub_message(url_str: &str, trading_symbols: &[String]) -> Result<(), ProcessError> {
    let payload = SubMessage::new(&trading_symbols.to_vec())?;
    send_message(url_str, &payload).await
}

async fn send_auth_message(url_str: &str) -> Result<(), ProcessError> {
    let payload = AuthMessage::new()?;
    send_message(url_str, &payload).await
}

async fn remove_connection(url_str: &str) {
    let mut connections = WEBSOCKET_CONNECTIONS.lock().await;
    connections.remove(url_str);
    eprintln!("{} - Removed connection to ws url {}", chrono::Local::now(), url_str);
}

pub async fn remove_active_connections() {
    eprintln!("Shutting down - closing all websockect connections");
    let connections = WEBSOCKET_CONNECTIONS.lock().await;

    for (_, ws_stream) in connections.iter() {
        let mut ws_stream = ws_stream.lock().await;

        if let Err(e) = ws_stream.close(None).await {
            eprintln!("Error sending close message: {:?}", e);
        }
    }
    eprintln!("Waiting few seconds for all connections to close...");
    sleep(Duration::from_secs(10)).await;
}
