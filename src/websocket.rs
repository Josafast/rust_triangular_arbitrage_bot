use crate::scaled_decimals::deserialize_price_to_u128;
use futures_util::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use std::ops::ControlFlow;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::protocol::{Message, WebSocketConfig},
};
use tokio_util::{sync::CancellationToken};
use url::Url;
use tokio::{net::TcpStream, sync::broadcast};
use crate::config::{get_binance_url, get_pairs_for_triangle_arbitrage};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
//type WsWriter = futures_util::stream::SplitSink<WsStream, Message>;

#[derive(Debug, Deserialize, Clone)]
pub struct OrderTicker {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b", deserialize_with = "deserialize_price_to_u128")]
    pub bid_price: u128,
    #[serde(rename = "B", deserialize_with = "deserialize_price_to_u128")]
    pub bid_qty: u128,
    #[serde(rename = "a", deserialize_with = "deserialize_price_to_u128")]
    pub ask_price: u128,
    #[serde(rename = "A", deserialize_with = "deserialize_price_to_u128")]
    pub ask_qty: u128,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StreamTicker {
    #[allow(dead_code)]
    pub stream: String,
    #[serde(rename = "data")]
    pub order: OrderTicker,
}


pub struct BinanceWs {
    url: Url,
    tx: broadcast::Sender<OrderTicker>,
}

impl BinanceWs {
    fn init(binance_url: &str) -> Self {
        let url = Url::parse(binance_url).expect("Invalid Url");

        let (tx, _) = broadcast::channel(100);

        Self {
            url,
            tx,
        }
    }

    
    pub fn new_with_url(binance_url: String) -> Self {
        Self::init(&binance_url)
    }

    #[cfg(test)]
    pub fn new_dummy(tx: broadcast::Sender<OrderTicker>) -> Self {
        let url = Url::parse("ws://localhost:1234").expect("Invalid Url");

        Self {
            url,
            tx
        }
    }

    pub fn get_receiver(&self) -> broadcast::Receiver<OrderTicker> {
        self.tx.subscribe()
    }
}

impl Default for BinanceWs {
    fn default() -> Self {
        let pairs = get_pairs_for_triangle_arbitrage();

        let re = Regex::new(r"pair").unwrap();

        let env_url: String = get_binance_url();

        let mut i = 0;
        let binance_url = re.replace_all(&env_url, |_caps: &regex::Captures| {
            let replacement = pairs.get(i).map_or("", |v| v);
            i += 1;
            replacement.to_lowercase()
        }).into_owned();

        Self::init(&binance_url)
    }
}

static WSCONFIG: Lazy<WebSocketConfig> = Lazy::new(|| WebSocketConfig {
    max_message_size: Some(64 * 1024),
    max_frame_size: Some(64 * 1024),
    accept_unmasked_frames: false,
    ..WebSocketConfig::default()
});

impl BinanceWs {
    pub async fn stream_websocket(&self, shutdown_token: CancellationToken) {
        let mut retry_delay = tokio::time::Duration::from_secs(1);

        while !shutdown_token.is_cancelled() {
            match connect_async_with_config(self.url.clone(), Some(*WSCONFIG), true).await {
                Ok((ws_stream, _)) => {
                    log::info!("Connected to BINANCE");
                    retry_delay = tokio::time::Duration::from_secs(1);
                    
                    self.stream_prices(ws_stream, shutdown_token.clone()).await;
                }
                Err(e) => {
                    log::error!("Can't connect to Binance: {e}. Retrying in {:?}...", retry_delay);
                }
            }

            tokio::select! {
                _ = shutdown_token.cancelled() => break,
                _ = tokio::time::sleep(retry_delay) => {
                    retry_delay = std::cmp::min(retry_delay * 2, tokio::time::Duration::from_secs(60));
                }
            }
        }                   
    }

    async fn stream_prices(&self, ws_stream: WsStream, shutdown_token: CancellationToken) {
        let (mut write, mut read) = ws_stream.split();

        let (response_tx, mut response_rx) = tokio::sync::mpsc::channel::<Message>(100);

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    log::info!("Stream prices stopping due to shutdown signal");
                    break;
                },
                
                Some(msg_to_send) = response_rx.recv() => {
                    if let Err(e) = write.send(msg_to_send).await {
                        log::error!("Error sending Pong: {e}");
                        break;
                    }   
                },

                maybe_msg = read.next() => {
                    match maybe_msg {
                        Some(Ok(msg)) => {
                            if let ControlFlow::Break(_) = self.handle_ws_message(msg, &response_tx).await {
                                break; 
                            }
                        }
                        Some(Err(e)) => {
                            log::error!("Binance stream error (Connection Reset): {}", e);
                            break;
                        }
                        _ => {
                            log::warn!("Binance stream closed (None)");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_ws_message(&self, msg: Message, response_tx: &tokio::sync::mpsc::Sender<Message>) -> ControlFlow<(), ()> {
        match msg {
            Message::Text(text) => {
                self.process_text_message(&text);
                ControlFlow::Continue(())
            },
            Message::Ping(payload) => {
                if let Err(e) = response_tx.send(Message::Pong(payload)).await {
                    log::error!("Could'nt get queue Pong on channel: {e}");
                    return ControlFlow::Break(())
                }
                ControlFlow::Continue(())
            }
            Message::Close(_) => {
                log::warn!("Binance closed connection");
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        }
    }

    fn process_text_message(&self, text: &str) {
        match serde_json::from_str::<StreamTicker>(text) {
            Ok(ticker) => {
                let _ = self.tx.send(ticker.order);
            }
            Err(e) => log::error!("Deserializing Error: {e} | Text: {text}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;
    use tokio_tungstenite::tungstenite::protocol::Message;
    use dotenv::dotenv;
    use tokio_util::sync::CancellationToken;
    use tokio::sync::broadcast;
    use tokio::time::{
        advance,
        Duration
    };

    use crate::BinanceWs;

    #[test]
    fn test_process_invalid_json_does_not_panic() {
        dotenv().ok();
        let _ = env_logger::builder().is_test(true).try_init();

        let (tx, _rx) = broadcast::channel(10);
        let binance = BinanceWs::new_dummy(tx);

        binance.process_text_message("This is not a JSON");

        binance.process_text_message(r#"{"method":"ping"}"#);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_exponential_backoff_logic() {
        dotenv::dotenv().ok();
        let _ = env_logger::builder().is_test(true).try_init();

        let addr = "127.0.0.1:12345";
        let binance_ws = BinanceWs::new_with_url(format!("ws://{}", addr));

        let shutdown_token = CancellationToken::new();
        let token_clone = shutdown_token.clone();
        tokio::spawn(async move {
            binance_ws.stream_websocket(token_clone).await;
        });

        advance(Duration::from_millis(10)).await;

        for i in 1..=4 {
            let wait_time = Duration::from_secs(2u64.pow(i-1));

            let almost = wait_time - Duration::from_millis(100);
            advance(almost).await;

            advance(Duration::from_millis(101)).await;
            tokio::task::yield_now().await;

            println!("[Test] {:?} have passed, verifiying log...", wait_time);
        }    

        shutdown_token.cancel(); 
    }

    #[tokio::test]
    async fn test_ping_response_logic() {
        let (tx, _) = tokio::sync::broadcast::channel(10);
        let binance = BinanceWs::new_dummy(tx);
        
        let (response_tx, mut response_rx) = tokio::sync::mpsc::channel(10);
        
        let ping_data = vec![1, 2, 3];
        let ping_msg = Message::Ping(ping_data.clone());
        
        let result = binance.handle_ws_message(ping_msg, &response_tx).await;
        
        assert!(matches!(result, ControlFlow::Continue(())));
        
        let sent_msg = response_rx.recv().await.expect("Shoud has been an anwser message");
        if let Message::Pong(pong_data) = sent_msg {
            assert_eq!(pong_data, ping_data, "Pong must return same data than Ping");
        } else {
            panic!("Was waited a Message::Pong");
        }
    }
}
