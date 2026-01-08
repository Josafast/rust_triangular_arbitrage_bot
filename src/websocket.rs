use futures_util::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::{env, ops::ControlFlow};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::protocol::{Message, WebSocketConfig},
};
use tokio_util::{sync::CancellationToken};
use url::Url;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWriter = futures_util::stream::SplitSink<WsStream, Message>;

#[derive(Debug, Deserialize, Clone)]
pub struct OrderTicker {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bid_price: Decimal,
    #[serde(rename = "B")]
    pub bid_qty: Decimal,
    #[serde(rename = "a")]
    pub ask_price: Decimal,
    #[serde(rename = "A")]
    pub ask_qty: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StreamTicker {
    pub stream: String,
    #[serde(rename = "data")]
    pub order: OrderTicker,
}

use tokio::{net::TcpStream, sync::broadcast};

pub struct BinanceWs {
    url: Url,
    tx: broadcast::Sender<StreamTicker>,
    shutdown_token: CancellationToken,
}

impl BinanceWs {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        let binance_url = match env::var("BINANCE_WS_URL") {
            Ok(value) => value,
            Err(e) => {
                log::error!("La variable de entorno BINANCE_WS_URL no existe: {}", e);
                std::process::exit(1);
            }
        };

        let url = Url::parse(&binance_url).expect("Invalid Url");

        let (tx, _) = broadcast::channel(100);

        Self {
            url,
            tx,
            shutdown_token,
        }
    }

    pub fn get_receiver(&self) -> broadcast::Receiver<StreamTicker> {
        self.tx.subscribe()
    }
}

static WSCONFIG: Lazy<WebSocketConfig> = Lazy::new(|| WebSocketConfig {
    max_message_size: Some(64 * 1024),
    max_frame_size: Some(64 * 1024),
    accept_unmasked_frames: false,
    ..WebSocketConfig::default()
});

impl BinanceWs {
    pub async fn stream_websocket(&self) {
        loop {
            if self.shutdown_token.is_cancelled() {
                break;
            }

            match connect_async_with_config(self.url.clone(), Some(*WSCONFIG), true).await {
                Ok((ws_stream, _)) => {
                    log::info!("Connected to BINANCE");
                    self.stream_prices(ws_stream).await;
                }
                Err(e) => {
                    log::error!("Can't to connecto to Binance: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn stream_prices(&self, ws_stream: WsStream) {
        let (mut write, mut read) = ws_stream.split();

        loop {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => return,
                msg = read.next() => {
                    match msg {
                        Some(Ok(msg)) => {
                            if let ControlFlow::Break(_) = self.handle_ws_message(msg, &mut write).await {
                                break;
                            }
                        },
                        Some(Err(e)) => {
                            log::error!("Binance stream error: {}", e);
                            break;
                        },
                        None => {
                            log::warn!("Binance stream ended");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_ws_message(&self, msg: Message, write: &mut WsWriter) -> ControlFlow<(), ()> {
        match msg {
            Message::Text(text) => {
                self.process_text_message(&text);
                ControlFlow::Continue(())
            }
            Message::Ping(payload) => {
                if let Err(e) = write.send(Message::Pong(payload)).await {
                    log::error!("Error enviando Pong: {e}");
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            }
            Message::Close(_) => {
                log::warn!("Binance cerró la conexión");
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        }
    }

    fn process_text_message(&self, text: &str) {
        match serde_json::from_str::<StreamTicker>(text) {
            Ok(order) => {
                let _ = self.tx.send(order);
            }
            Err(e) => log::error!("Error deserializando: {e} | Texto: {text}"),
        }
    }
}
