use futures_util::{/*SinkExt, */ StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Deserialize)]
struct OrderTicker {
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid_price: Decimal,
    #[serde(rename = "B")]
    bid_qty: Decimal,
    #[serde(rename = "a")]
    ask_price: Decimal,
    #[serde(rename = "A")]
    ask_qty: Decimal
}

#[derive(Debug, Deserialize)]
struct StreamTicker {
    stream: String,
    #[serde(rename = "data")]
    order: OrderTicker,
}


pub async fn stream_websocket(url: &str, token: CancellationToken) {
    let url = Url::parse(url).expect("Invalid Binance URL");

    loop {
        if token.is_cancelled() {
            break;
        }

        stream_prices(url.clone(), token.clone()).await;
    }

    token.cancel();
}

async fn stream_prices(url: Url, token: CancellationToken) {
    match connect_async(url.clone()).await {
        Ok((ws_stream, _)) => {
            log::info!("Connected to BINANCE");
            let (_, mut read) = ws_stream.split();
                loop {
                    tokio::select! {
                        _ = token.cancelled() => return,
                        msg = read.next() => {
                            match msg {
                                Some(Ok(msg)) => {
                                    match msg {
                                        Message::Text(text) => {
                                            if let Ok(order) = serde_json::from_str::<StreamTicker>(&text) {
                                                dbg!(&order);
                                            }
                                        },
                                        Message::Ping(_) => {},
                                        Message::Close(_) => {
                                            log::warn!("Binance cerró la conexión");
                                            break;
                                        },
                                        _ => {}
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
        },
        Err(e) => {
            log::error!("Can't to connecto to Binance: {e}");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }
}
