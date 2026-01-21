use dotenv::dotenv;
use tokio_util::sync::CancellationToken;
use triangular_arbitrage_bot::{BinanceWs, analyze_tickers};

mod common;

#[tokio::test]
async fn simulate_binance_multiple_tickers() {
    dotenv().ok();
    env_logger::init();

    tokio::spawn(async move {
        common::offline_tickers_server().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let shutdown_token = CancellationToken::new();
    let binance_ws = BinanceWs::new_with_url("ws://127.0.0.1:9001".to_string());

    let rx_binance = binance_ws.get_receiver();

    let cancellation_analyze = shutdown_token.clone();
    tokio::spawn(async move {
        analyze_tickers(rx_binance, cancellation_analyze).await;
    });

    binance_ws.stream_websocket(shutdown_token.clone()).await;
}
