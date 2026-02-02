use dotenv::dotenv;
use tokio::{signal, sync::broadcast};
use tokio_util::sync::CancellationToken;
use triangular_arbitrage_bot::{BinanceWs, analyze_tickers};

mod common;

#[tokio::test]
#[ignore]
async fn simulate_binance_multiple_tickers() {
    dotenv().ok();
    let _ = env_logger::builder().is_test(true).try_init();

    let addr = "127.0.0.1:9001";
    let (tx, rx) = broadcast::channel::<common::ServerControl>(100);
    let shutdown_token = CancellationToken::new();
    common::setup_offline_websocket_server(addr.parse().unwrap(), rx, shutdown_token.clone()).await;

    let offline_tickers_server_handler = tokio::spawn(async move {
        common::offline_tickers_server_sender(tx.clone()).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let binance_ws = BinanceWs::new_with_url(format!("ws://{}", addr));

    let rx_binance = binance_ws.get_receiver();

    let cancellation_analyze = shutdown_token.clone();
    let analyze_handler = tokio::spawn(async move {
        analyze_tickers(rx_binance, cancellation_analyze).await;
    });

    let cancellation_binance = shutdown_token.clone();
    let binance_ws_handler = tokio::spawn(async move {
        binance_ws.stream_websocket(cancellation_binance).await;
    });

    tokio::select! {
        _ = signal::ctrl_c() => log::info!("Shutdown token was be turned off by the user (Ctrl+C). Exiting bot..."),
        _ = shutdown_token.cancelled() => log::error!("Shutdown token was be turned off externally. Exiting bot...")
    };

    shutdown_token.cancel();

    log::info!("Closing processes...");
    let _ = tokio::join!(analyze_handler, binance_ws_handler, offline_tickers_server_handler);
    log::info!("Bot exited successfully");
}
