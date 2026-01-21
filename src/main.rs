use dotenv::dotenv;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use triangular_arbitrage_bot::BinanceWs;
use triangular_arbitrage_bot::analyze_tickers;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let shutdown_token = CancellationToken::new();
    let binance_ws = BinanceWs::default();

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
    let _ = tokio::join!(analyze_handler, binance_ws_handler);
    log::info!("Bot exited successfully");
}
