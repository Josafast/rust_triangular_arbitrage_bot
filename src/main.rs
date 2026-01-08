use dotenv::dotenv;
use tokio_util::sync::CancellationToken;
use triangular_arbitrage_bot::BinanceWs;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let shutdown_token = CancellationToken::new();
    let binance_ws = BinanceWs::new(shutdown_token.clone());

    let mut rx = binance_ws.get_receiver();

    tokio::spawn(async move {
        log::info!("Receptor de datos iniciado...");
        while let Ok(tick) = rx.recv().await {
            log::info!("Recibido: {:<10} | Bid: {:>15} (q:{:>12}) | Ask: {:>15} (q:{:>12})", 
                &tick.order.symbol, 
                &tick.order.bid_price,
                &tick.order.bid_qty,
                &tick.order.ask_price,
                &tick.order.ask_qty
            );
        }
    });

    binance_ws.stream_websocket().await;
}
