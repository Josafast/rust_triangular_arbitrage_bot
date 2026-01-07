use dotenv::dotenv;
// use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use std::env;
use triangular_arbitrage_bot::websocket;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let binance_url = match env::var("BINANCE_WS_URL") {
        Ok(value) => value,
        Err(e) => {
            log::error!("La variable de entorno BINANCE_WS_URL no existe: {}", e);
            std::process::exit(1);
        }
    };

    log::info!("Binance URL: {}", binance_url);

    let shutdown_token = CancellationToken::new();
    // let (tx, _) = broadcast::channel(100);

    let binance_shutdown = shutdown_token.clone();
    tokio::spawn(async move {
        websocket::stream_websocket(&binance_url, binance_shutdown).await;
    });

    //log::info!("Bot corriendo. Presione Ctrl+C para salir.");

    loop {
        if shutdown_token.is_cancelled() {
            break;
        }
    }

    //tokio::signal::ctrl_c().await.expect("Error al escuchar señal de cierre");
    
    log::info!("Cerrando bot...");
}
