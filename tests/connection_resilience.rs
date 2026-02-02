use std::time::Duration;
use tokio::{
    sync::broadcast
};
use triangular_arbitrage_bot::BinanceWs;
use tokio_util::sync::CancellationToken;

mod common;

#[tokio::test]
#[ignore]
async fn bot_reconnects_after_server_disconnection() {
    dotenv::dotenv().ok();
    let _ = env_logger::builder().is_test(true).try_init();
    
    let addr = "127.0.0.1:9002";
    let (tx, rx) = broadcast::channel::<common::ServerControl>(100);
    let shutdown_token = CancellationToken::new();

    common::setup_offline_websocket_server(addr.parse().unwrap(), rx, shutdown_token.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let binance_ws = BinanceWs::new_with_url(format!("ws://{}", addr));
    let mut rx_binance = binance_ws.get_receiver();
    let cancellation_binance = shutdown_token.clone();
    let handler = tokio::spawn(async move {
        binance_ws.stream_websocket(cancellation_binance).await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
 
    send_messages(tx.clone());
    receiving_messages(&mut rx_binance).await;
    
    println!("[Test] First spread validated");

    println!("[Test] Dropping connection...");
    tx.send(common::ServerControl::DropConnection).unwrap();
    
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("[Test] Sending spread post-connection...");
    send_messages(tx.clone());
    receiving_messages(&mut rx_binance).await;
    
    println!("[Test] ¡SUCCESSFUL TEST!");

    shutdown_token.cancel();
    let _ = handler.await;
}

fn send_messages(tx: broadcast::Sender<common::ServerControl>) {
    for i in 1..=5 {
        let msg = format!(r#"{{"stream":"test","data":{{"s":"BTCUSDT","u":{},"b":"1.0","B":"1.0","a":"1.0","A":"1.0"}}}}"#, i);
        tx.send(common::ServerControl::Message(msg)).unwrap();
    }
}

async fn receiving_messages<T>(rx: &mut tokio::sync::broadcast::Receiver<T>)
where T: Clone + Send + Sync + 'static
{
    for _ in 1..=5 {
        let _order = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("Timeout spread")
            .expect("Closed channel");
    }
}
