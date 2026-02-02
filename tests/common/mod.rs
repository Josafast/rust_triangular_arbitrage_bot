use std::net::SocketAddr;
use csv::ReaderBuilder;
use futures_util::{SinkExt};
use tokio::{net::TcpListener, sync::broadcast, time::{Instant, Duration}};
use tokio_util::sync::CancellationToken;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone)]
pub enum ServerControl {
    Message(String),
    #[allow(dead_code)]
    DropConnection
}

#[allow(dead_code)]
fn search_global_init_timestamp(files: &[(&str, &str)]) -> u64 {
    let mut min_time = u64::MAX;

    for (_, path) in files {
        let mut rdr = ReaderBuilder::new()
            .from_path(path)
            .expect("NO");

        if let Some(result) = rdr.records().next() {
            let record = result.expect("Error to read first line");
            let timestamp = record[6].parse::<u64>().unwrap_or(u64::MAX);

            if timestamp < min_time {
                min_time = timestamp
            }
        }
    }

    min_time
}

pub async fn setup_offline_websocket_server(
    addr: SocketAddr, 
    rx: broadcast::Receiver<ServerControl>, 
    shutdown_token: CancellationToken
) {
    let listener = TcpListener::bind(&addr).await.expect("Failed to bind");

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    let mut current_rx = rx.resubscribe();

                    tokio::spawn(async move {
                        let mut ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();

                        loop {
                            tokio::select! {
                                Ok(control) = current_rx.recv() => {
                                    match control {
                                        ServerControl::Message(msg) => {
                                            if ws_stream.send(Message::Text(msg)).await.is_err() { break };
                                        },
                                        ServerControl::DropConnection => {
                                            drop(ws_stream);
                                            break;
                                        }
                                    }
                                },
                                _ = tokio::time::sleep(Duration::from_secs(30)) => { break; }
                            }
                        }
                    });
                },
                _ = shutdown_token.cancelled() => { break; }
            }
        }
    });
}

#[allow(dead_code)]
pub async fn offline_tickers_server_sender(tx: broadcast::Sender<ServerControl>) {
    let files = vec![
        ("BTCUSDT",
         "test_book_json_files/BTCUSDT-bookTicker-2024-03-30.csv"),
        ("ETHBTC",
         "test_book_json_files/ETHBTC-bookTicker-2024-03-30.csv"),
        ("ETHUSDT",
         "test_book_json_files/ETHUSDT-bookTicker-2024-03-30.csv")
    ];

    let global_time_timestamp: u64 = search_global_init_timestamp(&files); 
    let init_timestamp = Instant::now();

    for (symbol, path) in files {
        let tx_clone = tx.clone();
        
        tokio::spawn(async move {
            let reader = csv::Reader::from_path(path);

            for result in reader.expect("REASON").records() {
                let record = result.unwrap();
                 
                let event_time = record[5].parse::<u64>().unwrap_or(0);
                if event_time < global_time_timestamp { continue; }
                let delay = std::time::Duration::from_millis(event_time - global_time_timestamp);
                let objetive_time = init_timestamp + delay;
                let now = Instant::now();
                if objetive_time > now {
                    tokio::time::sleep(objetive_time - now).await;
                }

                let msg_content = format!(r#"{{"stream":"{}","data":{{"u":{},"s":"{}","b":"{}","B":"{}","a":"{}","A":"{}"}}}}"#, "btcusdt@bookTicker", &record[0], symbol, &record[1], &record[2], &record[3], &record[4]);
                if tx_clone.send(ServerControl::Message(msg_content)).is_err() { continue; }
            }
        });
    }
}
