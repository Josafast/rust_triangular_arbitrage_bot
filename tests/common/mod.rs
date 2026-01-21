use csv::ReaderBuilder;
use futures_util::{SinkExt};
use tokio::{net::TcpListener, sync::broadcast, time::Instant};
use tokio_tungstenite::tungstenite::Message;

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

async fn setup_offline_websocket_server(mut rx: broadcast::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:9001").await.unwrap();

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();

            while let Ok(msg_content) = rx.recv().await {
                if (ws_stream.send(Message::Text(msg_content)).await).is_err() { break; }
            }
        }
    });
}

pub async fn offline_tickers_server() {
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
    
    let (tx, rx) = broadcast::channel::<String>(100);
    setup_offline_websocket_server(rx).await;

    for (symbol, path) in files {
        let tx_clone = tx.clone();
        
        tokio::spawn(async move {
            let reader = csv::Reader::from_path(path);

            for result in reader.expect("REASON").records() {
                let record = result.unwrap();
                 
                // Calculate time to send
                let event_time = record[5].parse::<u64>().unwrap_or(0);
                if event_time < global_time_timestamp { continue; }
                let delay = std::time::Duration::from_millis(event_time - global_time_timestamp);
                let objetive_time = init_timestamp + delay;
                let now = Instant::now();
                if objetive_time > now {
                    tokio::time::sleep(objetive_time - now).await;
                }

                let msg_content = format!("{{\"stream\":\"{}\",\"data\":{{\"u\":{},\"s\":\"{}\",\"b\":\"{}\",\"B\":\"{}\",\"a\":\"{}\",\"A\":\"{}\"}}}}", "btcusdt@bookTicker", &record[0], symbol, &record[1], &record[2], &record[3], &record[4]);
                if tx_clone.send(msg_content).is_err() { continue; }
            }
        });
    }
}
