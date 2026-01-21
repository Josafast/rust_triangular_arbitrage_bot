use crate::{
    triangle_calculator::Triangle, 
    config::get_pairs_for_triangle_arbitrage, 
    websocket::OrderTicker, 
    scaled_decimals::FastMath
};
use tokio::{sync::broadcast, time::Instant};
use tokio_util::{sync::CancellationToken};

pub async fn analyze_tickers(mut rx: broadcast::Receiver<OrderTicker>, shutdown_token: CancellationToken) {
    log::info!("Inited data receiver...");

    let mut init_timestamp = Instant::now(); 

    let pairs = get_pairs_for_triangle_arbitrage();
    let mut triangle = Triangle::new(pairs);

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                log::info!("Exiting bot...");
                break;
            },
            Ok(order) = rx.recv() => {
                let ticker_timestamp = Instant::now();
                let duration = ticker_timestamp.duration_since(init_timestamp);
                log::info!("\nTime between orders: {:?}", duration);
                init_timestamp = ticker_timestamp;

                if triangle.process_ticker(&order).is_err() {
                    continue;
                }

                /*
                log::info!("\nReceived: {:<10} | Bid: {:>15} (q:{:>12}) | Ask: {:>15} (q:{:>12})", 
                    &order.symbol, 
                    &order.bid_price,
                    &order.bid_qty,
                    &order.ask_price,
                    &order.ask_qty
                );
                */

                if let Ok(net_profit) = triangle.calculate_opportunity() {
                    log::warn!("{:.2}%", FastMath::ptc(net_profit));
                }

                let calculate_timestamp = Instant::now();
                let calculate_duration = calculate_timestamp.duration_since(ticker_timestamp);
                log::info!("\nCalculate duration: {:?}", calculate_duration);
            }
        }
    }
}
