use std::env;

#[inline(always)]
pub fn get_env(var: &str) -> String {
    env::var(var).unwrap_or_else(|e| {
        log::error!("ERROR, {} environment variable can't reachable: {}", var, e);
        std::process::exit(1);
    })
}

#[inline(always)]
pub fn get_pairs_for_triangle_arbitrage() -> [&'static str; 3] {
    let exchange_pairs = get_env("ARBITRAGE_TRIANGLE_PAIRS"); 

    let mut iter = exchange_pairs.split(',');

    let mut leak_next = || {
        let string = iter.next().unwrap_or("").to_string();
        Box::leak(string.into_boxed_str()) as &'static str
    };

    [leak_next(), leak_next(), leak_next()]
}

#[inline(always)]
pub fn get_fee() -> u128 {
    get_env("FEES").parse::<u128>().expect("FEES would be an integer")
}

#[inline(always)]
pub fn get_binance_url() -> String {
    get_env("BINANCE_WS_URL")
}
