use crate::{config::get_fee, websocket::OrderTicker};
use crate::scaled_decimals::FastMath;

pub struct Triangle {
    symbol_a: &'static str,
    symbol_b: &'static str,
    symbol_c: &'static str,

    a_price: u128,
    b_price: u128,
    c_price: u128,
    
    fee_ratio: u128,

    ab_reciprocal: u128,
}

impl Triangle {
    pub(crate) fn new(pairs: [&'static str; 3]) -> Self {
        let [symbol_a, symbol_b, symbol_c] = pairs;

        Self {
            symbol_a,
            symbol_b,
            symbol_c,
            a_price: 0,
            b_price: 0,
            c_price: 0,
            fee_ratio: FastMath::saturate(get_fee()),
            ab_reciprocal: 0
        }
    }

    fn update_reciprocal(&mut self) {
        let denominator = FastMath::mul(self.a_price, self.b_price);
        if denominator > 0 {
            self.ab_reciprocal = FastMath::inverse_div(denominator);
        }
    }

    pub(crate) fn process_ticker(&mut self, order: &OrderTicker) -> Result<bool, bool> {
        let (old_price, price, index) = if order.symbol == self.symbol_a {
            (&mut self.a_price, order.ask_price, 0)
        } else if order.symbol == self.symbol_b {
            (&mut self.b_price, order.ask_price, 1)
        } else if order.symbol == self.symbol_c {
            (&mut self.c_price, order.bid_price, 2)
        } else {
            return Err(false);
        };

        if *old_price == price {
            return Err(false);
        }
        *old_price = price;

        if index == 0 || index == 1 { self.update_reciprocal(); }

        Ok(true)
    }

    fn get_ratio_margin(&self) -> u128 {
        FastMath::mul(self.c_price, self.ab_reciprocal)
    }

    fn get_net_profit(&self, brute_profit: u128) -> u128 {
        let fee_ratio_square = FastMath::mul(self.fee_ratio, self.fee_ratio);
        let fee_ratio_cube = FastMath::mul(fee_ratio_square, self.fee_ratio);

        FastMath::mul(brute_profit, fee_ratio_cube)
    }

    pub(crate) fn calculate_opportunity(&self) -> Result<u128, bool> {
        let brute_profit = self.get_ratio_margin();
        let net_profit = self.get_net_profit(brute_profit);

        if FastMath::compare_scale(net_profit) {
            Ok(net_profit)
        } else {
            Err(false)
        }
    }
}
