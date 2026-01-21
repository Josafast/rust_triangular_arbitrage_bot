use serde::{Deserialize, Deserializer};

const SCALE: u128 = 1_000_000_000;
const DECIMALS: u32 = 9;

#[inline(always)]
fn str_to_u128(string_var: &str) -> u128 {
    let mut parts = string_var.split('.');
    let integer_part = parts.next().unwrap_or("0").parse::<u128>().unwrap_or(0);
    let fraction_part = parts.next().unwrap_or("");

    let result = integer_part * SCALE;

    let mut fraction_val = 0u128;
    let mut len = 0;
    for c in fraction_part.chars() {
        if let Some(digit) = c.to_digit(10) {
            fraction_val = fraction_val * 10 + digit as u128;
            len += 1;
            if len == DECIMALS { break; }
        }
    }

    if len < DECIMALS {
        fraction_val *= 10u128.pow(DECIMALS - len);
    };

    result + fraction_val
}

#[inline(always)]
pub fn deserialize_price_to_u128<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    
    Ok(str_to_u128(s))
}

pub struct FastMath;

impl FastMath {
    #[inline(always)]
    pub fn mul(a: u128, b: u128) -> u128 {
        (a * b) / SCALE
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn div(a: u128, b: u128) -> u128 {
        (a * SCALE) / b
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn to_printable(val: u128) -> f64 {
        val as f64 / SCALE as f64
    }

    #[inline(always)]
    pub fn ptc(val: u128) -> f64 {
        let gain_or_loss = (val as f64 - SCALE as f64) / SCALE as f64;
        gain_or_loss * 100.0
    }
    
    #[inline(always)]
    pub fn inverse_div(val: u128) -> u128 {
        (SCALE * SCALE) / val
    }

    #[inline(always)]
    pub fn saturate(val: u128) -> u128 {
        SCALE.saturating_sub(val)
    }

    #[inline(always)]
    pub fn compare_scale(val: u128) -> bool {
        val > SCALE
    }
}
