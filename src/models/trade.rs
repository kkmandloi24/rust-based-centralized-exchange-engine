


#[derive(Debug,Clone)]
pub struct Trade{
  pub trade_id:u64,
 pub price:f64,
 pub quantity:f64,
 pub seller_order_id:u64,
 pub buyer_order_id:u64,
 pub timestamp: u64,
}