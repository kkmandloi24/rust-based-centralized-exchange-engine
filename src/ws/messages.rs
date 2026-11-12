use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", content = "data")] // Helpful for frontend parsing
pub enum MarketEvent {
    Snapshot {
        bids: Vec<(f64, f64)>, // [(price, size), ...]
        asks: Vec<(f64, f64)>,
        last_update_id: u64,
    },
    Update {
        side: String, // "buy" or "sell"
        price: f64,
        total_quantity: f64, //order remained at this price becuase on front -end we show at price 64 we have 100 orders
        last_update_id: u64,
    },
    Cancel{
        side:String,
        price:f64,
        total_quantity:f64,
        last_updated_id:u64
    }
   
}

#[derive(serde::Deserialize, Debug)]
pub struct ClientCommand {
    pub action: String, // e.g., "subscribe" or "unsubscribe"
    pub market: String, // e.g., "BTC-USDT"
}
