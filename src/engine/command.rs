use tokio::sync::oneshot;
use crate::models::{Order,Side,OrderStatus,PublicOrderDto};
use crate::ws::messages::MarketEvent;


#[derive(Debug)]
pub enum MarketCommand {
    PlaceOrder {
        order: Order,
        respond_to: oneshot::Sender<Result<OrderAck, String>>,
    },
    CancelOrder {
        order_id: u64,
        user_id:u32,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    ModifyOrder {
        order_id: u64,
        new_price: f64,
        new_quantity:f64,
        user_id:u32,
        respond_to: oneshot::Sender<Result<OrderAck, String>>,
    },

    //use this command so user can get throgh it his active orders
    GetOrdersForUser { user_id: u32, resp: oneshot::Sender<Vec<PublicOrderDto>> }, // 👈 ADD THIS

    //for Socket new user sent entire orderbook snapshot, so we are adding this below command by using this 
    //offer socket snapshot a sender to send the snapshot
    // In your commands enum file

    // ... existing commands ...
    GetSnapshot { resp:oneshot::Sender<MarketEvent> 
    },

}


/////STRUCT//////////////////////////////////////////

use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize,Clone)]
pub struct OrderAck {
    pub order_id: u64,              // Unique internal ID assigned by the exchange
    // pub client_order_id: Option<String>, // Optional reference tag provided by the trader
    pub market_pair: String,        // e.g., "BTC-USDT"
    // pub status: OrderStatus,        // Accepted, PartiallyFilled, Filled, or Rejected
    // pub side: Side,            // Buy or Sell
    // pub price: u64,                 // Price fixed point (e.g., scaled integers to avoid float issues)
    // pub original_qty: u64,          // Initial quantity requested
    // pub executed_qty: u64,          // Quantity matched immediately upon arrival (if marketable)
    // pub remaining_qty: u64,         // Quantity resting on the order book
    // pub sequence_number: u64,       // The strict FIFO sequence number assigned by the market worker
    // pub timestamp: u64,             // Epoch timestamp when the engine processed it
}
