// src/engine/orderbook.rs

// src/engine/orderbook.rs

use crate::models::{Order, Side, OrderType, OrderStatus, Trade};

#[derive(Debug,)]
pub struct OrderBook {
    bids: Vec<Order>, // Buy orders (sorted descending by price)
    asks: Vec<Order>, // Sell orders (sorted ascending by price)
    next_trade_id:u64
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: Vec::new(),
            asks: Vec::new(),
            next_trade_id:1,
        }
    }

    // --- ADD THESE TWO GETTERS ---which gives clone

    pub fn get_bids(&self) -> Vec<Order> {
        self.bids.clone()
    }

    pub fn get_asks(&self) -> Vec<Order> {
        self.asks.clone()
    }

    /// Core matching engine algorithm implementing Price-Time Priority
    pub fn add_order(&mut self, mut order: Order) -> Vec<Trade> {
        let mut trades = Vec::new();

        match order.side {
            Side::Buy => {
                // A buy order matches against existing ASKS (sell orders)
                while order.remaining_quantity > 0.0 && !self.asks.is_empty() {
                    // Sort asks ascending: lowest price first, then earliest timestamp (Time Priority)
                    self.asks.sort_by(|a, b| {
                        a.price.partial_cmp(&b.price).unwrap()
                            .then_with(|| a.timestamp.cmp(&b.timestamp)).then_with(|| a.id.cmp(&b.id))
                    });

                    let best_ask = &mut self.asks[0];

                    // If it's a Limit order and the buyer's price is lower than the best ask, stop matching
                    // Market orders bypass this price check completely.
                    if order.order_type == OrderType::Limit && order.price < best_ask.price {
                        break;
                    }

                    // Calculate execution quantity
                    let trade_qty = order.remaining_quantity.min(best_ask.remaining_quantity);

                    // Record the trade (Executes at the resting order's price!)
                   let trade=Trade {
                        trade_id:self. next_trade_id, //asinginin old id then below increment
                        price: best_ask.price,
                        quantity: trade_qty,
                        buyer_order_id: order.id,
                        seller_order_id: best_ask.id,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64,
                    };

                    self.next_trade_id+=1; //incrementing the id before pushing it
                    trades.push(trade);

                    // Update remaining quantities
                    order.remaining_quantity -= trade_qty;
                    best_ask.remaining_quantity -= trade_qty;

                    // Update order statuses
                    order.status = if order.remaining_quantity == 0.0 {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };

                    best_ask.status = if best_ask.remaining_quantity == 0.0 {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };

                    // Remove fully filled ask from the order book
                    if best_ask.status == OrderStatus::Filled {
                        self.asks.remove(0);
                    }
                }

                // Post-matching handling for Buy orders
                if order.remaining_quantity > 0.0 {
                    match order.order_type {
                        OrderType::Limit => {
                            // Unfilled Limit orders rest on the bids book
                            order.status = if order.status == OrderStatus::PartiallyFilled {
                                OrderStatus::PartiallyFilled
                            } else {
                                OrderStatus::New
                            };
                            self.bids.push(order);
                        }
                        OrderType::Market => {
                            // Unfilled Market orders (due to lack of liquidity) are cancelled
                            order.status = OrderStatus::Cancelled;
                        }
                    }
                }
            }
            Side::Sell => {
                // A sell order matches against existing BIDS (buy orders)
                while order.remaining_quantity > 0.0 && !self.bids.is_empty() {
                    // Sort bids descending: highest price first, then earliest timestamp
                    self.bids.sort_by(|a, b| {
                        b.price.partial_cmp(&a.price).unwrap()
                            .then_with(|| a.timestamp.cmp(&b.timestamp)).then_with(|| b.id.cmp(&a.id))
                    });

                    let best_bid = &mut self.bids[0];

                    // If it's a Limit order and the seller's price is higher than the best bid, stop matching
                    // Market orders bypass this price check.
                    if order.order_type == OrderType::Limit && order.price > best_bid.price {
                        break;
                    }

                    let trade_qty = order.remaining_quantity.min(best_bid.remaining_quantity);

                    let trade=Trade{
                        trade_id:self.next_trade_id,
                        price: best_bid.price,
                        quantity: trade_qty,
                        buyer_order_id: best_bid.id,
                        seller_order_id: order.id,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64,
                    };

                    self.next_trade_id+=1;
                    trades.push(trade);
              

                    order.remaining_quantity -= trade_qty;
                    best_bid.remaining_quantity -= trade_qty;

                    order.status = if order.remaining_quantity == 0.0 {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };

                    best_bid.status = if best_bid.remaining_quantity == 0.0 {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };

                    if best_bid.status == OrderStatus::Filled {
                        self.bids.remove(0);
                    }
                }

                // Post-matching handling for Sell orders
                if order.remaining_quantity > 0.0 {
                    match order.order_type {
                        OrderType::Limit => {
                            order.status = if order.status == OrderStatus::PartiallyFilled {
                                OrderStatus::PartiallyFilled
                            } else {
                                OrderStatus::New
                            };
                            self.asks.push(order);
                        }
                        OrderType::Market => {
                            order.status = OrderStatus::Cancelled;
                        }
                    }
                }
            }
        }

        trades
    }

    


//Helper for MODIFY ORDER////////////////////////////

// Helper to remove an order by ID from either bids or asks, returning the order and its original index/side
    pub fn remove_order_with_location(&mut self, order_id: u64) -> Option<(Order, Side, usize)> {
        if let Some(index) = self.bids.iter().position(|o| o.id == order_id) {
            let order = self.bids.remove(index);
            return Some((order, Side::Buy, index));
        }
        if let Some(index) = self.asks.iter().position(|o| o.id == order_id) {
            let order = self.asks.remove(index);
            return Some((order, Side::Sell, index));
        }
        None
    }

/// Restore an order back to its exact original index to preserve time priority
    pub fn restore_order_at_index(&mut self, order: Order, side: Side, index: usize) {
        match side {
            Side::Buy => {
                let idx = std::cmp::min(index, self.bids.len());
                self.bids.insert(idx, order);
            }
            Side::Sell => {
                let idx = std::cmp::min(index, self.asks.len());
                self.asks.insert(idx, order);
            }
        }
    }


////////////////////////////////////////////////Modify Order///////////
pub fn modify_order(
        &mut self,
        order_id: u64,
        new_price: f64,
        new_quantity: f64,
    ) -> Result<Option<(Order, f64, bool)>, String> {
        
        // 1. Find the order immutably first to inspect and validate
        let (old_price, old_quantity, filled_quantity) = {
            let order_ref = self.bids.iter().find(|o| o.id == order_id)
                .or_else(|| self.asks.iter().find(|o| o.id == order_id));

            let order = match order_ref {
                Some(o) => o,
                None => return Ok(None), // Order not found
            };

            let old_price = order.price;
            let old_quantity = order.quantity;
            let filled = old_quantity - order.remaining_quantity;
            (old_price, old_quantity, filled)
        }; // Immutable borrow ends here!

        // 2. Validate BEFORE touching the vector structure
        if new_quantity < filled_quantity {
            return Err(format!(
                "Cannot reduce quantity below already filled amount ({})",
                filled_quantity
            ));
        }

        // 3. Now that validation passed, remove the order from its location
        let (mut target_order, side, original_index) = match self.remove_order_with_location(order_id) {
            Some(res) => res,
            None => return Ok(None),
        };

        // 4. Determine if priority is lost (Price change or size increase)
        let price_changed = (new_price - old_price).abs() > f64::EPSILON;
        let quantity_increased = new_quantity > old_quantity;

        let is_increase;
        let cost_diff;

        if price_changed || quantity_increased {
            // --- CASE A: Price Change or Quantity Increase (Loses Time Priority) ---mean order price chnages means order will change
            let old_cost = old_price * old_quantity;
            let new_cost = new_price * new_quantity;

            cost_diff = (new_cost - old_cost).abs();
            is_increase = new_cost > old_cost;

            // Update fields
            target_order.price = new_price;
            target_order.quantity = new_quantity;
            target_order.remaining_quantity = new_quantity - filled_quantity;

            // Re-insert at the back of the queue (loses time priority)
            self.add_order(target_order.clone());
        } else {
            // --- CASE B: Quantity Decrease at Same Price (Preserves Time Priority) ---
            let old_cost = old_price * old_quantity;
            let new_cost = old_price * new_quantity;

            cost_diff = (old_cost - new_cost).abs();
            is_increase = false;

            // Update fields in-place
            target_order.quantity = new_quantity;
            target_order.remaining_quantity = new_quantity - filled_quantity;

            // Put the order back at its exact original position (preserves time priority)
            self.restore_order_at_index(target_order.clone(), side, original_index);
        }

        Ok(Some((target_order, cost_diff, is_increase)))
    }


///////////////////////////////////////////////////////////////////


/// Find and remove an order from either bids or asks by its ID
    pub fn cancel_order(&mut self, order_id: u64) -> Option<Order> {
        // 1. Check if it's in the bids vector
        if let Some(index) = self.bids.iter().position(|o| o.id == order_id) {
            return Some(self.bids.remove(index));
        }

        // 2. Check if it's in the asks vector
        if let Some(index) = self.asks.iter().position(|o| o.id == order_id) {
            return Some(self.asks.remove(index));
        }

        // 3. Not found anywhere
        None
    }

}

//////////////////////////////////////////////













    


//     }//



//    }//

//    trades

//  }//


//    }//









////////////


// src/engine/orderbook.rs

// use crate::models::{Order, Side, OrderStatus, Trade};

// pub struct OrderBook {
//     bids: Vec<Order>, // Buy orders (sorted descending by price)
//     asks: Vec<Order>, // Sell orders (sorted ascending by price)
// }

// impl OrderBook {
//     pub fn new() -> Self {
//         Self {
//             bids: Vec::new(),
//             asks: Vec::new(),
//         }
//     }

//     /// Core matching engine algorithm using Price-Time Priority
//     pub fn add_order(&mut self, mut order: Order) -> Vec<Trade> {
//         let mut trades = Vec::new();

//         match order.side {
//             Side::Buy => {
//                 // A buy order matches against existing ASKS (sell orders)
//                 while order.remaining_quantity > 0.0 && !self.asks.is_empty() {
//                     // Sort asks ascending: lowest price first, then earliest timestamp
//                     self.asks.sort_by(|a, b| {
//                         a.price.partial_cmp(&b.price).unwrap()
//                             .then_with(|| a.timestamp.cmp(&b.timestamp))
//                     });

//                     let best_ask = &mut self.asks[0];

//                     // If buyer's limit price is lower than the best ask, stop matching
//                     if order.price < best_ask.price && order.order_type == crate::models::OrderType::Limit {
//                         break;
//                     }

//                     // Calculate execution quantity
//                     let trade_qty = order.remaining_quantity.min(best_ask.remaining_quantity);

//                     // Record the trade (executes at the resting order's price)
//                     trades.push(Trade {
//                         price: best_ask.price,
//                         quantity: trade_qty,
//                         buyer_order_id: order.id,
//                         seller_order_id: best_ask.id,
//                         timestamp: std::time::SystemTime::now()
//                             .duration_since(std::time::UNIX_EPOCH)
//                             .unwrap()
//                             .as_millis() as u64,
//                     });

//                     // Update remaining quantities
//                     order.remaining_quantity -= trade_qty;
//                     best_ask.remaining_quantity -= trade_qty;

//                     // Update order statuses
//                     order.status = if order.remaining_quantity == 0.0 {
//                         OrderStatus::Filled
//                     } else {
//                         OrderStatus::PartiallyFilled
//                     };

//                     best_ask.status = if best_ask.remaining_quantity == 0.0 {
//                         OrderStatus::Filled
//                     } else {
//                         OrderStatus::PartiallyFilled
//                     };

//                     // Remove fully filled ask from the order book
//                     if best_ask.status == OrderStatus::Filled {
//                         self.asks.remove(0);
//                     }
//                 }

//                 // If buy order still has quantity left, rest it on the bids book
//                 if order.remaining_quantity > 0.0 && order.order_type == crate::models::OrderType::Limit {
//                     self.bids.push(order);
//                 }
//             }
//             Side::Sell => {
//                 // A sell order matches against existing BIDS (buy orders)
//                 while order.remaining_quantity > 0.0 && !self.bids.is_empty() {
//                     // Sort bids descending: highest price first, then earliest timestamp
//                     self.bids.sort_by(|a, b| {
//                         b.price.partial_cmp(&a.price).unwrap()
//                             .then_with(|| a.timestamp.cmp(&b.timestamp))
//                     });

//                     let best_bid = &mut self.bids[0];

//                     // If seller's limit price is higher than the best bid, stop matching
//                     if order.price > best_bid.price && order.order_type == crate::models::OrderType::Limit {
//                         break;
//                     }

//                     let trade_qty = order.remaining_quantity.min(best_bid.remaining_quantity);

//                     trades.push(Trade {
//                         price: best_bid.price,
//                         quantity: trade_qty,
//                         buyer_order_id: best_bid.id,
//                         seller_order_id: order.id,
//                         timestamp: std::time::SystemTime::now()
//                             .duration_since(std::time::UNIX_EPOCH)
//                             .unwrap()
//                             .as_millis() as u64,
//                     });

//                     order.remaining_quantity -= trade_qty;
//                     best_bid.remaining_quantity -= trade_qty;

//                     order.status = if order.remaining_quantity == 0.0 {
//                         OrderStatus::Filled
//                     } else {
//                         OrderStatus::PartiallyFilled
//                     };

//                     best_bid.status = if best_bid.remaining_quantity == 0.0 {
//                         OrderStatus::Filled
//                     } else {
//                         OrderStatus::PartiallyFilled
//                     };

//                     if best_bid.status == OrderStatus::Filled {
//                         self.bids.remove(0);
//                     }
//                 }

//                 // If sell order still has quantity left, rest it on the asks book
//                 if order.remaining_quantity > 0.0 && order.order_type == crate::models::OrderType::Limit {
//                     self.asks.push(order);
//                 }
//             }
//         }

//         trades
//     }
// }




//////////////////////////////////////////////


// use crate::models::{Order, Side, OrderStatus, Trade};
// use std::collections::VecDeque;

// pub struct OrderBook {
//     // For a simple, clean portfolio MVP, we can store orders in vectors or queues.
//     // In production, matching engines use B-Tree maps or specialized price-level indexes.
//     bids: Vec<Order>, // Buy orders
//     asks: Vec<Order>, // Sell orders
// }

// impl OrderBook {
//     pub fn new() -> Self {
//         Self {
//             bids: Vec::new(),
//             asks: Vec::new(),
//         }
//     }

//     /// Core function to add an order and attempt matching
//     pub fn add_order(&mut self, mut order: Order) -> Vec<Trade> {
//         //we created a empty vector for keeping the filled orders
//         let mut trades = Vec::new();

//         match order.side {
//             Side::Buy => {
//                 // A buy order matches against existing ASKS (sell orders)
//                 // Condition: Buy price >= Ask price
//                 while order.remaining_quantity > 0.0 && !self.asks.is_empty() {
//                     // Sort asks ascending so the lowest price is always at index 0
//                     self.asks.sort_by(|a, b| {
//                         a.price.partial_cmp(&b.price).unwrap()
//                             .then_with(|| a.timestamp.cmp(&b.timestamp)) // Time priority tie-breaker
//                     });

//                     let best_ask = &mut self.asks[0];

//                     // If the buyer's price is lower than the best ask, no match can happen
//                     if order.price < best_ask.price && order.order_type == crate::models::OrderType::Limit {
//                         break;
//                     }

//                     // Determine trade quantity
//                     let trade_qty = order.remaining_quantity.min(best_ask.remaining_quantity);

//                     // Create the trade record
//                     trades.push(Trade {
//                         price: best_ask.price, // Execution happens at the resting order's price
//                         quantity: trade_qty,
//                         buyer_order_id: order.id,
//                         seller_order_id: best_ask.id,
//                         timestamp: std::time::SystemTime::now()
//                             .duration_since(std::time::UNIX_EPOCH)
//                             .unwrap()
//                             .as_millis() as u64,
//                     });

//                     // Update remaining quantities
//                     order.remaining_quantity -= trade_qty;
//                     best_ask.remaining_quantity -= trade_qty;

//                     // Update order statuses
//                     if order.remaining_quantity == 0.0 {
//                         order.status = OrderStatus::Filled;
//                     } else {
//                         order.status = OrderStatus::PartiallyFilled;
//                     }

//                     if best_ask.remaining_quantity == 0.0 {
//                         best_ask.status = OrderStatus::Filled;
//                     } else {
//                         best_ask.status = OrderStatus::PartiallyFilled;
//                     }

//                     // Remove fully filled asks from the book
//                     if best_ask.status == OrderStatus::Filled {
//                         self.asks.remove(0);
//                     }
//                 }

//                 // If the buy order still has remaining quantity, push it to the bids book
//                 if order.remaining_quantity > 0.0 && order.order_type == crate::models::OrderType::Limit {
//                     self.bids.push(order);
//                 }
//             }


//             Side::Sell => {
//                 // A sell order matches against existing BIDS (buy orders)
//                 // Condition: Sell price <= Bid price
//                 while order.remaining_quantity > 0.0 && !self.bids.is_empty() {
//                     // Sort bids descending so the highest price is always at index 0
//                     self.bids.sort_by(|a, b| {
//                         b.price.partial_cmp(&a.price).unwrap()
//                             .then_with(|| a.timestamp.cmp(&b.timestamp)) // Time priority tie-breaker
//                     });

//                     let best_bid = &mut self.bids[0];

//                     // If the seller's price is higher than the best bid, no match can happen
//                     if order.price > best_bid.price && order.order_type == crate::models::OrderType::Limit {
//                         break;
//                     }

//                     let trade_qty = order.remaining_quantity.min(best_bid.remaining_quantity);

//                     trades.push(Trade {
//                         price: best_bid.price,
//                         quantity: trade_qty,
//                         buyer_order_id: best_bid.id,
//                         seller_order_id: order.id,
//                         timestamp: std::time::SystemTime::now()
//                             .duration_since(std::time::UNIX_EPOCH)
//                             .unwrap()
//                             .as_millis() as u64,
//                     });

//                     order.remaining_quantity -= trade_qty;
//                     best_bid.remaining_quantity -= trade_qty;

//                     if order.remaining_quantity == 0.0 {
//                         order.status = OrderStatus::Filled;
//                     } else {
//                         order.status = OrderStatus::PartiallyFilled;
//                     }

//                     if best_bid.remaining_quantity == 0.0 {
//                         best_bid.status = OrderStatus::Filled;
//                     } else {
//                         best_bid.status = OrderStatus::PartiallyFilled;
//                     }

//                     if best_bid.status == OrderStatus::Filled {
//                         self.bids.remove(0);
//                     }
//                 }

//                 if order.remaining_quantity > 0.0 && order.order_type == crate::models::OrderType::Limit {
//                     self.asks.push(order);
//                 }
//             }
//         }

//         trades
//     }
// }

// /////////////////////////////////////////////////////

// // src/main.rs
// mod models;
// mod engine;

// use models::{Order, Side, OrderType, OrderStatus};
// use engine::OrderBook;

// fn main() {
//     let mut orderbook = OrderBook::new();

//     // 1. Place a Sell Order (Ask) at $100 for 2.0 BTC
//     let sell_order = Order {
//         id: 1,
//         price: 100.0,
//         quantity: 2.0,
//         remaining_quantity: 2.0,
//         side: Side::Sell,
//         order_type: OrderType::Limit,
//         status: OrderStatus::New,
//         timestamp: 1,
//     };

//     println!("Adding sell order...");
//     let trades = orderbook.add_order(sell_order);
//     println!("Trades generated: {:?}", trades);

//     // 2. Place a matching Buy Order (Bid) at $100 for 1.5 BTC
//     let buy_order = Order {
//         id: 2,
//         price: 100.0,
//         quantity: 1.5,
//         remaining_quantity: 1.5,
//         side: Side::Buy,
//         order_type: OrderType::Limit,
//         status: OrderStatus::New,
//         timestamp: 2,
//     };

//     println!("Adding matching buy order...");
//     let trades = orderbook.add_order(buy_order);
//     println!("Trades generated: {:?}", trades);
// }

// ////////////////////////////////////////////


