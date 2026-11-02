use serde::{Serialize, Deserialize};


#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize, Deserialize)]
pub enum Side{
  Buy,
  Sell
}

#[derive(Debug, Clone, Copy, PartialEq, Eq,Serialize, Deserialize)]
pub enum OrderType {
    Limit,  // Executed at a specific price or better
    Market, // Executed immediately at the best available price
}

#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize, Deserialize)]
pub enum OrderStatus{
  New,
  PartiallyFilled,
  Filled,
  Cancelled,
}

#[derive(Debug,Clone,Serialize, Deserialize,)]
pub struct Order{
 pub id:u64,
 pub user_id: u32,
 pub price:f64,    
 pub quantity:f64,
 pub remaining_quantity:f64,  //to track the partial filled
 pub order_type: OrderType,
 pub side:Side,
 pub status:OrderStatus,
 pub timestamp: u64,      // Used to break price ties (First-In, First-Out)

}



//for get all order handler response: 
#[derive(Serialize)]
pub struct UserOrdersResponse {
    pub status: String,
    pub count: usize,
    pub orders: Vec<PublicOrderDto>,
}

/// A clean DTO (Data Transfer Object) for external clients. 
/// This protects internal engine fields if needed and formats numbers nicely.
#[derive(Serialize, Clone)]
pub struct PublicOrderDto {
    pub id: u64,
    pub price: f64,
    pub quantity: f64,
    pub remaining_quantity: f64,
    pub side: String, // "Buy" or "Sell"
}



