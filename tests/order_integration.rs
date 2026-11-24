
use rust::engine::orderbook::OrderBook;
use rust::models::{Order,OrderType,OrderStatus,Side,};


#[test]
fn test_buy_sell_order_workflow(){
//Helper function to create order
fn create_order(id:u64,user_id:u32,price:f64,quantity:f64,side:Side,timestamp:u64)->Order{
  Order{
    id,
    user_id,
    price,
    quantity,
    remaining_quantity:quantity,
    order_type:OrderType::Limit,
    side,
    status:OrderStatus::New,
    timestamp,

  }
}

   // 1. Create orderbook
    let mut orderbook = OrderBook::new();

    // 2. Create orders
    let buy_order =
        create_order(1, 101, 150.0, 2.0, Side::Buy, 1000);

    let sell_order =
        create_order(2, 102, 150.0, 3.0, Side::Sell, 1001);

    // 3. Add BUY order
    let buy_trades = orderbook.add_order(buy_order);

    assert_eq!(buy_trades.len(), 0);

    // 4. Add SELL order
    let sell_trades = orderbook.add_order(sell_order);

    assert_eq!(sell_trades.len(), 1);

    // 5. Verify trade execution
    assert_eq!(sell_trades[0].price, 150.0);
    assert_eq!(sell_trades[0].quantity, 2.0);

    // 6. Verify orderbook state
    assert_eq!(orderbook.asks.len(), 1);
    assert_eq!(orderbook.bids.len(), 0);

    // 7. Verify remaining SELL quantity

  //Now we find the remaining quntity of the order which on order (sell/asks)
//first we take the order frm ordrbook

let order= orderbook.asks.iter().find(|o|o.id==2);

assert_eq!(order.unwrap().remaining_quantity, 1.0)

}
