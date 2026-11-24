
use crate::models::{Order,Side,OrderType, OrderStatus};
use actix_web::{web, HttpMessage,HttpRequest, HttpResponse, Responder};
use crate::db::AppState;
use serde::Deserialize;

use crate::engine::MarketPair;
use crate::engine::command::MarketCommand;

use tokio::sync::oneshot;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::ledger::BalanceCommand;


#[derive(Deserialize)]
pub struct OrderRequest {
    pub market: String,       // e.g., "SolUsdc"
    pub side: Side,           // Buy or Sell
    pub order_type: OrderType,// Limit or Market
    pub price: f64,
    pub quantity: f64,
}


pub async fn place_order_handler(
    state: web::Data<AppState>,
    body: web::Json<OrderRequest>,
    req: HttpRequest,
) -> impl Responder {
    // 1. Authenticate user from request extensions
    let user_id = match req.extensions().get::<u32>().copied() {
        Some(id) => id,
        None => return HttpResponse::Unauthorized().json("Unauthorized"),
    };

    // 2. Parse market string into MarketPair enum FIRST (fixes DashMap key type mismatch)
    let market_pair = match body.market.to_lowercase().as_str() {
        "solusdc" => MarketPair::SolUsdc,
        "btcusdc" => MarketPair::BtcUsdc,
        "ethusdc" => MarketPair::EthUsdc,
        _ => return HttpResponse::BadRequest().json("Invalid or unsupported market pair"),
    };

    // 3. Determine base and quote assets
    let (base_asset, quote_asset) = match market_pair {
        MarketPair::SolUsdc => ("SOL", "USD"),
        MarketPair::BtcUsdc => ("BTC", "USD"),
        MarketPair::EthUsdc => ("ETH", "USD"),
    };

    // 4. Retrieve the sender for this specific market worker from DashMap
    let sender = match state.market_senders.get(&market_pair) {
        Some(entry) => entry.value().clone(), // Notice `.value().clone()` (method call)
        None => return HttpResponse::BadRequest().json(format!("Unsupported market pair: {:?}", market_pair)),
    };

    // 5. Calculate required asset and amount to lock (Pre-trade Risk Check)
    let (asset_to_lock, lock_amount) = match body.side {
        Side::Buy => (quote_asset, body.price * body.quantity),
        Side::Sell => (base_asset, body.quantity),
    };

    // 6. Request balance worker to safely lock funds
    let (resp_tx, resp_rx) = oneshot::channel();
    let _ = state.balance_tx.send(BalanceCommand::LockFunds {
        user_id,
        asset_identifier: asset_to_lock.to_string(),
        amount: lock_amount,
        resp: resp_tx,
    }).await;

    match resp_rx.await {
        Ok(Ok(_)) => { /* Funds successfully locked! */ }
        Ok(Err(err_msg)) => return HttpResponse::BadRequest().json(err_msg),
        Err(_) => return HttpResponse::InternalServerError().json("Balance worker communication failed"),
    }

    // 7. Construct the Order
    let current_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let order = Order {
        id: 0,   // Assigned sequentially by the MarketWorker's engine
        user_id,
        price: body.price,
        quantity: body.quantity,
        remaining_quantity: body.quantity,
        order_type: body.order_type,
        side: body.side,
        status: OrderStatus::New,
        timestamp: current_timestamp,
    };

    // 8. Dispatch command to the MarketWorker Actor and await result
    let (resp_tx, resp_rx) = oneshot::channel();
    let cmd = MarketCommand::PlaceOrder {
        order,
        respond_to: resp_tx, // Ensure field name matches your MarketCommand enum definition
    };

    if let Err(_) = sender.send(cmd).await {
        // SAFETY ROLLBACK: Unlock funds if the worker channel is closed
        let (rollback_tx, rollback_rx) = oneshot::channel();
        let _ = state.balance_tx.send(BalanceCommand::UnlockFunds {
            user_id,
            asset_identifier: asset_to_lock.to_string(),
            amount: lock_amount,
            resp: rollback_tx,
        }).await;
        let _ = rollback_rx.await;

        return HttpResponse::InternalServerError().json("Market worker channel closed");
    }

    // 9. Await response from the MarketWorker
    match resp_rx.await {
        Ok(Ok(order_ack)) => {
            HttpResponse::Ok().json(serde_json::json!({
                "status": "Success",
                "message": "Order placed and funds locked successfully",
                "ack": order_ack
            }))
        }
        Ok(Err(engine_err)) => {
            // SAFETY ROLLBACK: Unlock funds if matching engine / worker rejects the order
            let (rollback_tx, rollback_rx) = oneshot::channel();
            let _ = state.balance_tx.send(BalanceCommand::UnlockFunds {
                user_id,
                asset_identifier: asset_to_lock.to_string(),
                amount: lock_amount,
                resp: rollback_tx,
            }).await;
            let _ = rollback_rx.await;

            HttpResponse::BadRequest().json(format!("Matching engine error: {}", engine_err))
        }
        Err(_) => {
            // SAFETY ROLLBACK: Unlock funds if response channel drops
            let (rollback_tx, rollback_rx) = oneshot::channel();
            let _ = state.balance_tx.send(BalanceCommand::UnlockFunds {
                user_id,
                asset_identifier: asset_to_lock.to_string(),
                amount: lock_amount,
                resp: rollback_tx,
            }).await;
            let _ = rollback_rx.await;

            HttpResponse::InternalServerError().json("Market worker dropped response channel")
        }
    }
}



