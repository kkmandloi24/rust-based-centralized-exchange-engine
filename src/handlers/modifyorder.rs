
use tokio::sync::oneshot;
use crate::models::{SignupRequest,SignupResponse,ClientResponse, Order,Side,OrderType, OrderStatus};
use actix_web::{web, HttpMessage,HttpRequest, HttpResponse, Responder};
use crate::db::AppState;
use serde::{Serialize, Deserialize};

use crate::engine::{MarketPair,MatchingEngine,ExchangeServer};
use crate::wal::{Wal};
use crate::engine::command::MarketCommand;


use std::time::{SystemTime, UNIX_EPOCH};
use crate::BalanceCommand;




#[derive(Deserialize)]
pub struct ModifyOrderRequest {
    pub market: String,
    pub order_id: u64,
    pub new_price: f64,
    pub new_quantity: f64,
}

pub async fn modify_order_handler(
    app_state: web::Data<AppState>,
    req: HttpRequest,
    req_body: web::Json<ModifyOrderRequest>,
) -> impl Responder {
    // 1. Extract authenticated user_id from request extensions
    let user_id = match req.extensions().get::<u32>() {
        Some(id) => *id,
        None => return HttpResponse::Unauthorized().json("Unauthorized access"),
    };


     // 2. Parse the market string into your MarketPair enum
    let market_pair = match req_body.market.to_lowercase().as_str() {
        "solusdc" => MarketPair::SolUsdc,
        "btcusdc" => MarketPair::BtcUsdc,
        "ethusdc" => MarketPair::EthUsdc,
        _ => return HttpResponse::BadRequest().json("Invalid or unsupported market pair"),
    };

    let sender= match app_state.market_senders.get(&market_pair){
        Some(sender)=>sender,
        None=>return HttpResponse::BadRequest().json(format!("No active worker found for market: {:?}", market_pair)),
    };

    let (resp_tx, resp_rx)=oneshot::channel();

    let cmd= MarketCommand::ModifyOrder { order_id:req_body.order_id,
         new_price:req_body.new_price, new_quantity:req_body.new_quantity, user_id, respond_to:resp_tx };

    if let Err(_)=  sender.send(cmd).await{
    return HttpResponse::InternalServerError().json("Market worker channel closed");

    }  


 // 7. Await the response from the MarketWorker actor
    match resp_rx.await {
        Ok(Ok(order_ack)) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "message": format!("Order #{} successfully modified", req_body.order_id),
            "ack": order_ack
        })),
        Ok(Err(err_msg)) => HttpResponse::BadRequest().json(serde_json::json!({
            "status": "error",
            "message": err_msg
        })),
        Err(_) => HttpResponse::InternalServerError().json(serde_json::json!({
            "status": "error",
            "message": "Market worker dropped response channel"
        })),
    }


}


