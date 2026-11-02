

use crate::models::{SignupRequest,SignupResponse,ClientResponse, Order,Side,OrderType, OrderStatus};
use actix_web::{web, HttpMessage,HttpRequest, HttpResponse, Responder};
use crate::db::AppState;
use serde::{Serialize, Deserialize};

use crate::engine::{MarketPair,MatchingEngine,ExchangeServer};
use crate::wal::{Wal};

use tokio::sync::oneshot;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::BalanceCommand;




#[derive(Deserialize)]
pub struct ModifyOrderRequest {
    pub market: MarketPair,
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

    // 2. Lock app state locks for server & WAL
    let mut server = app_state.server.lock().unwrap();
    let mut wal = app_state.wal.lock().unwrap();

    // 3. Invoke ExchangeServer modifying_order method
    match server
        .modifying_order(
            req_body.market,
            req_body.order_id,
            req_body.new_price,
            req_body.new_quantity,
            user_id,
            &mut wal,
        )
        .await
    {
        Ok(Ok(_)) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "message": format!("Order #{} successfully modified", req_body.order_id)
        })),
        Ok(Err(err_msg)) => HttpResponse::BadRequest().json(serde_json::json!({
            "status": "error",
            "message": err_msg
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "status": "error",
            "message": format!("Internal server error: {}", e)
        })),
    }
}