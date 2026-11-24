
use actix_web::{web, HttpResponse, Responder, HttpRequest, HttpMessage};
use crate::db::AppState;
use crate::models::{UserOrdersResponse};


pub async fn getallorders_handler( app_state:web::Data::<AppState>, req:HttpRequest)->impl Responder{

let user_id= match req.extensions().get::<u32>(){
  Some(id)=>*id,
  None=>return HttpResponse::Unauthorized().json("unauthorized access")


};

println!(" this is the user_id: {:?}", user_id);


match app_state.get_all_user_orders(user_id).await{
  Ok(orders)=>{
    let count=orders.len();
    HttpResponse::Ok().json(UserOrdersResponse{
      status: "success".to_string(),
      count,
      orders,
    })

  }
  Err(e)=> HttpResponse::InternalServerError().json(serde_json::json!(
    {"status":"error",
    "message":e}))
  }


}