
use std::env;
use actix_web::{ dev::{ServiceRequest, ServiceResponse}, body::MessageBody, web, error::ErrorUnauthorized, middleware::Next, Error, HttpMessage};
use jsonwebtoken::{decode,DecodingKey, Validation};
use crate::models::auth::Claim;
use crate::db::AppState;
use sqlx::Row;
pub async fn protect(req:ServiceRequest,next:Next<impl MessageBody>)
->Result<ServiceResponse<impl MessageBody>,Error>{


  let auth_header= req.headers().get("Authorization").and_then(|h|h.to_str()
  .ok()).and_then(|h|h.strip_prefix("Bearer "));

  let token= match auth_header{
    Some(t)=>t,
    None=>return Err(ErrorUnauthorized("Missing Authorization header")),

  };


let jwt_secret = env::var("JWT_SECRET").expect("JWT_SECRET must be set");
 let decoded= match decode::<Claim>(token, &DecodingKey::from_secret(jwt_secret.as_bytes()),
 &Validation::default()).map(|data| data.claims){
  Ok(claims)=>claims,
  Err(err)=>{
    println!("JWT error: {:?}", err);
        return Err(ErrorUnauthorized("Invalid or expired token"));
  }
 };

 println!("token details:{:?}",decoded);

let app_state = req
        .app_data::<web::Data<AppState>>()
        .expect("AppState not configured");

let user_id=decoded.sub; //taken out user id from the token 
let token_issue_time=decoded.iat;

//we smartly used the conditioning, 
//01 we have user valid password changed so token is invalid not allowed to access using token
//02 we have tokenbut user deleted his account after token creation/login not allowed to acces
//in other conditions we allowed to login, so i all condition we need to check user 

//Chekinf if user cheanges his password after generating JWT token

//Stoped in memory user  management, now we are using in the Supabase
// {
//     let users = app_state.users.lock().unwrap();

//     let user = users
//         .iter()
//         .find(|u| u.id == user_id);


let user = sqlx::query(
        "SELECT id, updated_at FROM users WHERE id = $1"
    )
    .bind(user_id as i32) // Adjust type if your DB id is BIGINT / i64
    .fetch_optional(&app_state.db_pool)
    .await;


    match user {
        Ok(Some(row)) => {
            // Use row.try_get or row.get with column name string, 
            // ensuring sqlx::Row is imported at the top of the file.
            let updated_at: Option<chrono::DateTime<chrono::Utc>> = row.get("updated_at");
            
            if let Some(time) = updated_at {
                // Cast to u64 so it matches token_issue_time's type
                let update_timestamp = time.timestamp() as u64;
                
                if update_timestamp > token_issue_time {
                    return Err(ErrorUnauthorized("Profile updated, please login again"));
                }
            }
        }
        Ok(None) => {
            return Err(ErrorUnauthorized("User not exists, please signup again"));
        }
        Err(err) => {
            println!("Database error in protect middleware: {}", err);
            return Err(ErrorUnauthorized("Authentication failed"));
        }
    }

    
//sending id in the req so in the next handler we can get authenticated user id
req.extensions_mut().insert(user_id);
next.call(req).await


}