
pub mod users;   //only this - on mai we have to do use model::user::User
pub mod auth;
pub mod wallet;
//again extract User from user file, as below
// pub use users::User;   //if we use this as well with the pub mod user here then have to do only on main  use model::{User}
// pub use auth::*;
// pub use auth::{SignupRequest,SignupResponse, LoginRequest,LoginResponse, AuthResponse,ClientResponse,Claim};

pub mod order;
pub mod trade;

//with this module creatio we have to import the  above module in any file as
// we have to import from the root folder individually
// use crate::models::order::{Order,Side,OrderType,orderStatus}
//use crate::models::trade::Trade
   


//we reexport with using use here so we can import it in the any files as use crate::models::{Order,Side,OrderType,OrderStatus}
pub use order::{Order,Side,OrderType,OrderStatus,UserOrdersResponse,PublicOrderDto};
pub use trade::Trade;
pub use auth::{SignupRequest,SignupResponse,ClientResponse, LoginResponse};




