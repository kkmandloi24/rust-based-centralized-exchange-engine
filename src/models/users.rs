use serde::Deserialize;


#[derive(Debug, Deserialize)]
pub struct User {
   pub id: u32,
   pub username: String,
   pub email:String,
   pub password: String,
   pub pass_change_at: Option<u64>,
}







