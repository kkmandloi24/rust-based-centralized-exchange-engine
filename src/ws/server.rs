
//we are using this file experienmenting the unit test

// fn add(a:i32,b:i32)->i32{
//   a+b
// }

// #[cfg(test)]
// mod tests{
//   use super::*;

//   #[test]
//   fn fn_add_test(){
//     let result=add(10,5);
//     assert_eq!(result,15);
//   }
// }


// /////////////////////////
// CREATE TABLE user(
//   id BIGSERIAL PRIMARY KEY
//   user_name VARCHAR(50) NOT NULL,
//   email TEXT UNIQUE NOT NULL,
//   password_hash TEXT NOT NULL,
// );

// CREATE TABLE balance(
//   id BIGSERIAL PRIMARY KEY,
//   user_id BIGINT NOT NULL
//   RFERENCES user(id),
//   asset TEXT NOT NULL,
//   available NUMERIC(30,10) NOT NULL DEFAULT 0,
//   locked NUMERIC (30,10) NOT NULL DEFAULT 0

// )


// CREATE TABLE person(

//   id INT,
//   name VARCHAR(100),
//   city VARCHAR(100),
// );


// ALTER TABLE balance 
// ADD Constraint uq_user_asset UNIQUE(user_id, asset);


// INSERT INTO balance (id, user_id, asset, available, locked)
// VALUES(1,2,'Solusdc',30.0, 20.0);


// //Now we get the all data from the balance
// SELECT * FROM balance;

// Select * from balance 
// where user_id=1 AND asset='Btcusdc'


// SELECT available, locked,asset
// FROM balance;

// UPDATE balance 
// SET available=available+10
// WHERE user_id=1 AND asset='btcusdc';


// //Update the column valaue 
// UPDATE balance 
// SET available=available+10,
// locked=locked-10
// WHERE user_id=1 AND asset='Btcusdc';
// SELECT * from balance;

// DELETE FROM balance
// WHERE user_id=2 AND asset='Sousdc';
// SELECT * FROM balance;


// CREATE TABLE (
//   id BIGSERIAL PRIMARY KEY,
//   user_id BIGINT UNIQUE NOT NULL,
//   asset TEXT UNIQUE NOT NULL
//   available NUMERIC(30,10) NOT NULL DEFAULT 0,
//   locked NUMERIC(30,10) NOT NULL DEFAULT 0,

// );

// use sqlx::PgPool;

// struct CreatePerson{
//   nam:String,
//   city:String,
// }


// async fn create_row(pool:&PgPool, person:CreatePerson)-> Result<(),sqlx::Error>{

//   sqlx::query(
//     r#"
//     INSERT INTO person(name,city)
//     VALUE($1,$2);
//     "#,
//     bind(person.name),
//     bind(pesron.city),
//   ).execute(pool).await?
//   Ok(())

// }

///////////////////////////////////////////////////////////
use serde::{Serialize,Deserialize};
use std::collections::HashMap;

//wallet structure deining 
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetInfo{
  pub id: u32,
  pub name:String,
  pub symbol:AssetSymbol,
  pub decimal:u8

}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AssetSymbol{
  SOL,
  ETH,
  BTC,
  USDT,
  USDC
}

pub fn get_asset_registry()->Vec<AssetInfo>{
vec![ 
AssetInfo{id:1,name:"Solana".into(), symbol:AssetSymbol::SOL,decimal:9 }, 
AssetInfo{id:2,name:"BTC".into(), symbol:AssetSymbol::BTC,decimal:6}, 
AssetInfo{id:3,name:"Ethereum".into(), symbol:AssetSymbol::ETH,decimal:18}, 
AssetInfo{id:4,name:"Tehter".into(), symbol:AssetSymbol::USDT,decimal:6}, 
AssetInfo{id:5,name:"US Dollar".into(), symbol:AssetSymbol::USDC,decimal:2}, 
]

}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetBalance{
 pub available:f64,
 pub locked:f64
}

fn get_user_default_balance()->HashMap<AssetSymbol, AssetBalance>{

  let mut wallet= HashMap::new();
  let registery= get_asset_registry();

  for asset in registery{
    let formated= format!("{:.1$}", 0.0, asset.decimal as usize);

    wallet.insert(asset.symbol, AssetBalance{
      available:formated.clone().parse().unwrap_or(0.0),
      locked:formated.parse().unwrap_or(0.0)
    });
  }
  wallet
}

//We Store ALL USER WALLET IN DbSTORE
//User wallet HashMap<userid, wallet/Hashmap<AssetSymbol, AssetBalance>
pub type BalanceStore = HashMap<u32, HashMap<AssetSymbol, AssetBalance>>;


//CHECKED THE DB IF THE USER HAND WALET EIST IF NOT THEN CRETE WALLET
pub fn get_default_balance(db:&mut BalanceStore, user_id:u32)-> &mut HashMap<AssetSymbol, AssetBalance>{
  db.entry(user_id).or_insert_with(get_user_default_balance)
}


// #[cfg(test)]
// mod tests{
//   use super::*;

//   #[test]
//   fn testing_wallet(){

//     // let wallet = HashMap::new()
//     // let wallet_get_user_default_balance();

//     let user_id:u32=3;
//     let mut db:BalanceStore= HashMap::new();

//     // let db:DbStore=HashMap<u32, HashMap<AssetSymbol, AssetBalance>>;

//     let user_wallet= get_default_balance(&mut db,user_id);
//     let sol_balance=user_wallet.get_mut(&AssetSymbol::SOL);





//     //Now we create user wallet


//     assert!(sol_balance.unwrap().available<=f64::EPSILON);


//   }
// }






