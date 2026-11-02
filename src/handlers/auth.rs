// mod models;

use crate::models::{SignupRequest,SignupResponse,ClientResponse, LoginResponse};
use crate::models::users::{User};
use actix_web::{web, HttpResponse, Responder};
use crate::utils::jwt::create_jwt;
use crate::db::AppState;

use sqlx::{Row,PgPool};
use actix_web::error::{ErrorInternalServerError,};
use bcrypt::{hash,verify};
// or argon2 depending on your crate

///Signup with the  Supbase Db query saving user in DB
// 1). SIGNUP HANDLER
// pub async fn sign_up(
//     body: web::Json<SignupRequest>, 
//     app_state: web::Data<AppState>
// ) -> impl Responder {
    
   
//     // 1. Check if username or email already exists in Supabase
//     let existing_user = sqlx::query(
//         "SELECT id FROM Users WHERE username = $1 OR email = $2",
//         body.username,
//         body.email
//     )
//     .fetch_optional(&app_state.db_pool)
//     .await;

//     match existing_user {
//         Ok(Some(_)) => {
//             return HttpResponse::Conflict().json(SignupResponse {
//                 message: String::from("User already exists"),
//                 token: None,
//             });
//         }
//         Err(err) => {
//             eprintln!("Database check failed: {:?}", err);
//             return HttpResponse::InternalServerError().json(ClientResponse {
//                 message: String::from("Something went wrong"),
//             });
//         }
//         _ => {} // User doesn't exist, proceed
//     }

//     // 2. Hash the password
//     let hashed_password = match bcrypt::hash(&body.password, DEFAULT_COST) {
//         Ok(hash) => hash,
//         Err(err) => {
//             println!("Password hashing error: {}", err);
//             return HttpResponse::InternalServerError().json(ClientResponse {
//                 message: String::from("Something went wrong"),
//             });
//         }
//     };

//     // 3. Insert user into Supabase PostgreSQL table & return generated ID
//     let insert_result = sqlx::query(
//         r#"
//         INSERT INTO Users (username, email, password_hash)
//         VALUES ($1, $2, $3)
//         RETURNING id
//         "#,
//         body.username,
//         body.email,
//         hashed_password
//     )
//     .fetch_one(&app_state.db_pool) //getting pool
//     .await;

//     let user_id = match insert_result {
//         Ok(record) => record.id,
//         Err(err) => {
//             eprintln!("Failed to insert user into Supabase: {:?}", err);
//             return HttpResponse::InternalServerError().json(ClientResponse {
//                 message: String::from("Could not register user"),
//             });
//         }
//     };

//     // 4. Create JWT token using the database user ID
//     let token = match create_jwt(user_id) {
//         Ok(token) => token,
//         Err(err) => {
//             println!("JWT creation failed: {}", err);
//             return HttpResponse::InternalServerError().json(ClientResponse {
//                 message: String::from("Could not create a token"),
//             });
//         }
//     };

//     HttpResponse::Created().json(SignupResponse {
//         message: String::from("Successfully Signup"),
//         token: Some(token),
//     })
// }


// //1). SIGNUP without the Supabase 
// pub async fn sign_up(body: web::Json<SignupRequest>, app_state:web::Data<AppState>) -> impl Responder {
 
//     //locking the app_state variable
//     let mut users=app_state.users.lock().unwrap();

//     //lock the user id creation/till this thread complete
//     let mut user_index=app_state.user_index.lock().unwrap();

// let user_found= users.iter().find(|u| u.username == body.username);
// //     println!("User not exist create the user")


//     if user_found.is_some() {
//        return HttpResponse::Conflict().json(SignupResponse {
//            message: String::from("User already exists"),
//            token: None,
//        })

// };

  
// //existing user
// // let user= match users.iter().find(|u| u.username == body.username){
// //   Some(user)=>{
// //     return Err(ErrorConflict("User {user} already exists"));
// //   }
// //   None=>{
// //     println!("User not exist create the user")


// //   }
// // };

  

// //if user not exist/user is first time
// *user_index += 1;
// let new_user_id = *user_index;
//     //  let hashed_password=  hash(&body.password,12).map_err(|e| format!("Password hashing failed: {e}"))?;
     
//      //creating hashed password//gives result need to handle ok and Err
//      let hashed_password= match hash(&body.password, 12){
//         Ok(hashed_password)=>hashed_password,
//         Err(err) => {
//         println!("There was an error creating the hashed password: {}", err);

//     return HttpResponse::InternalServerError().json(ClientResponse {
//     message: String::from("Something went wrong"),
// })
//     },
//   };
         
     
// //saving user in the DB
//     users.push(User{
//         id:new_user_id,
//         username:body.username.clone(),
//         password:hashed_password,
//         email:body.email.clone(),
//         pass_change_at: None,
       
//     });
//     //  pass_change_at:Option<u64>
//     drop(users);
//     drop(user_index);

    
// //Create Token
//   let token= match create_jwt(new_user_id){
//     Ok(token)=>token,
//     Err(err)=>{
//         println!("JWT creation failed {}", err);
//         return HttpResponse::InternalServerError().json(ClientResponse {
//     message: String::from("Could not create a token"),
// })

// }
// // Return success response with the created token
//     // HttpResponse::Ok().json(SignupResponse {
//     //     message: String::from("User registered successfully"),
//     //     token: Some(token), // Wrap your token in Some()
//     // })
    

//   };
 
//    return HttpResponse::Created().json(SignupResponse {
//         message: String::from("Successfully Signup"),
//         token: Some(token),
//     })
      
// }

pub async fn sign_up(
    body: web::Json<SignupRequest>, 
    app_state: web::Data<AppState>
) -> impl Responder {
    
    // 1. Check if username or email already exists in Supabase
    let user_found = sqlx::query(
        "SELECT id FROM users WHERE username = $1 OR email = $2"
    )
    .bind(&body.username)
    .bind(&body.email)
    .fetch_optional(&app_state.db_pool)
    .await;

    match user_found {
        Ok(Some(_)) => {
            return HttpResponse::Conflict().json(SignupResponse {
                message: String::from("User already exists"),
                token: None,
            });
        }
        Ok(None) => {
            // User does not exist, proceed with signup
        }
        Err(err) => {
            println!("Database error checking user: {}", err);
            return HttpResponse::InternalServerError().json(ClientResponse {
                message: String::from("Something went wrong"),
            });
        }
    }

    // 2. Hash the password
    let hashed_password = match hash(&body.password, 12) {
        Ok(hash) => hash,
        Err(err) => {
            println!("There was an error creating the hashed password: {}", err);
            return HttpResponse::InternalServerError().json(ClientResponse {
                message: String::from("Something went wrong"),
            });
        },
    };

    // 3. Insert the new user into Supabase and retrieve their database ID
    // Assumes your database table columns are: username, email, password_hash
    // and `id` is a serial/BIGSERIAL or IDENTITY column.
    let insert_result = sqlx::query_scalar::<_, i32>(
        "INSERT INTO users (username, email, password_hash) VALUES ($1, $2, $3) RETURNING id"
    )
    .bind(&body.username)
    .bind(&body.email)
    .bind(&hashed_password)
    .fetch_one(&app_state.db_pool)
    .await;

    let new_user_id = match insert_result {
        Ok(id) => id as u32, // Cast back to u32 if your JWT function expects it
        Err(err) => {
            println!("Failed to insert user into database: {}", err);
            return HttpResponse::InternalServerError().json(ClientResponse {
                message: String::from("Could not register user"),
            });
        }
    };

    // 4. Create JWT Token using the new user ID
    let token = match create_jwt(new_user_id) {
        Ok(token) => token,
        Err(err) => {
            println!("JWT creation failed {}", err);
            return HttpResponse::InternalServerError().json(ClientResponse {
                message: String::from("Could not create a token"),
            });
        }
    };

    // 5. Return success response with token
    HttpResponse::Created().json(SignupResponse {
        message: String::from("Successfully Signup"),
        token: Some(token),
    })
}


/////////////////////////////////////

// 2) LOGIN HANDLER
// pub async fn login(
//     body: web::Json<SignupRequest>, 
//     app_state: web::Data<AppState>
// ) -> impl Responder {

//     // 1. Fetch user from Supabase by username
//     let user_record = sqlx::query(
//         r#"
//         SELECT id, password_hash FROM Users WHERE username = $1
//         "#,
//         body.username
//     )
//     .fetch_optional(&app_state.db_pool) 
//     .await;

//     let user = match user_record {
//         Ok(Some(record)) => record,
//         Ok(None) => {
//             return HttpResponse::Unauthorized().json(ClientResponse {
//                 message: String::from("Invalid username or password"),
//             });
//         }
//         Err(err) => {
//             eprintln!("Database error during login: {:?}", err);
//             return HttpResponse::InternalServerError().json(ClientResponse {
//                 message: String::from("Something went wrong"),
//             });
//         }
//     };

//     // 2. Verify password against hash stored in Supabase
//     let correct_password = bcrypt::verify(&body.password, &user.password_hash).unwrap_or(false);

//     if !correct_password {
//         return HttpResponse::Unauthorized().json(ClientResponse {
//             message: String::from("Invalid username or password"),
//         });
//     }

//     // 3. Generate JWT Token on successful verification
//     let token = match create_jwt(user.id) {
//         Ok(token) => token,
//         Err(err) => {
//             println!("JWT creation failed: {}", err);
//             return HttpResponse::InternalServerError().json(ClientResponse {
//                 message: String::from("Could not create token"),
//             });
//         }
//     };

//     HttpResponse::Ok().json(LoginResponse {
//         message: String::from("Login Successfully"),
//         token: Some(token),
//     })
// }





//2) LOGIN WITHOUT SUPBASE DB ENTRY (my own written)
// pub async fn login(body: web::Json<SignupRequest>, app_state:web::Data<AppState>) -> impl Responder {

//     let  users=app_state.users.lock().unwrap();

//   let user= match users.iter().find(|u| u.username==body.username){
//     Some(user)=>user,
//     None=>{
//         return HttpResponse::Unauthorized().json(ClientResponse{
//             message:String::from("InValid username and password"),
            
//         })
//     }
//   };

 

//   //matching the password user inputs password onlogin field and one which is saved in DB
//   //Db password is hashed , so we have to hashed he input password and compare them



//   let correct_password= match verify(&body.password, &user.password){
//     Ok(correct_password)=>correct_password,
//     Err(_)=>{
//         return HttpResponse::Unauthorized().json(ClientResponse{
//             message:String::from("InValid password"),
           
//         })

//     }

//   };

//     if !correct_password{
//          return HttpResponse::Unauthorized().json(ClientResponse {
//         message: String::from("Invalid username and password"),

//     });

//     }

//     //If User is ahs right password and username then we create the token

//   let user_id= user.id;

//   drop(users);

//   let token= match create_jwt(user_id){
//     Ok(token)=>token,
//     Err(err)=>{
//         println!("JWT creation failed: {}", err);
//         return HttpResponse::InternalServerError().json(ClientResponse{
//             message:String::from("could not create token"),

//         })
//     }
//   };


//         HttpResponse::Ok().json(LoginResponse {
//         message: String::from(" Login Successfully"),
//         token: Some(token),
//     })

//     }





pub async fn login(
    body: web::Json<SignupRequest>, 
    app_state: web::Data<AppState>
) -> impl Responder {

    // 1. Query the user's id and password_hash from Supabase by username
    let user_result = sqlx::query(
        "SELECT id, password_hash FROM users WHERE username = $1"
    )
    .bind(&body.username)
    .map(|row: sqlx::postgres::PgRow| {
        let id: i32 = row.get("id");
        let password_hash: String = row.get("password_hash"); // Use owned String instead of &str
        (id, password_hash)
    })
    .fetch_optional(&app_state.db_pool)
    .await;

    // 2. Handle database result & check if user exists
    let (user_id, password_hash) = match user_result {
        Ok(Some(record)) => record,
        Ok(None) => {
            return HttpResponse::Unauthorized().json(ClientResponse {
                message: String::from("Invalid username and password"),
            });
        }
        Err(err) => {
            println!("Database error during login: {}", err);
            return HttpResponse::InternalServerError().json(ClientResponse {
                message: String::from("Something went wrong"),
            });
        }
    };

    // 3. Verify the password hash against user input
    let correct_password = match verify(&body.password, &password_hash) {
        Ok(valid) => valid,
        Err(_) => false,
    };

    if !correct_password {
        return HttpResponse::Unauthorized().json(ClientResponse {
            message: String::from("Invalid username and password"),
        });
    }

    // 4. Generate JWT Token using the database user ID
    let token = match create_jwt(user_id as u32) {
        Ok(token) => token,
        Err(err) => {
            println!("JWT creation failed: {}", err);
            return HttpResponse::InternalServerError().json(ClientResponse {
                message: String::from("Could not create token"),
            });
        }
    };

    // 5. Return success response with token
    HttpResponse::Ok().json(LoginResponse {
        message: String::from("Login Successfully"),
        token: Some(token),
    })
}