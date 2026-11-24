
//we are using this file experienmenting the unit test

fn add(a:i32,b:i32)->i32{
  a+b
}

#[cfg(test)]
mod tests{
  use super::*;

  #[test]
  fn fn_add_test(){
    let result=add(10,5);
    assert_eq!(result,15);
  }
}

