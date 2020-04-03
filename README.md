# acmrust
Rust  implementation of aliyun acm client. 阿里云acm 客户端rust实现。
https://help.aliyun.com/product/59604.html?spm=a2c4g.11186623.3.1.156916b9DgJzHa

## How to use
```rust
use std::error::Error;
use std::env;
use acm::AcmClient;
use futures_util::{
    stream::{Stream, StreamExt},
};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let tenant =  env::var("tenant").unwrap(); 
    let access_key = env::var("access_key").unwrap();
    let access_secret = env::var("access_secret").unwrap();
    let endpoint = "acm.aliyun.com".to_string();

    let mut client = AcmClient::new(tenant, access_key, access_secret, endpoint );
    // query config
    let result = client.get_config("acm_test", "DEFAULT_GROUP").await?;
    println!("result is {:?}", result);


    // subscribe config updates
    let mut stream = client.subscribe("acm_test", "DEFAULT_GROUP");

    tokio::spawn(async move {
        let mut stream = client.subscribe("acm_test", "DEFAULT_GROUP");
        while let Some(message) = stream.next().await {
            println!("config change1: {:?}", message);
        }
     });

    while let Some(message) = stream.next().await {
        println!("config change2: {:?}", message);
    }

    Ok(())
}

```