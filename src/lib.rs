use std::time::Instant;
use std::{
    io,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio;
use tokio::prelude::*;
use tokio::timer::delay;
use std::time::Duration;
use std::error::Error;
use std::collections::HashMap;
use reqwest;
use crypto::mac::Mac;
use crypto::sha1::Sha1;
use crypto::hmac::Hmac;
use crypto::md5::Md5;
use crypto::digest::Digest;
use chrono::prelude::*;
use base64;
use percent_encoding::percent_decode;

use futures_channel::{mpsc, oneshot};

// use tokio::sync::{mpsc, oneshot};
use futures_util::{
    stream::{Stream, StreamExt},
};

type SubStreamInner = mpsc::UnboundedReceiver<String>;
type SubStreamSink = mpsc::UnboundedSender<String>;

const WORD_SEPARATOR :char =  2 as char;
const LINE_SEPARATOR :char = 1 as char;

struct InnerClient {
    tenant: String,
    access_key: String,
    access_secret: String,
    endpoint: String,
    server_list: Arc<Mutex<Option<Vec<String>>>>,
    index: AtomicUsize,
}

pub struct AcmClient {
    inner: Arc<InnerClient>,
    pending_subs: Arc<Mutex<Vec<(String, String)>>>,
    subscriptions: Arc<Mutex<HashMap<(String, String), Vec<SubStreamSink>>>>,
    listening: AtomicBool,
}

pub struct SubStream {
    group_id: String,
    data_id: String,
    inner: SubStreamInner,
}

impl Stream for SubStream {
    type Item = String;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

fn compute_md5(data: &str) -> String {
    let mut md5 = Md5::new();
    md5.input(data.as_bytes());
    let hash = md5.result_str();
    hash
}

impl InnerClient {
    pub fn new(tenant: String, access_key: String, access_secret:String, endpoint:String) -> Self {
        InnerClient{
            tenant, 
            access_key, 
            access_secret, 
            endpoint,
            server_list: Arc::new(Mutex::new(None)),
            index: AtomicUsize::new(0),
        }
    }

    async fn request(&self, url:&str,  method: Option<&str>,  data: HashMap<&str,&str>, headers: Option<HashMap<&str,&str>>) -> Result<String, Box<dyn Error>> {
        let now = Utc::now();
        let ts = now.timestamp_millis().to_string();
        let mut sign_str = {
            if data.contains_key("group") && data.contains_key("tenant") {
                format!("{}+{}", data.get("tenant").unwrap(), data.get("group").unwrap())
            } else if data.contains_key("group") {
                data.get("group").unwrap().to_string()
            } else if data.contains_key("tenant") {
                data.get("tenant").unwrap().to_string()
            } else {
                "".to_string()
            }
        };

        sign_str = format!("{}+{}", sign_str, ts);
        // println!("sign_str is {}", sign_str);

        let mut sha1 = Hmac::new(Sha1::new(), self.access_secret.as_bytes());
        sha1.input(sign_str.as_bytes());
        let sig = base64::encode(sha1.result().code());
        let params = data.iter().map(|(k, v)| {
            (k.to_string(), v.to_string())
        }).collect::<Vec<_>>();
        // println!("params is {:?}", params);

        let client = reqwest::Client::new();
        let mut req = match method {
            Some("post") => {
                client.post(url).form(&params)
            },
            _ => {
                client.get(url).query(&params)
            }
        };

        req = req
            .header("Client-Version", "0.1")
            .header("Content-Type", "application/x-www-form-urlencoded; charset=GBK")
            .header("Spas-AccessKey", &self.access_key)
            .header("timeStamp", ts.clone())
            .header("exConfigInfo", "true")
            .header("Spas-Signature", sig.clone());

        if headers.is_some() {
            let headers = headers.unwrap();
            if headers.len() > 0 {
                for (key, value) in headers {
                    req = req.header(key, value);
                }
            }
        }
        let res = req.send().await?;
        let body = res.text().await?;
        Ok(body)
    }

    async fn get_server(&self) -> Result<(String), Box<dyn Error>> {
        let index = self.index.fetch_add(1, Ordering::SeqCst);
        // let server_list = *self.server_list.clone().lock().unwrap();
        
        // if server_list.is_some() {
        //     let list :Vec<String> = server_list.unwrap().drain(..).collect();
        //     let len = list.len();
        //     return Ok((list[index % len]));
        // }

        let server_url = format!("http://{}:8080/diamond-server/diamond", self.endpoint);
        let res = reqwest::Client::new()
            .get(server_url.as_str())
            .send()
            .await?;

        let body = res.text().await?;

        let mut server_list = body.split("\n").filter(|s| s.trim().len() > 0).map(|s| s.to_string()).collect::<Vec<String>>();
        // println!("{:?}", server_list);
        let server = &server_list[0];
        *self.server_list.lock().unwrap() = Some(server_list.clone());
        return Ok(server.to_string());
    }

    pub async fn poll_config(&self, probe_update: &str) -> Result<String, Box<dyn Error>> {
        let server = self.get_server().await?;
        let url = format!("http://{}:8080/diamond-server/config.do", server);

        let mut params = HashMap::new();
        params.insert("Probe-Modify-Request", probe_update);

        let mut headers = HashMap::new();
        headers.insert("longPullingTimeout", "30000");

        self.request(url.as_str(), Some("post"), params, Some(headers)).await
    } 

    pub async fn get_config(&self, data_id: &str, group_id: &str) -> Result<String, Box<dyn Error>> {
        let server = self.get_server().await?;
        let url = format!("http://{}:8080/diamond-server/config.do", server);

        let mut params = HashMap::new();
        params.insert("dataId", data_id);
        params.insert("group", group_id);
        params.insert("tenant", &self.tenant);

        self.request(url.as_str(), None, params, None).await
    }
}


impl AcmClient {
    pub fn new(tenant: String, access_key: String, access_secret:String, endpoint:String) -> Self {
        let inner = InnerClient::new(tenant, access_key, access_secret, endpoint);

        AcmClient {
            inner: Arc::new(inner),
            pending_subs: Arc::new(Mutex::new(Vec::new())),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            listening: AtomicBool::new(false),
        }
    }

    fn start_listen(&self) {
        let subscriptions = self.subscriptions.clone();
        let pending_subs = self.pending_subs.clone();
        let client = self.inner.clone();

        tokio::spawn(async move {
            let mut map :HashMap<(String, String), String> = HashMap::new();
            loop {
                let items :Vec<(String, String)> = pending_subs.lock().unwrap().drain(..).collect();

                for (data_id, group_id) in items {                                    
                    if let Ok(content) = client.get_config(&data_id, &group_id).await {
                        if content.trim() == "config data not exist" {
                            continue;
                        }
                        map.insert((data_id, group_id), compute_md5(&content));
                    } else {
                        pending_subs.lock().unwrap().push((data_id, group_id));
                    }
                }

                let mut probe_update = String::new();

                for ((data_id, group_id), hash) in &map {
                    probe_update.push_str(&data_id);
                    probe_update.push(WORD_SEPARATOR);
                    probe_update.push_str(&group_id);
                    probe_update.push(WORD_SEPARATOR);

                    probe_update.push_str(&hash);
                    probe_update.push(WORD_SEPARATOR);

                    probe_update.push_str(&client.tenant);
                    probe_update.push(LINE_SEPARATOR);
                }

                {
                    
                    let result = match client.poll_config(&probe_update).await {
                        Ok(data) => data,
                        Err(e) => {
                            continue
                        },
                    };
                    
                    // "acm_test%02DEFAULT_GROUP%02d708d81f-0bc8-4914-b461-8ba64da6c564%01\n"

                    let content = match percent_decode(result.as_bytes()).decode_utf8() {
                        Ok(data) => data,
                        Err(e) => {
                            continue;
                        },
                    };

                    let lines = content.split(LINE_SEPARATOR);
                    
                    for line in lines {
                        if line.trim().len() == 0 {
                            continue;
                        }
                        let keyArr :Vec<&str>= line.split(WORD_SEPARATOR).collect();
                        if keyArr.len() < 2 {
                            continue;
                        }

                        let data_id = keyArr[0];
                        let group_id = keyArr[1];

                        if let Ok(content) = client.get_config(&data_id, &group_id).await {
                            if content.trim() == "config data not exist" {
                                continue;
                            }

                            let key = (data_id.to_string(), group_id.to_string());
                            let new_hash = compute_md5(&content);
                            
                            if map.get(&key).unwrap().to_string() == new_hash {
                                continue;
                            }

                            map.insert(key.clone(), new_hash);

                            for mut tx in subscriptions.lock().unwrap().get(&key).unwrap() {
                                tx.unbounded_send(content.clone()).unwrap();
                            }
                        }
                    }
                }
                
            }
        });
    }

    pub fn subscribe(&mut self, data_id: &str, group_id: &str) -> SubStream {
        let (tx, rx) = mpsc::unbounded(); 
        
        let dataid = data_id.to_string();
        let groupid = group_id.to_string();
        let key = (dataid.clone(), groupid.clone());

        self.subscriptions.lock().unwrap().entry(key.clone())
            .or_insert(Vec::new())
            .push(tx);

        self.pending_subs.lock().unwrap().push(key);

        if !self.listening.swap(true, Ordering::Relaxed) {
            self.start_listen(); 
        }

        SubStream {
            data_id: dataid,
            group_id: groupid,
            inner: rx,
        }
    }

    pub fn unsubscribe() {
        
    }

    pub async fn get_config(&self, data_id: &str, group_id: &str) -> Result<String, Box<dyn Error>> {
        self.inner.get_config(data_id, group_id).await
    }
}
