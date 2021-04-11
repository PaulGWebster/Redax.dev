extern crate serde_json;
extern crate serde;
extern crate redis;

use tungstenite::{connect, Message};
use url::Url;

use std::collections::HashSet;
use std::sync::mpsc::{ channel, TryRecvError };
use std::{thread, time};
use std::path::Path;

use time::{Duration};

use serde::{Deserialize, Serialize};
//use serde_json::Result;

const RECONNECT_DELAY: Duration = time::Duration::from_millis(1);
const THROTTLE_DELAY: Duration = time::Duration::from_millis(100);
const REDIS_PATH_TCP: &'static str = "redis+unix:///tmp/redis.sock";
const REDIS_PATH_UNIX: &'static str = "redis://127.0.0.1:6379";

// JSON Structures
#[derive(Serialize, Deserialize)]
struct SubscriptionJsonRootProductsElement {
    id: String,
    status: String
}
#[derive(Serialize, Deserialize)]
struct SubscriptionJsonRoot {
    products:Vec<SubscriptionJsonRootProductsElement>
}

#[derive(Serialize, Deserialize)]
struct SubscribeProductRoot {
    r#type: String,
    channels: Vec<SubscribeProductRootChannel>
}
#[derive(Serialize, Deserialize)]
struct SubscribeProductRootChannel {
    name: String, // Should be 'full'
    product_ids: Vec<String>
}
#[derive(Serialize, Deserialize)]
struct SubscribedDataPacket {
    r#type: String,
    sequence: Option<u128>,
    product_id: Option<String>
}

fn main() {
    let mut gdax_productid_online = HashSet::new();

    // Our subscription watcher thread
    let ws_subscribe_json = r#"{"channels":[{"name":"status"}],"type":"subscribe"}"#;
    let (gdax_subscription_watcher_ipc1_send, gdax_subscription_watcher_ipc1_recv) = channel();
    let (_gdax_subscription_watcher_ipc2_send, gdax_subscription_watcher_ipc2_recv) = channel();
    let _gdax_subscription_watcher_thread = thread::spawn(
        move || { 
            run_ingress_collector(
                gdax_subscription_watcher_ipc1_send,
                gdax_subscription_watcher_ipc2_recv,
                ws_subscribe_json.to_string()
            ) 
        }
    );

    // Our data harvesting threads (4 of)
    // 1
    let (gdax_websocket1_ipc1_send, _gdax_websocket1_ipc1_recv) = channel();
    let (gdax_websocket1_ipc2_send, gdax_websocket1_ipc2_recv) = channel();
    let _gdax_websocket1_thread = thread::spawn(
        move || {
            run_ingress_collector(
            gdax_websocket1_ipc1_send,
            gdax_websocket1_ipc2_recv,
            "".to_string()
            )
        }
    );
    // 2
    let (gdax_websocket2_ipc1_send, _gdax_websocket2_ipc1_recv) = channel();
    let (gdax_websocket2_ipc2_send, gdax_websocket2_ipc2_recv) = channel();
    let _gdax_websocket2_thread = thread::spawn(
        move || {
            run_ingress_collector(
            gdax_websocket2_ipc1_send,
            gdax_websocket2_ipc2_recv,
            "".to_string()
            )
        }
    );
    // 3
    let (gdax_websocket3_ipc1_send, _gdax_websocket3_ipc1_recv) = channel();
    let (gdax_websocket3_ipc2_send, gdax_websocket3_ipc2_recv) = channel();
    let _gdax_websocket3_thread = thread::spawn(
        move || {
            run_ingress_collector(
            gdax_websocket3_ipc1_send,
            gdax_websocket3_ipc2_recv,
            "".to_string()
            )
        }
    );

    // Main processing loop
    loop {
        // A inscope place to notify of a subscription update
        let mut flag_update_subscriptions = false;
        let mut flag_thread_throttle = true;

        // step 1 - check for any new subscription returns
        match gdax_subscription_watcher_ipc1_recv.try_recv() {
            Ok(message) => {
                // If we received a message there may be more, disable the throttle
                flag_thread_throttle = false;

                // Create a mutatable buffer - so we can overwrite the primary buffer instantly.
                let mut gdax_productid_buffer = HashSet::new();
    
                // LEARN ME/TODO: 
                let packetroot: SubscriptionJsonRoot = serde_json::from_str(&message).unwrap();
                let online_products: Vec<_> = packetroot.products.into_iter().filter(|product| product.status == "online").collect();
                let product_ids: Vec<_> = online_products.iter().map(|product| product.id.clone()).collect();

                // Itereate over the product_ids Vec and add them to the HashSet
                for product_id in &product_ids {
                    gdax_productid_buffer.insert(product_id.clone());
                }
        
                // Compare the current items and the newly generated
                let product_id_difference = 
                    gdax_productid_buffer.symmetric_difference(&gdax_productid_online);

                // We do not care what, we just want to know there is a difference
                if product_id_difference.count() > 0 {
                    // Ok we need to update the primary store
                    gdax_productid_online = gdax_productid_buffer;
                    // Let us also tell everyone else about the new subscription options
                    flag_update_subscriptions = true;
                }
            },
            Err(TryRecvError::Empty) => {
                // TryRecv will raise these when there is nothing to read
            },
            Err(exception) => {
                println!("[core] An exception was raised: {}",exception);
                continue;
            }
        };

        // Process for our ingress workers
        if flag_update_subscriptions {
            let subscribe_json = generate_subscribe_full_channel(gdax_productid_online.clone());

            for sender in &[&gdax_websocket1_ipc2_send, &gdax_websocket2_ipc2_send, &gdax_websocket3_ipc2_send] { 
                match sender.send(subscribe_json.clone()) {
                    Ok(_) => {},
                    Err(exception) => {
                        println!("[core] Error sending on websocket channel '{}'",exception);
                        break;
                    }
                }
            }
        }

        // There is not a high amount of activity on the main thread, so if nothing 
        // removed the thread_throttle let us wait for THROTTLE_DELAY
        if flag_thread_throttle {
            thread::sleep(THROTTLE_DELAY);
        }
    }
}

// Accept a HashSet and return a JSON query as string with all channels, fully subscribed
fn generate_subscribe_full_channel(gdax_valid_product_ids: HashSet<String>) -> String
{
    let gdax_product_list: Vec<String> = gdax_valid_product_ids.into_iter().collect();
    let mut gdax_channel_list: Vec<SubscribeProductRootChannel> = Vec::new();
    gdax_channel_list.push(SubscribeProductRootChannel { name:"full".to_string(), product_ids:gdax_product_list });

    let subscribe_packet = SubscribeProductRoot {
        r#type: "subscribe".to_string(),
        channels: gdax_channel_list
    };
    return serde_json::to_string(&subscribe_packet).unwrap();
}

fn run_ingress_collector(
    ipc_to_main: std::sync::mpsc::Sender<String>,
    ipc_from_main: std::sync::mpsc::Receiver<String>,
    mut json_packet: String
) -> () {
    // Predefine some often compared strings for speed
    let _comp_subscription = "subscriptions".to_string();
    let _comp_status = "status".to_string();

    // Connect us to redis - Next thing to go..
    let mut redis_con = redis::Client::open(REDIS_PATH_TCP)
    .expect("Invalid connection URL")
    .get_connection()
    .expect("failed to connect to Redis");

    if Path::new(REDIS_PATH_UNIX).exists() {
        println!("Attempting unix socket connection");
        redis_con = redis::Client::open(REDIS_PATH_UNIX)
            .expect("Invalid connection URL")
            .get_connection()
            .expect("failed to connect to Redis");
    }

    if json_packet.len() == 0 {
        // This is a thread in waiting, block till 
        // we get told what to say
        println!("[WebSocket] Starting.");
        json_packet = ipc_from_main.recv().unwrap();
        println!("[WebSocket] Running.");
    }

    loop {
        println!("[WebSocket] Connecting.");
        let (mut socket, response) =
            connect(Url::parse("wss://ws-feed.pro.coinbase.com").unwrap()).expect("Can't connect");

        let status_code =  response.status();
        //println!("Response HTTP code: {}", status_code);
        if status_code != 101 {
            thread::sleep(RECONNECT_DELAY);
            // If we did not get the right status wait 1ms and just retry to recon
            continue;
        }

        // Send our subscription message
        // Error catch this TODO
        socket.write_message(Message::Text(json_packet.to_string())).unwrap();

        'websocket_read: loop {
            let msg = match socket.read_message() {
                Ok(message_read) => {
                    // Err good, do nothing?
                    message_read
                },
                Err(exception) => {
                    println!("[WebSocket] Exception raised: {}",exception);
                    break;
                },
            };

            // Decode the JSON message to a minimal degree
            let deserialized_packet: SubscribedDataPacket = 
                match serde_json::from_str(&msg.clone().to_string()) {
                    Ok(obj) => {obj},
                    Err(exception) => {
                        println!("[WebSocket] JSON decode error! {}",exception);
                        break;
                    },
                };

            if deserialized_packet.r#type == _comp_subscription {
                continue 'websocket_read;
            }
            else if deserialized_packet.r#type == _comp_status {
                match ipc_to_main.send(msg.to_string()) {
                    Ok(message) => {
                        message
                    }
                    Err(e) => {
                        println!("[WebSocket] Channel problem 1:{}",e);
                    }
                }
            }
            else {
                let sequence = deserialized_packet.sequence.unwrap();
                let product_id = deserialized_packet.product_id.unwrap();
                let pkey = format!("{}:{}",product_id,sequence);

                let pkey_clone = pkey.clone();

                // Set it in the main set
                let test_redis_set: i8 = match redis::cmd("SETNX").arg(pkey).arg(msg.clone().to_string()).query(&mut redis_con) {
                    Ok(json_test) => {
                        //println!("Success setting REDIS DATA: {}, error was: {}", pkey_clone, json_test);
                        json_test
                    },
                    Err(e) => {
                        println!("[WebSocket] REDIS SET problem, {} -> {}",pkey_clone, e);
                        continue;
                    }
                };
                if test_redis_set == 1 {
                    redis::cmd("PUBLISH").arg(product_id).arg(sequence.clone().to_string()).execute(&mut redis_con);
                }
            }

            match ipc_from_main.try_recv() {
                Ok(message) => {
                    // We have a message from our parent
                    json_packet = message;
                },
                Err(_) => {
                    // We got nothing, just continue processing
                    continue 'websocket_read;
                }
            }
        }
    };
}