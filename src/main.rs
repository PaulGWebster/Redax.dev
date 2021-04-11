extern crate serde_json;
extern crate serde;
extern crate redis;

use tungstenite::{connect, Message};
use url::Url;

use std::collections::HashSet;
use std::sync::mpsc::channel;
use std::{thread, time};
use std::path::Path;

use time::{Duration};

use serde::{Deserialize, Serialize};
//use serde_json::Result;

const RECONNECT_DELAY: Duration = time::Duration::from_millis(1);
const REDIS_PATH_TCP: &'static str = "redis+unix:///tmp/redis.sock";
const REDIS_PATH_UNIX: &'static str = "redis://127.0.0.1:6379";

// JSON Structures
#[derive(Serialize, Deserialize, Debug)]
struct SubscriptionJsonRootProductsElement {
    id: String,
    status: String
}
#[derive(Serialize, Deserialize, Debug)]
struct SubscriptionJsonRoot {
    products:Vec<SubscriptionJsonRootProductsElement>
}

#[derive(Serialize, Deserialize, Debug)]
struct SubscribeProductRoot {
    r#type: String,
    channels: Vec<SubscribeProductRootChannel>
}
#[derive(Serialize, Deserialize, Debug)]
struct SubscribeProductRootChannel {
    name: String, // Should be 'full'
    product_ids: Vec<String>
}
#[derive(Serialize, Deserialize, Debug)]
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
    // Send a compiled json packet
    //println!("Blah: {}",generate_subscribe_full_channel(gdax_subscription_items.clone()));
    // gdax_websocket1_ipc2_send.send(generate_subscribe_full_channel(gdax_subscription_items.clone()));

    // Main processing loop
    loop {
        // A inscope place to notify of a subscription update
        let mut updated_subs = false;
        let mut thread_throttle = true;

        // step 1 - check for any new subscription returns
        match gdax_subscription_watcher_ipc1_recv.try_recv() {
            Ok(message) => {
                thread_throttle = false;

                let mut gdax_productid_buffer = HashSet::new();
    
                let packetroot: SubscriptionJsonRoot = serde_json::from_str(&message).unwrap();
                let online_products: Vec<_> = packetroot.products.into_iter().filter(|product| product.status == "online").collect();
                let product_ids: Vec<_> = online_products.iter().map(|product| product.id.clone()).collect();

                // Itereate over the
                for product_id in &product_ids {
                    //println!("x: {}",product_id);
                    gdax_productid_buffer.insert(product_id.clone());
                }
        
                // Compare the current items and the new generated list
                let product_id_difference = 
                    gdax_productid_buffer.symmetric_difference(&gdax_productid_online);

                // We do not care what, we just want to know there is a difference
                if product_id_difference.count() > 0 {
                    // Ok we need to update the primary store
                    gdax_productid_online = gdax_productid_buffer;
                    // When we process it later in this loop, we can check if it 
                    // needs an update:
                    updated_subs = true;
                }
            },
            Err(_) => {
                // The subscription items list may never be empty
                // otherwise any watchers will not know what to watch for
                // in the event it is, assume we are not initilized yet and 
                // simply goto the next iteration
                // if gdax_productid_online.is_empty() {
                //     thread::sleep(RECONNECT_DELAY);
                //     continue;
                // }
            }
        };

        // Process for our ingress workers
        if updated_subs {
            thread_throttle = false;

            let subscribe_json = generate_subscribe_full_channel(gdax_productid_online.clone());

            match gdax_websocket1_ipc2_send.send(subscribe_json.clone()) {
                Ok(_) => {},
                Err(e) => {
                    println!("Error sending on channel for websocket1! '{}'",e);
                    break;
                }
            }
            match gdax_websocket2_ipc2_send.send(subscribe_json.clone()) {
                Ok(_) => {},
                Err(e) => {
                    println!("Error sending on channel for websockett2 '{}'",e);
                    break;
                }
            }
        }

        // Here we might want to read from all those threads and remove duplicates...
        // however we might be better with a global object all threads can access
        // and let them figure it out themself

        if thread_throttle {
            thread::sleep(RECONNECT_DELAY * 10);
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
                match redis::cmd("SETNX").arg(pkey).arg(msg.clone().to_string()).query(&mut redis_con) {
                    Ok(json_test) => {
                        //println!("Success setting REDIS DATA: {}, error was: {}", pkey_clone, json_test);
                        json_test
                    },
                    Err(e) => {
                        println!("[WebSocket] REDIS SET problem, {} -> {}",pkey_clone, e);
                        continue;
                    }
                };
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