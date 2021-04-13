extern crate serde_json;
extern crate serde;
extern crate redis;

// Using statements, I think these are internal crates?
use tungstenite::{connect, Message};
use url::Url;
use time::Duration;
use serde::{Deserialize, Serialize};

use std::collections::HashSet;
use std::sync::mpsc::{ channel, TryRecvError };
use std::{thread, time};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::str;
use std::fs;
use std::os::unix::net::{UnixStream,UnixListener};

// Constants
const DELAY_1MS: Duration = time::Duration::from_millis(1);
const DELAY_100MS: Duration = time::Duration::from_millis(100);
const REDIS_PATH_UNIX: &'static str = "redis+unix:///tmp/redis.sock";
const GDAX_SOCK_PATH: &'static str =  "/tmp/gdax.sock";
const EMPTY_STRING: &'static str =  "";

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

// Other structs
#[derive(Hash, Eq, PartialEq)]
struct Sequence {
    product: String,
    value: u128
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
            EMPTY_STRING.to_string()
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
            EMPTY_STRING.to_string()
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
            EMPTY_STRING.to_string()
            )
        }
    );

    // Start a TCPServer
    let (gdax_refine_ipc1_send, _gdax_refine_ipc1_recv) = channel();
    let (_gdax_refine_ipc2_send, gdax_refine_ipc2_recv) = channel();
    let _gdax_refine_thread = thread::spawn(
        move || {
            af_server_init (
                gdax_refine_ipc1_send,
                gdax_refine_ipc2_recv
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
            thread::sleep(DELAY_100MS);
        }

        //
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
    println!("[Redis] Attempting unix socket connection");
    let mut redis_con = redis::Client::open(REDIS_PATH_UNIX)
        .expect("[Redis] Invalid connection URL")
        .get_connection()
        .expect("[Redis] Failed to connect to Redis");

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
            thread::sleep(DELAY_1MS);
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

// This creates the initial unix socket and hands off the management
// to another thread
fn af_server_init (
    ipc_to_main: std::sync::mpsc::Sender<String>,
    ipc_from_main: std::sync::mpsc::Receiver<String>
) -> () {
    println!("[AFUNIX] Server thread started");

    match fs::remove_file(GDAX_SOCK_PATH) {
        _ => {},
    };
    let listener = UnixListener::bind(GDAX_SOCK_PATH).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let ipc_to_main_clone = ipc_to_main.clone();
                thread::spawn(|| af_server_client_manager(stream,ipc_to_main_clone));
            },
            Err(err) => {
                println!("[AFUNIX] Error: {}", err);
                break;
            }
        }
    }

    drop(listener);
}

fn af_server_client_manager(
    client: UnixStream, 
    ipc_to_main: std::sync::mpsc::Sender<String>
) {
    println!("[AFUNIX] Manager thread started");

    let mut client_read = BufReader::new(&client);
    let mut client_write = BufWriter::new(&client);

    // Read the first line this will be what we subscribe to
    let mut operating_mode = String::new();
    match client_read.read_line(&mut operating_mode) {
        Ok(_) => {},
        Err(exception) => {
            println!("[AFUNIX] Client error: {}",exception);
            return;
        }
    };

    // Strip newline from the read line
    while operating_mode.ends_with('\n') || operating_mode.ends_with('\r') {
        operating_mode.pop();
    }

    // Check we have a valid operating mode
    if operating_mode.len() != 0 {
        println!("[AFUNIX] OperatingMode: {}",operating_mode);
    }
    else {
        println!("[AFUNIX] Manager thread exiting.");
        return;
    }

    // Create a redis client
    println!("[Redis] Attempting unix socket connection");
    let mut redis_con_subscriber = redis::Client::open(REDIS_PATH_UNIX)
        .expect("[Redis] Invalid connection URL")
        .get_connection()
        .expect("[Redis] failed to connect to Redis (UNIX)");

    let mut redis_con_retriever = redis::Client::open(REDIS_PATH_UNIX)
        .expect("[Redis] Invalid connection URL")
        .get_connection()
        .expect("[Redis] failed to connect to Redis (UNIX)");

    // Change the connection type to a pubsub
    let mut pubsub = redis_con_subscriber.as_pubsub();

    // Create a place to make sure of order of packets is ensured
    HashSet<Sequence> sequenceCheck = HashSet::new();

    // Do what the client says
    pubsub.psubscribe(operating_mode).unwrap();
    
    loop {
        let pubsub_message = match pubsub.get_message() {
            Ok(message_rx_success) => {
                message_rx_success
            },
            Err(_) => {
                thread::sleep(DELAY_1MS);
                continue;
            }
        };

        // Extract the requirements for the primary key
        let payload : String = pubsub_message.get_payload().unwrap();
        let pkey = format!("{}:{}",pubsub_message.get_channel_name(), payload);

        // Retrieve the full packet
        let json_packet: String = match redis::cmd("GET").arg(pkey).query(&mut redis_con_retriever) {
            Ok(json_test) => {
                json_test
            },
            Err(exception) => {
                println!("[Redis] Failed to read DATA: {}", exception);
                continue;
            }
        };

        // Write back to the client
        match client_write.write(json_packet.as_bytes()) {
            Ok(_) => {
                match client_write.write("\n".to_string().as_bytes()) {
                    Ok(_) => {},
                    Err(exception) => {
                        println!("[AFUNIX] Failed to write client data!: {}", exception);
                        break
                    },
                }
            },
            Err(exception) => {
                println!("[AFUNIX] Failed to write client data!: {}", exception);
                break;
            }
        };

    };
}
