extern crate serde_json;
extern crate serde;

use tungstenite::{connect, Message};
use url::Url;

use std::collections::HashSet;
use std::sync::mpsc::channel;
use std::{thread, time};

use time::{Duration};

use serde::{Deserialize, Serialize};
//use serde_json::Result;

const RECONNECT_DELAY: Duration = time::Duration::from_millis(1);

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

fn main() {
    let mut gdax_subscription_items = HashSet::new();

    // Our subscription watcher thread
    let ws_subscribe_json = r#"{"channels":[{"name":"status"}],"type":"subscribe"}"#;
    let (gdax_subscription_watcher_ipc1_send, gdax_subscription_watcher_ipc1_recv) = channel();
    let (gdax_subscription_watcher_ipc2_send, gdax_subscription_watcher_ipc2_recv) = channel();
    let gdax_subscription_watcher_thread = thread::spawn(
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
    let (gdax_websocket1_ipc1_send, gdax_websocket1_ipc1_recv) = channel();
    let (gdax_websocket1_ipc2_send, gdax_websocket1_ipc2_recv) = channel();
    let gdax_websocket1_thread = thread::spawn(
        move || {
            run_ingress_collector(
            gdax_websocket1_ipc1_send,
            gdax_websocket1_ipc2_recv,
            "".to_string()
            )
        }
    );

    // let (gdax_websocket2_tx, gdax_websocket2_rx) = channel();
    // let gdax_websocket2_thread = thread::spawn(move || { run_ingress_collector(gdax_websocket2_tx,1) });

    // let (gdax_websocket3_tx, gdax_websocket3_rx) = channel();
    // let gdax_websocket3_thread = thread::spawn(move || { run_ingress_collector(gdax_websocket3_tx,1) });

    // let (gdax_websocket4_tx, gdax_websocket4_rx) = channel();
    // let gdax_websocket4_thread = thread::spawn(move || { run_ingress_collector(gdax_websocket4_tx,1) });

    // Main processing loop
    loop {
        // step 1 - check for any new subscription returns
        match gdax_subscription_watcher_ipc1_recv.try_recv() {
            Ok(message) => {
                let mut gdax_subscription_items_buffer = HashSet::new();
    
                let packetroot: SubscriptionJsonRoot = serde_json::from_str(&message).unwrap();
                let online_products: Vec<_> = packetroot.products.into_iter().filter(|product| product.status == "online").collect();
                let product_ids: Vec<_> = online_products.iter().map(|product| product.id.clone()).collect();

                // Itereate over the 
                for product_id in &product_ids {
                    println!("x: {}",product_id);
                    gdax_subscription_items_buffer.insert(product_id.clone());
                }
        
                // Overwrite the main buffer
                gdax_subscription_items = gdax_subscription_items_buffer;
            },
            Err(_) => {
                // The subscription items list may never be empty
                // otherwise any watchers will not know what to watch for
                // in the event it is, assume we are not initilized yet and 
                // simply goto the next iteration
                if gdax_subscription_items.is_empty() {
                    thread::sleep(RECONNECT_DELAY);
                    continue;
                }
            }
        };

    }
}

fn run_ingress_collector(
    ipc_to_main: std::sync::mpsc::Sender<String>,
    ipc_from_main: std::sync::mpsc::Receiver<String>,
    mut json_packet: String
) -> () {
    if json_packet.len() == 0 {
        // This is a thread in waiting, block till 
        // we get told what to say
        json_packet = ipc_from_main.recv().unwrap();
    }
    loop {
        let mut rx_first: bool;
        let (mut socket, response) =
            connect(Url::parse("wss://ws-feed.pro.coinbase.com").unwrap()).expect("Can't connect");

        let status_code =  response.status();
        //println!("Response HTTP code: {}", status_code);
        if status_code != 101 {
            thread::sleep(RECONNECT_DELAY);
            // If we did not get the right status wait 1ms and just retry to recon
            continue;
        }

        // As we are connected, reset the first connect counter
        rx_first = true;

        // Send our subscription message
        socket.write_message(Message::Text(json_packet.to_string())).unwrap();

        'websocket_read: loop {
            let msg = match socket.read_message() {
                Ok(message_read) => {
                    // Err good, do nothing?
                    message_read
                },
                Err(exception) => {
                    println!("WebSocket raised error! {}",exception);
                    break;
                },
            };

            if rx_first == true{
                // This is purposeful, the first message will be a repeat of what we subscribed to.
                rx_first = false;
                continue 'websocket_read;
            }

            match ipc_to_main.send(msg.to_string()) {
                Ok(message) => {
                    message
                }
                Err(e) => {
                    println!("IMPOSSIBLE Channel problem? exception({})",e);
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