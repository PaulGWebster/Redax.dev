extern crate serde_json;
extern crate serde;

use tungstenite::{connect, Message};
use url::Url;

use std::sync::mpsc::channel;
use std::{thread, time};

use time::{Duration};

use serde::{Deserialize, Serialize};
//use serde_json::Result;

const RECONNECT_DELAY: Duration = time::Duration::from_millis(1);
const WS_SUBSCRIBE_LIST: &'static str = "{\"channels\":[{\"name\":\"status\"}],\"type\":\"subscribe\"}";

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
    let (gdax_subscription_watcher_tx_original, gdax_subscription_watcher_rx) = channel();
    let gdax_subscription_watcher_tx = gdax_subscription_watcher_tx_original.clone();
    let gdax_subscription_watcher_thread = thread::spawn(move || { run_ingress_collector(gdax_subscription_watcher_tx_original, 0) });

    // let (gdax_websocket1_tx, gdax_websocket1_rx) = channel();
    // let gdax_websocket1_thread = thread::spawn(move || { run_ingress_collector(gdax_websocket1_tx) });

    // let (gdax_websocket2_tx, gdax_websocket2_rx) = channel();
    // let gdax_websocket2_thread = thread::spawn(move || { run_ingress_collector(gdax_websocket2_tx) });
    
    loop {
        let message = gdax_subscription_watcher_rx.recv().unwrap();
        let packetroot: SubscriptionJsonRoot = serde_json::from_str(&message).unwrap();
        // println!("BLAH: {:?}", deserialized);
        let online_products: Vec<_> = packetroot.products.into_iter().filter(|product| product.status == "online").collect();
    }

    // let _ = gdax_subscription_watcher_thread.join();
}

fn run_ingress_collector(gdax_websocket1_tx: std::sync::mpsc::Sender<String>, subscription_type: u8) -> () {
    loop {
        let mut rx_first: bool;
        let (mut socket, response) =
            connect(Url::parse("wss://ws-feed.pro.coinbase.com").unwrap()).expect("Can't connect");

        let status_code =  response.status();
        println!("Response HTTP code: {}", status_code);
        if status_code != 101 {
            thread::sleep(RECONNECT_DELAY);
            // If we did not get the right status wait 1ms and just retry to recon
            continue;
        }

        // As we are connected, reset the first connect counter
        rx_first = true;

        if subscription_type == 0 {
            // We are the primary subscriber in charge of harvesting the main list
            socket.write_message(Message::Text(WS_SUBSCRIBE_LIST.to_string())).unwrap();
        }
        else {
            println!("Unimplemented type: {}",subscription_type.to_string());
        }
        
        loop {
            let msg = socket.read_message().expect("Error reading message");
            if rx_first == true{
                // This is purposeful, the first message will be a repeat of what we subscribed to.
                rx_first = false;
                continue
            }

            match gdax_websocket1_tx.send(msg.to_string()) {
                Ok(message) => {
                    message
                }
                Err(e) => {
                    println!("IMPOSSIBLE Channel problem? type({}) exception({})",subscription_type.to_string(),e);
                }
            }

        }
    };
}