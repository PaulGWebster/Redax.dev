use tungstenite::{connect, Message};
use url::Url;

use std::sync::mpsc::channel;
use std::{thread, time};

use time::{Duration};

const RECONNECT_DELAY: Duration = time::Duration::from_millis(1);

fn main() {
    let (gdax_websocket1_tx, gdax_websocket1_rx) = channel();
    let gdax_websocket1_thread = thread::spawn(move || { run_ingress_collector(gdax_websocket1_tx) });

    // let (gdax_websocket2_tx, gdax_websocket2_rx) = channel();
    // let gdax_websocket2_thread = thread::spawn(move || { run_ingress_collector(gdax_websocket2_tx) });

    loop {
        let message = gdax_websocket1_rx.recv().unwrap();
        println!("BLAH: {:?}", message);
    }

    let _ = gdax_websocket1_thread.join();
}

fn run_ingress_collector(gdax_websocket1_tx: std::sync::mpsc::Sender<String>) -> () {
    loop {
        let (mut socket, response) =
            connect(Url::parse("wss://ws-feed.pro.coinbase.com").unwrap()).expect("Can't connect");

        let status_code =  response.status();
        if status_code != 101 {
            thread::sleep(RECONNECT_DELAY);
            continue;
        }

        println!("Response HTTP code: {}", status_code);

        socket.write_message(Message::Text("Hello WebSocket".into())).unwrap();
        loop {
            let msg = socket.read_message().expect("Error reading message");
            println!("Received: {}", msg);
            gdax_websocket1_tx.send("test".to_string());
        }


    };
}