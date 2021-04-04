use tungstenite::{connect, Message};
use url::Url;

use std::sync::mpsc::channel;
use std::thread;

fn main() {
    let (gdax_websocket1_ingress, gdax_websocket1_egress) = channel();

    let gdax_websocket1_thread = thread::spawn(move || {
        run_ingress_collector(gdax_websocket1_egress);
    });

    let _ = gdax_websocket1_thread.join();
}

fn run_ingress_collector(gdax_websocket1_egress: std::sync::mpsc::Sender) -> () {
    let (mut socket, response) =
        connect(Url::parse("wss://echo.websocket.org").unwrap()).expect("Can't connect");

    let status_code =  response.status();

    println!("Connected to the server");
    println!("Response HTTP code: {}", status_code);
    println!("Response contains the following headers:");
    for (ref header, _value) in response.headers() {
        println!("* {}", header);
    }

    socket.write_message(Message::Text("Hello WebSocket".into())).unwrap();
    loop {
        let msg = socket.read_message().expect("Error reading message");
        println!("Received: {}", msg);
    }
    // socket.close(None);
}