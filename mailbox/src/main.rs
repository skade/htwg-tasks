extern crate redisish;

use std::net::{TcpListener,TcpStream};
use std::io::prelude::*;
use std::io::BufReader;
use std::collections::VecDeque;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    let mut storage = VecDeque::new();

    for stream in listener.incoming() {
        match stream {
            Ok(mut s) => {
                handle(&mut s, &mut storage)
            }
            Err(e) => {
                println!("Connection error: {}", e);
            }
        }
    }
}

fn handle(stream: &mut TcpStream, storage: &mut VecDeque<String>) {
    let command = read_command(stream);
    match command {
        Ok(redisish::Command::Publish(message)) => {
            storage.push_back(message);
        }
        Ok(redisish::Command::Retrieve) => {
            let data = storage.pop_front();
            match data {
                Some(message) => write!(stream, "{}", message),
                None => write!(stream, "No message in inbox!\n")
            }.expect("Write failed!");
        }
        Err(e) => {
            write!(stream, "Error: {:?}\n", e).expect("Write failed!");
        }
    }
}

fn read_command(stream: &mut TcpStream) -> Result<redisish::Command, redisish::Error> {
    let mut read_buffer = String::new();
    let mut buffered_stream = BufReader::new(stream);
    let res = buffered_stream.read_line(&mut read_buffer);
    res.ok().expect("An error occured while reading!");
    redisish::parse(&read_buffer)
}