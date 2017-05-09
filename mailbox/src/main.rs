extern crate redisish;

use std::net::{TcpListener,TcpStream};
use std::io::prelude::*;
use std::io::BufReader;
use std::collections::VecDeque;

#[derive(Debug)]
enum ServerError {
    ParseError(redisish::Error),
    IoError(std::io::Error)
}

impl From<redisish::Error> for ServerError {
    fn from(e: redisish::Error) -> ServerError {
        ServerError::ParseError(e)
    }
}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> ServerError {
        ServerError::IoError(e)
    }
}

trait ResultExt<T, E> {
    fn pass<X, Y>(self) -> Result<X, Y> where T: Into<X>, E: Into<Y>;
}

impl<T,E> ResultExt<T, E> for Result<T, E> {
    fn pass<X, Y>(self) -> Result<X, Y> where T: Into<X>, E: Into<Y> {
        self.map(T::into).map_err(E::into)
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    let mut storage = VecDeque::new();

    for stream in listener.incoming() {
        let res = stream.pass()
                  .and_then(|mut s| {
                      handle(&mut s, &mut storage)
                  });
        
        if let Err(e) = res {
            println!("Error occured: {:?}", e);
        }
    }
}

fn handle(stream: &mut TcpStream, storage: &mut VecDeque<String>) -> Result<(), ServerError> {
    let command = read_command(stream)?;
    match command {
        redisish::Command::Publish(message) => {
            storage.push_back(message);
            Ok(())
        }
        redisish::Command::Retrieve => {
            let data = storage.pop_front();
            match data {
                Some(message) => write!(stream, "{}", message).pass(),
                None => write!(stream, "No message in inbox!\n").pass()
            }
        }
    }
}

fn read_command(stream: &mut TcpStream) -> Result<redisish::Command, ServerError> {
    let mut read_buffer = String::new();
    let mut buffered_stream = BufReader::new(stream);
    buffered_stream.read_line(&mut read_buffer)?;
    redisish::parse(&read_buffer).pass()
}