extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_io;
extern crate redisish;
extern crate bytes;

use std::collections::VecDeque;
use std::sync::{Mutex,Arc};

use std::io;
use std::str;

use tokio_io::codec::{Encoder, Decoder};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use bytes::BytesMut;
use tokio_proto::TcpServer;
use tokio_proto::pipeline::ServerProto;
use tokio_service::Service;

use futures::future;
use futures::future::FutureResult;

pub struct RedisishCodec;
pub struct RedisishProto;
pub struct MailboxService<T> {
    mailbox: Arc<SyncedMailbox<T>>
}

pub enum ServerResponse {
    Empty,
    Stored,
    Value(String)
}

struct SyncedMailbox<T> {
    inner: Mutex<VecDeque<T>>
}

impl<T> SyncedMailbox<T> {
    fn new() -> Self {
        SyncedMailbox { inner: Mutex::new(VecDeque::new()) }
    }
}

trait Storage<T> {
    fn add_message(&self, message: T) -> Result<(), ServerError>;
    fn retrieve_message(&self) -> Result<Option<T>, ServerError>;
}

impl<T> Storage<T> for SyncedMailbox<T> {
    fn add_message(&self, message: T) -> Result<(), ServerError> {
        self.inner.lock().map( |mut guard| {
            guard.push_back(message)
        }).map_err( |err| err.into() )
    }

    fn retrieve_message(&self) -> Result<Option<T>, ServerError> {
        self.inner.lock().map( |mut guard| {
            guard.pop_front()
        }).map_err( |err| err.into() )
    }
}

#[derive(Debug)]
pub enum ServerError {
    ParseError(redisish::Error),
    IoError(std::io::Error),
    LockError
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

impl<T> From<std::sync::PoisonError<T>> for ServerError {
    fn from(_: std::sync::PoisonError<T>) -> ServerError {
        ServerError::LockError
    }
}

impl Decoder for RedisishCodec {
    type Item = redisish::Command;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        if let Some(i) = buf.iter().position(|&b| b == b'\n') {
            // remove the serialized frame from the buffer.
            let line = buf.split_to(i + 1);

            // Turn this data into a UTF string and return it in a Frame.
            let input = match str::from_utf8(&line) {
                Ok(s) => s,
                Err(_) => return Err(io::Error::new(io::ErrorKind::Other,
                                             "invalid UTF-8")),
            };

            redisish::parse(&input).map(|v| v.into() )
                .map_err(|_| io::Error::new(io::ErrorKind::Other,
                                             "message parsing failed") )
        } else {
            Ok(None)
        }
    }
}

impl Encoder for RedisishCodec {
    type Item = ServerResponse;
    type Error = io::Error;

    fn encode(&mut self, msg: ServerResponse, buf: &mut BytesMut)
             -> io::Result<()>
    {
        let msg = match msg {
            ServerResponse::Empty => "No message stored",
            ServerResponse::Value(ref v) => v.as_ref(),
            ServerResponse::Stored => "Stored message"
        };

        buf.extend(msg.as_bytes());
        buf.extend(b"\n");
        Ok(())
    }
}

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for RedisishProto {
    /// For this protocol style, `Request` matches the `Item` type of the codec's `Encoder`
    type Request = redisish::Command;

    /// For this protocol style, `Response` matches the `Item` type of the codec's `Decoder`
    type Response = ServerResponse;

    /// A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, RedisishCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(RedisishCodec))
    }
}

impl Service for MailboxService<String> {
    // These types must match the corresponding protocol types:
    type Request = redisish::Command;
    type Response = ServerResponse;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = FutureResult<Self::Response, Self::Error>;

    // Produce a future for computing a response from a request.
    fn call(&self, command: Self::Request) -> Self::Future {
        let result = match command {
            redisish::Command::Publish(message) => {
                self.mailbox.add_message(message).map(|_| ServerResponse::Stored )
            }
            redisish::Command::Retrieve => {
                self.mailbox.retrieve_message().map(|data| {
                    match data {
                        Some(message) => ServerResponse::Value(message),
                        None => ServerResponse::Empty
                    }
                })
            }
        }.map_err( |_| {
            io::Error::new(io::ErrorKind::Other,
                                             "Internal Server Error")
        });

        future::result(result)
    }
}


fn main() {
    // Specify the localhost address
    let addr = "127.0.0.1:7878".parse().unwrap();

    // The builder requires a protocol and an address
    let server = TcpServer::new(RedisishProto, addr);

    let mailbox = Arc::new(SyncedMailbox::new());

    // We provide a way to *instantiate* the service for each new
    // connection; here, we just immediately return a new instance.
    server.serve(move || Ok(MailboxService { mailbox: mailbox.clone() }));

}
