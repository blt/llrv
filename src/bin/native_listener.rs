extern crate byteorder;
extern crate llrv;
extern crate protobuf;
extern crate rand;

use std::net::{TcpListener, TcpStream};
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use std::io::Read;
use llrv::protocols::native::Payload;
use std::thread;
use std::io;

fn handle_client(stream: TcpStream) {
    let mut buf = Vec::with_capacity(4000);
    let mut reader = io::BufReader::new(stream);

    loop {
        println!("LISTENER LOOP");
        let payload_size_in_bytes = match reader.read_u32::<BigEndian>() {
            Ok(i) => i as usize,
            Err(_) => {
                println!("READ PAYLOAD SIZE WAS ERROR");
                return;
            }
        };
        println!("PAYLOAD SIZE: {:?}", payload_size_in_bytes);
        buf.resize(payload_size_in_bytes, 0);
        if reader.read_exact(&mut buf).is_err() {
            println!("READ EXACT WAS ERROR");
            return;
        }
        match protobuf::parse_from_bytes::<Payload>(&buf) {
            Ok(pyld) => {
                println!("PAYLOAD: {:?}", pyld);
            }
            Err(e) => {
                println!("PAYLOAD ERROR: {:?}", e);
                return;
            }
        }
    }
}

fn recv() {
    let listener = TcpListener::bind("127.0.0.1:1972").unwrap();

    for stream in listener.incoming() {
        thread::spawn(|| handle_client(stream.unwrap()));
    }
}

fn main() {
    thread::spawn(recv).join().unwrap();
}
