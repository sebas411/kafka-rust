use std::{io::{Read, Write}, net::TcpListener};
use anyhow::Result;

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    println!("Listening on {}", listener.local_addr().unwrap().to_string());
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("Accepted new connection from {}", stream.peer_addr().unwrap().to_string());
                let mut buffer = [0u8; 1024];
                stream.read(&mut buffer)?;
                let _request_size = i32::from_be_bytes(buffer[..4].try_into()?);
                let correlation_id = i32::from_be_bytes(buffer[8..12].try_into()?);

                let mut response = vec![];
                let response_size = 0i32;
                response.extend(response_size.to_be_bytes());
                response.extend(correlation_id.to_be_bytes());
                stream.write_all(&response)?;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }
    Ok(())
}
