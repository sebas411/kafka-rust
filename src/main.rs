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
                let request_api_key = i16::from_be_bytes(buffer[4..6].try_into()?);
                let request_api_version = i16::from_be_bytes(buffer[6..8].try_into()?);
                let correlation_id = i32::from_be_bytes(buffer[8..12].try_into()?);

                let mut response = vec![];
                let response_size = 0i32;
                let mut response_body = vec![];
                match request_api_key {
                    18 =>  {
                        let error_code: i16;
                        if request_api_version > 4 || request_api_version < 0 {
                            error_code = 35;
                        } else {
                            error_code = 0;
                        }
                        response_body.extend(error_code.to_be_bytes());
                    },
                    _ => ()
                }
                response.extend(response_size.to_be_bytes());
                response.extend(correlation_id.to_be_bytes());
                response.extend(response_body);
                stream.write_all(&response)?;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }
    Ok(())
}
