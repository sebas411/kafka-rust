use std::{collections::HashMap, io::{Read, Write}, net::TcpListener};
use anyhow::Result;

struct ApiVersion {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl ApiVersion {
    fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self { api_key, min_version, max_version }
    }
    fn encode(&self) -> Vec<u8> {
        let mut encoded_bytes = vec![];
        encoded_bytes.extend(self.api_key.to_be_bytes());
        encoded_bytes.extend(self.min_version.to_be_bytes());
        encoded_bytes.extend(self.max_version.to_be_bytes());
        encoded_bytes
    }
}

fn generate_api_versions() -> HashMap<i16, ApiVersion> {
    let mut versions = HashMap::new();
    versions.insert(18, ApiVersion::new(18, 0, 4)); //ApiVersions
    versions
}

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    println!("Listening on {}", listener.local_addr().unwrap().to_string());

    let apiversions = generate_api_versions();
    
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
                let mut response_body = vec![];

                let error_code: i16;
                if let Some(api_version) = apiversions.get(&request_api_key) && request_api_version >= api_version.min_version && request_api_version <= api_version.max_version {
                    error_code = 0
                } else {
                    error_code = 35
                }

                response_body.extend(error_code.to_be_bytes());
                if error_code == 0 {
                    match request_api_key {
                        18 =>  {
                            response_body.extend(((apiversions.len() + 1) as i8).to_be_bytes());
                            for (_, api_version) in &apiversions {
                                response_body.extend(api_version.encode());
                            }
                            response_body.push(0); // tag
                            response_body.extend(0i32.to_be_bytes()); // throttle time
                            response_body.push(0); // tag
                        },
                        _ => ()
                    }
                }

                let response_size = 4 + response_body.len() as i32;
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
