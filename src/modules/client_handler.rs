use std::collections::HashMap;
use anyhow::Result;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::{ApiVersion, modules::value::{Topic, compact_array_encode}};

pub async fn handle_client(mut stream: TcpStream, apiversions: HashMap<i16, ApiVersion>, topics: Vec<Topic>) -> Result<()> {
    println!("Accepted new connection from {}", stream.peer_addr().unwrap().to_string());
    let mut buffer = [0u8; 10*1024]; // 10KiB buffer for request

    loop {
        stream.read(&mut buffer).await?;
        let _request_size = i32::from_be_bytes(buffer[..4].try_into()?);
        let request_api_key = i16::from_be_bytes(buffer[4..6].try_into()?);
        let request_api_version = i16::from_be_bytes(buffer[6..8].try_into()?);
        let correlation_id = i32::from_be_bytes(buffer[8..12].try_into()?);
        let mut response_version= 0;
    
        let mut response = vec![];
        let mut response_body = vec![];
    
        let error_code: i16;
        if let Some(api_version) = apiversions.get(&request_api_key) && request_api_version >= api_version.min_version && request_api_version <= api_version.max_version {
            error_code = 0
        } else {
            error_code = 35
        }
    
        if error_code == 0 {
            match request_api_key {
                1  => { // Fetch
                    response_version = 1;
                    response_body.extend(0i32.to_be_bytes());
                    response_body.extend(error_code.to_be_bytes());
                    response_body.extend(0i32.to_be_bytes());
                    response_body.push(1);
                    response_body.push(0); // tag buffer
                }
                18 => { // ApiVersions
                    response_body.extend(error_code.to_be_bytes());
                    // array length
                    response_body.extend(((apiversions.len() + 1) as i8).to_be_bytes());
                    // api versions
                    for (_, api_version) in &apiversions {
                        response_body.extend(api_version.encode()); // key, min, max
                        response_body.push(0); // tag
                    }
                    response_body.extend(0i32.to_be_bytes()); // throttle time
                    response_body.push(0); // tag
                },
                75 => { // DescribeTopicPartitions
                    let mut cursor = 12;
                    let client_id_length = u16::from_be_bytes(buffer[cursor .. cursor + 2].try_into()?) as usize;
                    let _client_id = String::from_utf8(buffer[cursor + 2 .. cursor + 2 + client_id_length].to_vec())?;
                    cursor += 2 + client_id_length + 1; // c_id_length + c_id + empty tag buffer

                    response_version = 1;
                    let throttle_time = 0i32;
                    let topics_requested = buffer[cursor] as usize - 1;
                    cursor += 1;
                    let mut req_topics = vec![];
                    for _ in 0..topics_requested {
                        let topic_name_length = buffer[cursor] as usize - 1;
                        let topic_name = String::from_utf8(buffer[cursor + 1 .. cursor + 1 + topic_name_length].to_vec())?;
                        req_topics.push(topic_name);
                        cursor += 1 + topic_name_length + 1; // name_length + name + empty tag buffer
                    }
                    req_topics.sort();

                    let mut response_topics = vec![];
                    for topic_req in req_topics {
                        let mut found = false;
                        for topic in &topics {
                            if topic.get_name() == topic_req {
                                found = true;
                                response_topics.push(topic.clone());
                                break;
                            }
                        }
                        if !found {
                            response_topics.push(Topic::new(3, &topic_req, [0u8; 16], false, vec![], 0));
                        }
                    }

                    response_body.extend(throttle_time.to_be_bytes()); // throttle time
                    response_body.extend(compact_array_encode(&response_topics));
                    response_body.extend((-1i8).to_be_bytes()); // null next cursor
                    response_body.push(0); // tags
                },
                _ => ()
            }
        } else {
            response_body.extend(error_code.to_be_bytes());
        }
        
        let mut response_size = 4 + response_body.len() as i32;
        if response_version == 1 {
            response_size += 1;
            response_body.insert(0, 0);
        }
        response.extend(response_size.to_be_bytes());
        response.extend(correlation_id.to_be_bytes());
        response.extend(response_body);
        stream.write_all(&response).await?;
    }
}