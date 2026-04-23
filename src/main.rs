use std::{collections::HashMap};
use anyhow::Result;
use tokio::net::TcpListener;

use crate::modules::{client_handler::handle_client, parser::parse_topic_file};
mod modules;

#[derive(Debug, Clone)]
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
    versions.insert(18, ApiVersion::new(18, 0, 4)); // ApiVersions
    versions.insert(75, ApiVersion::new(75, 0, 0)); // DescribeTopicPartitions
    versions.insert(1, ApiVersion::new(1, 0, 16)); // Fetch
    versions
}

#[tokio::main()]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").await?;
    println!("Listening on {}", listener.local_addr().unwrap().to_string());
    let apiversions = generate_api_versions();

    let topics = parse_topic_file("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")?;

    let mut handles = vec![];
    loop {
        let (stream, _) = listener.accept().await?;
        let apiversions = apiversions.clone();
        let topics = topics.clone();
        handles.push(tokio::spawn(async move {
            match handle_client(stream, apiversions, topics).await {
                Ok(_) => (),
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }));
    }
}
