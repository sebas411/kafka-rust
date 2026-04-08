use std::{collections::HashMap, fs};
use anyhow::Result;
use crate::modules::value::{Partition, Topic};

fn parse_varint(data: &[u8]) -> (i64, usize) {
    let mut cursor = 0;
    let mut output = (data[0] & 0b01111111) as i64;
    while data[cursor] >= 0b10000000 {
        cursor += 1;
        output <<= 7;
        let current_byte = (data[0] & 0b01111111) as i64;
        output &= current_byte;
    }
    (output, cursor + 1)
}

pub fn parse_topic_file(filename: &str) -> Result<Vec<Topic>> {
    let contents = fs::read(filename)?;
    if contents.len() < 148 {
        return Ok(vec![])
    }
    let mut topics: HashMap<[u8; 16], Topic> = HashMap::new();

    // skip 91 bytes: first record batch (metadata)
    let mut cursor = 91;
    while cursor < contents.len() { // read record batch
        // skip 57 bytes: skip to records length
        cursor += 57;
        let records_length = u32::from_be_bytes(contents[cursor..cursor+4].try_into()?);
        cursor += 4;


        for _ in 0..records_length { // each record
            let (_record_size, read) = parse_varint(&contents[cursor..]);
            cursor += read + 4; // record_length + skip until value
            let (_value_size, read) = parse_varint(&contents[cursor..]);
            cursor += read + 1; // value length
            let record_type = contents[cursor];
            cursor += 1;
            match record_type {
                2 => { // topic
                    cursor += 1;
                    let name_length = contents[cursor] as usize - 1;
                    let name = String::from_utf8(contents[cursor+1..cursor+1+name_length].to_vec())?;
                    cursor += 1 + name_length;
                    let id: [u8; 16] = contents[cursor..cursor+16].try_into()?;
                    cursor += 16 + 1; //skip id & tagged fields
                    let topic = Topic::new(0, &name, id, false, vec![], 0);
                    topics.insert(id, topic);
                },
                3 => { // partition
                    cursor += 1;
                    let partition_id = u32::from_be_bytes(contents[cursor..cursor+4].try_into()?);
                    cursor += 4;
                    let topic_id: [u8; 16] = contents[cursor..cursor+16].try_into()?;
                    cursor += 16;

                    let replica_length = contents[cursor] as usize -1;
                    cursor += 1;
                    let mut replicas = vec![];
                    for _ in 0..replica_length {
                        let replica_id = u32::from_be_bytes(contents[cursor..cursor+4].try_into()?);
                        replicas.push(replica_id);
                        cursor += 4;
                    }
                    
                    let isr_length = contents[cursor] as usize -1;
                    cursor += 1;
                    let mut in_sync_replicas = vec![];
                    for _ in 0..isr_length {
                        let replica_id = u32::from_be_bytes(contents[cursor..cursor+4].try_into()?);
                        in_sync_replicas.push(replica_id);
                        cursor += 4;
                    }
                    cursor += 2; // skip removing and adding replicas arrays (empty)

                    let leader_id = u32::from_be_bytes(contents[cursor..cursor+4].try_into()?);
                    cursor += 4;
                    let leader_epoch = u32::from_be_bytes(contents[cursor..cursor+4].try_into()?);
                    
                    cursor += 4 + 4; // leader epoch, partition epoch
                    let directories_len = contents[cursor] as usize - 1;
                    cursor += 1 + directories_len * 16 + 1; //skip all directories & tagged fields
                    if let Some(topic) = topics.get_mut(&topic_id) {
                        let partition = Partition::new(0, partition_id, leader_id, leader_epoch, replicas, in_sync_replicas, vec![], vec![], vec![]);
                        topic.add_partition(partition);
                    }
                }
                _ => ()
            }
            cursor += 1;
        }
    }
    Ok(topics.into_values().collect())
}