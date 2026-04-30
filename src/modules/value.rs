use crate::modules::parser::create_unsigned_varint;

pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub struct ProduceResponse {
    topic_name: String,
    partitions: Vec<ProduceResponsePartition>,
}

impl ProduceResponse {
    pub fn new(topic_name: &str) -> Self {
        Self { topic_name: topic_name.to_string(), partitions: Vec::new() }
    }
    pub fn insert_partition(&mut self, partition: ProduceResponsePartition) {
        self.partitions.push(partition);
    }
}

impl Encode for ProduceResponse {
    fn encode(&self) -> Vec<u8> {
        let mut response = vec![];
        response.push(self.topic_name.len() as u8 + 1);
        response.extend(self.topic_name.as_bytes()); // name
        response.extend(compact_array_encode(&self.partitions)); // partition array
        response.push(0); // tag_buffer
        response
    }
}

pub struct ProduceResponsePartition {
    error_code: i16,
    index: u32,
}

impl ProduceResponsePartition {
    pub fn new(error_code: i16, index: u32) -> Self {
        Self { error_code, index }
    }
}

impl Encode for ProduceResponsePartition {
    fn encode(&self) -> Vec<u8> {
        let mut response = vec![];
        response.extend(self.index.to_be_bytes()); // partition id
        response.extend(self.error_code.to_be_bytes()); // error code
        if self.error_code == 0 {
            response.extend(0i64.to_be_bytes()); // base offset
            response.extend((-1i64).to_be_bytes()); // log append time
            response.extend(0i64.to_be_bytes()); // log start offset
        } else {
            response.extend((-1i64).to_be_bytes()); // base offset
            response.extend((-1i64).to_be_bytes()); // log append time
            response.extend((-1i64).to_be_bytes()); // log start offset
        }
        response.push(1); // record errors array
        response.push(0); // error message
        response.push(0); // tag buffer
        
        response
    }
}

pub struct FetchResponse {
    topic_id: [u8; 16],
    partitions: Vec<FetchResponsePartition>,
}

impl FetchResponse {
    pub fn new(topic_id: [u8;16]) -> Self {
        Self { topic_id, partitions: vec![] }
    }
    pub fn insert_partition(&mut self, partition: FetchResponsePartition) {
        self.partitions.push(partition);
    }
}

impl Encode for FetchResponse {
    fn encode(&self) -> Vec<u8> {
        let mut response = vec![];
        response.extend(&self.topic_id);
        response.extend(compact_array_encode(&self.partitions));
        response
    }
}

pub struct FetchResponsePartition {
    error_code: i16,
    record: Option<Record>,
}

impl FetchResponsePartition {
    pub fn new(error_code: i16, record: Option<Record>) -> Self {
        Self { error_code, record }
    }
}

impl Encode for FetchResponsePartition {
    fn encode(&self) -> Vec<u8> {
        let mut response = vec![];
        response.extend(0i32.to_be_bytes()); // partition index
        response.extend(self.error_code.to_be_bytes()); // error code
        response.extend(0i64.to_be_bytes()); // high watermark
        response.extend(0i64.to_be_bytes()); // last stable offset
        response.extend(0i64.to_be_bytes()); // log start offset
        response.push(1);
        response.extend(0i32.to_be_bytes()); // preferred_read_replica
        if let Some(record) = &self.record { // records
            response.extend(&record.encode());
        } else {
            response.push(0)
        }
        response.push(0); // tags
        response.push(0); // tags
        
        response
    }
}

pub struct Record {
    record_data: Vec<u8>,
}

impl Record {
    pub fn new(record_data: Vec<u8>) -> Self {
        Self { record_data }
    }
}

impl Encode for Record {
    fn encode(&self) -> Vec<u8> {
        let mut response = vec![];
        response.extend(create_unsigned_varint(self.record_data.len() as u32 + 1));
        response.extend(&self.record_data);
        response
    }
}

#[derive(Debug, Clone)]
pub struct Topic {
    error_code: i16,
    name: String,
    id: [u8; 16],
    is_internal: bool,
    partitions: Vec<Partition>,
    authorized_operations: i32
}

impl Topic {
    pub fn new(error_code: i16, name: &str, id: [u8;16], is_internal: bool, partitions: Vec<Partition>, authorized_operations: i32) -> Self {
        Self { error_code, name: name.to_string(), id, is_internal, partitions, authorized_operations }
    }
    pub fn add_partition(&mut self, partition: Partition) {
        self.partitions.push(partition);
    }
    pub fn partitions_iter(&self) -> impl Iterator<Item = &Partition> {
        self.partitions.iter()
    }
    pub fn get_name(&self) -> String {
        self.name.clone()
    }
    pub fn get_id(&self) -> [u8; 16] {
        self.id
    }
}

impl Encode for Topic {
    fn encode(&self) -> Vec<u8> {
        let mut content = vec![];
        content.extend(self.error_code.to_be_bytes());
        content.extend(CompactString::new(&self.name).encode());
        content.extend(&self.id);
        content.push(self.is_internal.into());
        content.extend(compact_array_encode(&self.partitions));
        content.extend(self.authorized_operations.to_be_bytes());
        content.push(0); // tag buffer
        content
    }
}

#[derive(Debug, Clone)]
pub struct Partition {
    error_code: i16,
    index: u32,
    leader_id: u32,
    leader_epoch: u32,
    replica_nodes: Vec<u32>,
    isr_nodes: Vec<u32>,
    eligible_leader_replicas: Vec<u32>,
    last_known_elr: Vec<u32>,
    offline_replicas: Vec<u32>,
}

impl Encode for Partition {
    fn encode(&self) -> Vec<u8> {
        let mut content = vec![];
        content.extend(self.error_code.to_be_bytes());
        content.extend(self.index.to_be_bytes());
        content.extend(self.leader_id.to_be_bytes());
        content.extend(self.leader_epoch.to_be_bytes());
        content.extend(compact_array_encode(&self.replica_nodes));
        content.extend(compact_array_encode(&self.isr_nodes));
        content.extend(compact_array_encode(&self.eligible_leader_replicas));
        content.extend(compact_array_encode(&self.last_known_elr));
        content.extend(compact_array_encode(&self.offline_replicas));
        content.push(0);
        content
    }
}

impl Partition {
    pub fn new(error_code: i16, index: u32, leader_id: u32, leader_epoch: u32, replica_nodes: Vec<u32>, isr_nodes: Vec<u32>, eligible_leader_replicas: Vec<u32>, last_known_elr: Vec<u32>, offline_replicas: Vec<u32>) -> Self {
        Self { error_code, index, leader_id, leader_epoch, replica_nodes, isr_nodes, eligible_leader_replicas, last_known_elr, offline_replicas }
    }
    pub fn get_index(&self) -> u32 {
        self.index
    }
}

struct CompactString(String);

impl CompactString {
    fn new(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Encode for CompactString {
    fn encode(&self) -> Vec<u8> {
        let mut content = vec![];
        let string_bytes = self.0.as_bytes();
        content.push(string_bytes.len() as u8 + 1);
        content.extend(string_bytes);
        content
    }
}

pub fn compact_array_encode<T: Encode>(array: &Vec<T>) -> Vec<u8> {
    let mut content = vec![];
    content.push(array.len() as u8 + 1);
    for element in array {
        content.extend(element.encode());
    }
    content
}

impl Encode for u32 {
    fn encode(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

