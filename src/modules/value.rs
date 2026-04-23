pub trait Encode {
    fn encode(&self) -> Vec<u8>;
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
    error_code: i16
}

impl FetchResponsePartition {
    pub fn new(error_code: i16) -> Self {
        Self { error_code }
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
        response.push(0); // records_header
        response.push(0); // record
        response.push(0); // tags
        
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

