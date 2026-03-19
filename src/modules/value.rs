pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

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

pub struct Partition;

impl Encode for Partition {
    fn encode(&self) -> Vec<u8> {
        vec![]
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

