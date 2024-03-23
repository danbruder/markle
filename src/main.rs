use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;

use chrono::{DateTime, Utc};
use murmur3::murmur3_32;
use uuid::Uuid;

// Configuration for maximum clock drift allowed
static MAX_DRIFT: i64 = 60_000; // milliseconds

fn make_client_id() -> String {
    // Generate a new v4 UUID
    let uuid = Uuid::new_v4().to_string();
    // Remove dashes and take the last 16 characters
    uuid.replace("-", "")
        .chars()
        .rev()
        .take(16)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

#[derive(Debug, Clone)]
struct Timestamp {
    millis: i64,
    counter: u16,
    node: String,
}

impl Timestamp {
    fn new(millis: i64, counter: u16, node: String) -> Self {
        Timestamp {
            millis,
            counter,
            node,
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{}-{:04X}-{:016}",
            DateTime::<Utc>::from_utc(
                chrono::NaiveDateTime::from_timestamp_opt(self.millis / 1000, 0).unwrap(),
                Utc
            )
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            self.counter,
            self.node
        )
    }

    fn millis(&self) -> i64 {
        self.millis
    }

    fn counter(&self) -> u16 {
        self.counter
    }

    fn node(&self) -> &str {
        &self.node
    }

    fn set_millis(&mut self, millis: i64) {
        self.millis = millis;
    }

    fn set_counter(&mut self, counter: u16) {
        self.counter = counter;
    }

    fn set_node(&mut self, node: String) {
        self.node = node;
    }

    fn hash(&self) -> u32 {
        let timestamp_str = self.to_string();
        let mut buffer = Cursor::new(timestamp_str.as_bytes());

        // Use the murmur3_32 function with a chosen seed; 0 is used here for simplicity.
        // Adjust the seed as necessary for your application.
        murmur3_32(&mut buffer, 0).unwrap_or(0)
    }

    pub fn send(clock: &mut Timestamp) -> Result<Self, TimestampError> {
        let phys = Utc::now().timestamp_millis();

        let l_old = clock.millis;
        let c_old = clock.counter;

        let l_new = std::cmp::max(l_old, phys);
        let c_new = if l_old == l_new { c_old + 1 } else { 0 };

        if l_new - phys > MAX_DRIFT {
            return Err(TimestampError::ClockDriftError(l_new, phys, MAX_DRIFT));
        }

        if c_new > 65535 {
            return Err(TimestampError::OverflowError);
        }

        clock.set_millis(l_new);
        clock.set_counter(c_new);

        Ok(Timestamp::new(
            clock.millis,
            clock.counter,
            clock.node.clone(),
        ))
    }
}

// Implement Display for Timestamp to enable easy printing
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

// Errors related to timestamp processing
#[derive(Debug)]
enum TimestampError {
    ClockDriftError(i64, i64, i64),
    OverflowError,
    DuplicateNodeError(String),
}

impl fmt::Display for TimestampError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TimestampError::ClockDriftError(l_new, phys, max_drift) => write!(
                f,
                "maximum clock drift exceeded: {} - {} > {}",
                l_new, phys, max_drift
            ),
            TimestampError::OverflowError => write!(f, "timestamp counter overflow"),
            TimestampError::DuplicateNodeError(ref node) => {
                write!(f, "duplicate node identifier {}", node)
            }
        }
    }
}

impl std::error::Error for TimestampError {}

#[derive(Clone, Debug)]
struct Trie {
    hash: u64,
    children: HashMap<String, Trie>,
}

impl Trie {
    fn new() -> Trie {
        Trie {
            hash: 0,
            children: HashMap::new(),
        }
    }

    fn get_keys(&self) -> Vec<String> {
        self.children.keys().cloned().collect()
    }

    fn key_to_timestamp(key: &str) -> u64 {
        let full_key = format!("{:0<16}", key);
        u64::from_str_radix(&full_key, 3).unwrap_or(0) * 1000 * 60
    }

    fn insert(&mut self, timestamp: u64) {
        let hash = timestamp; // Simplified hash function for this example
        let minutes = timestamp / (1000 * 60);
        let key = format!("{:b}", minutes); // Using binary representation as a simplification
        self.hash ^= hash;
        self.insert_key(&key, hash);
    }

    fn insert_key(&mut self, key: &str, hash: u64) {
        if key.is_empty() {
            return;
        }
        let child_key = &key[0..1];
        let child = self
            .children
            .entry(child_key.to_string())
            .or_insert_with(Trie::new);
        child.insert_key(&key[1..], hash);
        child.hash ^= hash;
    }

    fn build(timestamps: Vec<u64>) -> Trie {
        let mut trie = Trie::new();
        for timestamp in timestamps {
            trie.insert(timestamp);
        }
        trie
    }

    fn diff(&self, other: &Trie) -> Trie {
        let mut diff = Trie::new();
        self.diff_recursive(other, &mut diff);
        diff
    }

    fn diff_recursive(&self, other: &Trie, diff: &mut Trie) {
        for key in self.get_keys() {
            let child = self.children.get(&key).unwrap();
            let other_child = other.children.get(&key);
            if let Some(other_child) = other_child {
                if child.hash != other_child.hash {
                    let diff_child = diff.children.entry(key.clone()).or_insert_with(Trie::new);
                    child.diff_recursive(other_child, diff_child);
                }
            } else {
                diff.children.insert(key.clone(), child.clone());
            }
        }
    }

    fn prune(&mut self, timestamp: u64) {
        let hash = timestamp; // Simplified hash function for this example
        let minutes = timestamp / (1000 * 60);
        let key = format!("{:b}", minutes); // Using binary representation as a simplification
        self.hash ^= hash;
        self.prune_key(&key, hash);
    }

    fn prune_key(&mut self, key: &str, hash: u64) {
        if key.is_empty() {
            return;
        }
        let child_key = &key[0..1];
        let child = self.children.get_mut(child_key);
        if let Some(child) = child {
            child.prune_key(&key[1..], hash);
            if child.children.is_empty() {
                self.children.remove(child_key);
            }
        }
        self.hash ^= hash;
    }

    fn debug(&self) {
        println!("{:#?}", self);
    }
}

fn main() {
    let mut trie1 = Trie::new();
    trie1.insert(123456789);
    let mut trie2 = Trie::new();
    trie2.insert(123000000); // Example usage

    let diff = trie1.diff(&trie2).get_keys();
    println!("{:?}", diff);

    let node = make_client_id();
    println!("Node: {}", node);
    let mut timestamp = Timestamp::new(1_586_515_200_000, 42, node);
    println!("Timestamp: {}", timestamp);

    timestamp.set_millis(1_586_515_200_100);
    timestamp.set_counter(45);
    println!("Updated Timestamp: {}", timestamp);
}
