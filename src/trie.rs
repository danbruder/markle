use std::collections::BTreeMap;

use crate::timestamp::{make_client_id, Epoch, Timestamp};

fn millis_to_base3(mut millis: i64) -> String {
    if millis == 0 {
        return "0".to_string();
    }

    let mut base3 = Vec::new();
    while millis > 0 {
        base3.push((millis % 3).to_string());
        millis /= 3;
    }
    base3.reverse();
    base3.join("")
}

#[derive(Clone, Debug)]
pub struct Trie {
    hash: u32,
    children: BTreeMap<String, Trie>,
}

/// Key to timestamp
///
/// Key is a base 3 representation of the minutes since epoch
fn key_to_timestamp(key: &str) -> Epoch {
    let full_key = format!("{:0<16}", key);
    let minutes = i64::from_str_radix(&full_key, 3).unwrap_or(0);
    let ms = minutes * 1000 * 60;
    Epoch(ms)
}

fn timestamp_to_key(ts: Timestamp) -> String {
    let millis = ts.millis();
    let minutes = millis / (1000 * 60);
    millis_to_base3(minutes)
}

impl Trie {
    pub fn new() -> Trie {
        Trie {
            hash: 0,
            children: BTreeMap::new(),
        }
    }

    fn get_keys(&self) -> Vec<String> {
        self.children.keys().cloned().collect()
    }

    pub fn insert(&mut self, timestamp: Timestamp) {
        // Want to be specific to the TS
        let hash = timestamp.hash();

        let key = timestamp_to_key(timestamp);
        println!("key: {}", key);
        println!("hash: {}", hash);
        println!("self.hash: {}", self.hash);
        self.hash ^= hash; // Assuming you're okay with casting u32 to u64
        println!("self.hash ^ hash: {}", self.hash);

        self.insert_key(&key, hash)
    }

    fn insert_key(&mut self, key: &str, hash: u32) {
        if key.is_empty() {
            return;
        }

        println!(" in insert_key key: {}", key);
        let child_key = &key[0..1];
        let child = self
            .children
            .entry(child_key.to_string())
            .or_insert_with(Trie::new);
        child.hash ^= hash;

        child.insert_key(&key[1..], hash)
    }

    fn build(timestamps: Vec<Timestamp>) -> Self {
        let mut trie = Trie::new();
        for timestamp in timestamps {
            trie.insert(timestamp);
        }
        trie
    }

    pub fn diff<'a>(&self, other: &'a Trie) -> Option<Epoch> {
        let mut path = Vec::new();
        if let Some(divergence_path) = self.diff_recursive(other, &mut path) {
            Some(key_to_timestamp(&divergence_path.join("")))
        } else {
            None
        }
    }

    fn diff_recursive<'a>(
        &self,
        other: &'a Trie,
        path: &'a mut Vec<String>,
    ) -> Option<Vec<String>> {
        // Same
        if self.hash == other.hash {
            return None;
        }

        for (key, child) in &self.children {
            println!("key: {}", key);
            if let Some(other_child) = other.children.get(key) {
                path.push(key.clone());
                if child.hash != other_child.hash {
                    // Divergence found, return the path to this point.
                    println!(
                        "child.hash: {} != other_child.hash: {}",
                        child.hash, other_child.hash
                    );
                    return Some(path.clone());
                } else if let Some(divergence_path) = child.diff_recursive(other_child, path) {
                    println!("found it: {:?}", divergence_path);
                    // Recurse deeper into the structure.
                    return Some(divergence_path);
                }
                println!("walking back");
                path.pop(); // Backtrack as this path did not lead to divergence.
            } else {
                // Key exists in `self` but not in `other`, indicating a divergence.
                println!("missing key: {} in other: {:?}", key, other);
                path.push(key.clone());
                return Some(path.clone());
            }
        }
        None // No divergence found in the traversed paths.
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_key_to_timestamp() {
        let got = key_to_timestamp("0");
        let want = Epoch(0);
        assert_eq!(got, want);

        let got = key_to_timestamp("1222022111000201");
        let want = Epoch(1699999980000);
        assert_eq!(got, want);
    }

    #[test]
    fn test_ts_to_key() {
        let key = "1222022111000201";
        let ts = Timestamp::new(1699999980000, 0, make_client_id());
        let got = timestamp_to_key(ts);
        let want = key;
        assert_eq!(got, want);

        let key = "2222222222222222";
        let ts = Timestamp::new(2582803200000, 0, make_client_id());
        let got = timestamp_to_key(ts);
        let want = key;
        assert_eq!(got, want);
    }
}
