use std::collections::HashMap;

use crate::timestamp::{make_client_id, Epoch, Timestamp};
use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub struct Trie {
    hash: u32,
    children: HashMap<String, Trie>,
}

impl Trie {
    pub fn new() -> Trie {
        Trie {
            hash: 0,
            children: HashMap::new(),
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
        println!("            hash: {:032b}", hash);
        println!("       self.hash: {:032b}", self.hash);
        self.hash = self.hash ^ hash;
        println!("self.hash ^ hash: {:032b}", self.hash);

        self.insert_key(&key, hash)
    }

    fn insert_key(&mut self, key: &str, hash: u32) {
        println!("Inserting key: {}", key);
        if key.is_empty() {
            return;
        }

        //println!(" in insert_key key: {}", key);
        let child_key = &key[0..1];
        let child = self
            .children
            .entry(child_key.to_string())
            .or_insert_with(Trie::new);
        println!("             hash: {:032b}", hash);
        println!("       child.hash: {:032b}", child.hash);
        child.hash = child.hash ^ hash;
        println!("child.hash ^ hash: {:032b}", child.hash);

        child.insert_key(&key[1..], hash)
    }

    pub fn build(timestamps: Vec<Timestamp>) -> Self {
        let mut trie = Trie::new();
        for timestamp in timestamps {
            trie.insert(timestamp);
        }
        trie
    }

    pub fn diff<'a>(&self, other: &'a Trie) -> Option<DateTime<Utc>> {
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
                    println!("from recurse: {:?}", divergence_path);
                    // Recurse deeper into the structure.
                    return Some(divergence_path);
                }
                path.pop(); // Backtrack as this path did not lead to divergence.
            } else {
                // Key exists in `self` but not in `other`, indicating a divergence.
                println!("missing key: {} in other: {:?}", key, other);
                path.push(key.clone());
                return Some(path.clone());
            }
        }

        // No divergence found in the traversed paths.
        None
    }

    fn prune(&mut self, timestamp: u32) {
        unimplemented!()
    }

    fn prune_key(&mut self, key: &str, hash: u32) {
        unimplemented!()
    }
}

/// To Base3
fn to_base3(mut input: i64) -> String {
    if input == 0 {
        return "0".to_string();
    }

    let mut base3 = Vec::new();
    while input > 0 {
        base3.push((input % 3).to_string());
        input /= 3;
    }
    base3.reverse();
    base3.join("")
}

/// Key to timestamp
///
/// Key is a base 3 representation of the minutes since epoch
fn key_to_timestamp(key: &str) -> DateTime<Utc> {
    let full_key = format!("{:0<16}", key);
    let minutes = i64::from_str_radix(&full_key, 3).unwrap_or(0);
    let ms = minutes * 1000 * 60;
    DateTime::from_timestamp_millis(ms).unwrap()
}

/// Timestamp to key
fn timestamp_to_key(ts: Timestamp) -> String {
    let millis = ts.millis();
    let minutes = millis / (1000 * 60);
    to_base3(minutes)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_key_to_timestamp() {
        let got = key_to_timestamp("0");
        let want = DateTime::from_timestamp_millis(0).unwrap();
        assert_eq!(got, want);

        let got = key_to_timestamp("1222022111000201");
        let want = DateTime::from_timestamp_millis(1699999980000).unwrap();
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

    #[test]
    fn test_diff() {
        use chrono::DateTime;

        let time1 = DateTime::parse_from_rfc3339("2022-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let time2 = DateTime::parse_from_rfc3339("2021-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let time3 = DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let ts1 = Timestamp::new(time1.timestamp_millis(), 0, make_client_id());
        let ts2 = Timestamp::new(time2.timestamp_millis(), 0, make_client_id());
        //let ts3 = Timestamp::new(time3.timestamp_millis(), 0, make_client_id());

        let trie1 = Trie::build(vec![ts1.clone(), ts2.clone()]);
        let trie2 = Trie::build(vec![]);

        let got = trie1.diff(&trie2);
        // Earliest time they were equal
        let want = Some(time2);
        assert_eq!(got, want);
    }
}
