use std::collections::BTreeMap;

use crate::timestamp::Timestamp;

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
    hash: u64,
    children: BTreeMap<String, Trie>,
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

    fn key_to_timestamp(key: &str) -> i64 {
        println!("key: {}", key);
        let full_key = format!("{:0<16}", key);
        println!("full_key: {}", full_key);
        let millis = i64::from_str_radix(&full_key, 3).unwrap_or(0) * 1000 * 60;
        millis / 1000
    }

    pub fn insert(&mut self, timestamp: Timestamp) {
        // Want to be specific to the TS
        let hash = timestamp.hash();

        let minutes = timestamp.ts_minutes();
        let key = millis_to_base3(minutes);
        println!("key: {}", key);
        println!("hash: {}", hash);
        println!("self.hash: {}", self.hash);
        self.hash ^= hash as u64; // Assuming you're okay with casting u32 to u64
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
        child.hash ^= hash as u64; // Casting u32 to u64 for hash

        child.insert_key(&key[1..], hash)
    }

    fn build(timestamps: Vec<Timestamp>) -> Self {
        let mut trie = Trie::new();
        for timestamp in timestamps {
            trie.insert(timestamp);
        }
        trie
    }

    pub fn diff<'a>(&self, other: &'a Trie) -> Option<i64> {
        let mut path = Vec::new();
        if let Some(divergence_path) = self.diff_recursive(other, &mut path) {
            Some(Trie::key_to_timestamp(&divergence_path.join("")))
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

    // #[test]
    // fn test_simple_insert_key() {
    //     let mut trie1 = Trie::new();
    //     let mut trie2 = Trie::new();
    //     trie1.insert_key("112", 1);
    //     trie2.insert_key("111", 1);

    //     let got = trie1.diff(&trie2).unwrap().to_string();
    //     let want = want.to_string();
    //     assert_eq!(got, want);
    // }

    // #[test]
    // fn test_insert_hash() {
    //     let now = 1711220570;
    //     let node = make_client_id();
    //     let timestamp = Timestamp::new(now, 0, node.clone());

    //     let mut trie = Trie::new();
    //     trie.insert(timestamp.clone()).unwrap();

    //     assert_eq!(trie.hash - 1, timestamp.hash() as u64);
    // }

    // #[test]
    // fn test_millis_to_base3() {
    //     let millis = 1_000_000;
    //     let got = millis_to_base3(millis);
    //     let want = "1000000000".to_string();
    //     assert_eq!(got, want);
    // }

    // #[test]
    // fn test_diff_is_some() {
    //     let now = 1711221133000;
    //     let before = 1711221133001;
    //     let want = 1;
    //     let node = make_client_id();
    //     let timestamp1 = Timestamp::new(now, 0, node.clone());
    //     let timestamp2 = Timestamp::new(before, 0, node.clone());

    //     let mut trie1 = Trie::new();
    //     let mut trie2 = Trie::new();
    //     trie1.insert(timestamp1);
    //     trie2.insert(timestamp2);

    //     let got = trie1.diff(&trie2).unwrap().to_string();
    //     let want = want.to_string();
    //     assert_eq!(got, want);
    // }

    /*
    #[test]
    fn test_hash_of_same_ts() {
        let now = Utc::now().timestamp_millis();
        let node = make_client_id();
        let timestamp1 = Timestamp::new(now, 0, node.clone());
        let timestamp2 = Timestamp::new(now, 0, node.clone());

        assert_eq!(timestamp1.hash(), timestamp2.hash());
    }
    */
}
