use std::collections::{BTreeSet, HashMap};

use crate::timestamp::{make_client_id, Epoch, Timestamp};
use chrono::{DateTime, Utc};

#[derive(Clone, Default, Debug)]
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
        self.hash = self.hash ^ hash;

        self.insert_key(&key, hash)
    }

    fn insert_key(&mut self, key: &str, hash: u32) {
        if key.is_empty() {
            return;
        }

        let child_key = &key[0..1];
        let child = self
            .children
            .entry(child_key.to_string())
            .or_insert_with(Trie::new);
        child.hash = child.hash ^ hash;

        child.insert_key(&key[1..], hash)
    }

    pub fn build(timestamps: Vec<Timestamp>) -> Self {
        let mut trie = Trie::new();
        for timestamp in timestamps {
            trie.insert(timestamp);
        }
        trie
    }

    fn prune(&mut self, timestamp: u32) {
        unimplemented!()
    }

    fn prune_key(&mut self, key: &str, hash: u32) {
        unimplemented!()
    }

    pub fn diff<'a>(&self, other: &'a Trie) -> Option<DateTime<Utc>> {
        let mut path = Vec::new();
        if let Some(divergence_path) = self.diff_recursive(other, &mut path) {
            Some(key_to_timestamp(&divergence_path.join("")))
        } else {
            None
        }
    }

    // find last time the two trees were equal, their divergent point
    fn diff_recursive<'a>(
        &self,
        other: &'a Trie,
        path: &'a mut Vec<String>,
    ) -> Option<Vec<String>> {
        // There is no divergent path
        if self.hash == other.hash {
            return None;
        }

        let mut keys: BTreeSet<String> = BTreeSet::from_iter(self.get_keys());
        keys.extend(other.get_keys());

        let mut diff_key = None;

        for key in keys.iter() {
            let child = self.children.get(key);
            let other_child = other.children.get(key);

            match (child, other_child) {
                (Some(c), Some(oc)) => {
                    if c.hash != oc.hash {
                        diff_key = Some(key.clone());
                        break;
                    }
                }
                (Some(_), None) => {
                    diff_key = Some(key.clone());
                    break;
                }
                (None, Some(_)) => {
                    diff_key = Some(key.clone());
                    break;
                }
                _ => {}
            }
        }

        if let Some(dk) = diff_key {
            path.push(dk.clone());
            match (self.children.get(&dk), other.children.get(&dk)) {
                (Some(c), Some(oc)) => c.diff_recursive(oc, path),
                (Some(c), None) => c.diff_recursive(&Trie::new(), path),
                (None, Some(oc)) => oc.diff_recursive(&Trie::new(), path),
                (None, None) => Trie::new().diff_recursive(&Trie::new(), path),
            }
        } else {
            Some(path.clone())
        }
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
    let b3 = to_base3(minutes);
    format!("{:0>16}", b3)
}

#[cfg(test)]
mod test {
    use super::*;
    use maplit::hashmap;

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

    // #[test]
    // fn test_diff_same() {
    //     let minute = 1000 * 60;
    //     let ts1 = Timestamp::new(10 * minute, 0, make_client_id());
    //     let ts2 = Timestamp::new(20 * minute, 0, make_client_id());

    //     println!("TS1: {:?}", timestamp_to_key(ts1.clone()));
    //     println!("TS2: {:?}", timestamp_to_key(ts2.clone()));

    //     let trie1 = Trie::build(vec![ts2.clone(), ts1.clone()]);
    //     let trie2 = Trie::build(vec![ts2.clone(), ts1.clone()]);

    //     let got = trie1.diff(&trie2);
    //     let want = Timestamp::new(0, 0, make_client_id()).into();
    //     assert_eq!(got, want);
    // }

    #[test]
    fn test_find_convergence() {
        let minute = 1000 * 60;
        let make_ts = |m: i64| Timestamp::new(m * minute, 0, make_client_id());
        let ts1 = make_ts(1);
        let ts2 = make_ts(2);
        let ts3 = make_ts(3);
        let ts4 = make_ts(4);
        let ts5 = make_ts(5);

        let trie1 = Trie::build(vec![ts4.clone(), ts3.clone(), ts1.clone()]);
        let trie2 = Trie::build(vec![ts5, ts4, ts2.clone(), ts1]);

        let got = trie1.diff(&trie2);
        let want = Some(ts2.into());
        assert_eq!(got, want);
    }
}
