mod timestamp;
mod trie;

use crate::{
    timestamp::{make_client_id, Timestamp},
    trie::Trie,
};
use chrono::Utc;

fn main() {
    let now = Utc::now().timestamp_millis();
    let node = make_client_id();
    let timestamp1 = Timestamp::new(now, 0, node.clone());
    let timestamp2 = Timestamp::new(now, 1, node);

    let mut trie1 = Trie::new();
    let mut trie2 = Trie::new();
    trie1.insert(timestamp1);
    trie2.insert(timestamp2);

    let diff = trie1.diff(&trie2).unwrap();
    println!("Diff: {:#?}", diff);
}
