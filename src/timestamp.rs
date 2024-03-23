use std::fmt;
use std::io::Cursor;

use chrono::Utc;
use murmur3::murmur3_32;
use uuid::Uuid;

// Configuration for maximum clock drift allowed
static MAX_DRIFT: i64 = 60_000; // milliseconds

#[derive(Debug, PartialEq, Clone)]
pub struct Timestamp {
    millis: i64,
    counter: u16,
    node: String,
}

impl Timestamp {
    pub fn new(millis: i64, counter: u16, node: String) -> Self {
        Timestamp {
            millis,
            counter,
            node,
        }
    }

    pub fn ts_minutes(&self) -> i64 {
        self.millis / 1000 / 60
    }

    fn to_string(&self) -> String {
        let time = chrono::DateTime::from_timestamp_millis(self.millis).unwrap();
        let time = time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        format!("{}-{:04X}-{:016}", time, self.counter, self.node)
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

    pub fn hash(&self) -> u32 {
        let timestamp_str = self.to_string();
        let mut buffer = Cursor::new(timestamp_str.as_bytes());

        // Use the murmur3_32 function with a chosen seed; 0 is used here for simplicity.
        // Adjust the seed as necessary for your application.
        murmur3_32(&mut buffer, 0).unwrap_or(0)
    }

    pub fn send(&mut self, phys: i64) -> Result<Self, TimestampError> {
        //let phys = Utc::now().timestamp_millis();

        let l_old = self.millis;
        let c_old = self.counter;

        let l_new = std::cmp::max(l_old, phys);
        let c_new = if l_old == l_new {
            c_old.checked_add(1).ok_or(TimestampError::OverflowError)?
        } else {
            0
        };

        if l_new - phys > MAX_DRIFT {
            return Err(TimestampError::ClockDriftError(l_new, phys, MAX_DRIFT));
        }

        self.set_millis(l_new);
        self.set_counter(c_new);

        Ok(Timestamp::new(self.millis, self.counter, self.node.clone()))
    }
}

// Implement Display for Timestamp to enable easy printing
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

// Errors related to timestamp processing
#[derive(Debug, PartialEq)]
enum TimestampError {
    ClockDriftError(i64, i64, i64),
    OverflowError,
    DuplicateNodeError(String),
}

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

pub fn make_client_id() -> String {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_to_string() {
        let ts = Timestamp::new(1, 0x1234, "1234123412341234".to_string());
        assert_eq!(
            ts.to_string(),
            "1970-01-01T00:00:00.001Z-1234-1234123412341234"
        );

        let ts = Timestamp::new(1711231855000, 65535 - 1, "1234123412341234".to_string());
        assert_eq!(
            ts.to_string(),
            "2024-03-23T22:10:55.000Z-FFFE-1234123412341234"
        );
    }

    #[test]
    fn test_send_overflow() {
        let mut ts = Timestamp::new(1, 0xFFFF, "1234123412341234".to_string());

        let got = ts.send(1).err().unwrap();
        let want = TimestampError::OverflowError;

        assert_eq!(got, want);
    }

    #[test]
    fn test_send_drift() {
        let mut ts = Timestamp::new(MAX_DRIFT + 1, 0x0, "1234123412341234".to_string());

        let got = ts.send(0).err().unwrap();
        let want = TimestampError::ClockDriftError(MAX_DRIFT + 1, 0, MAX_DRIFT);

        assert_eq!(got, want);
    }

    #[test]
    fn test_send_ok_counter() {
        let mut ts = Timestamp::new(1, 0x0, "1234123412341234".to_string());

        let got = ts.send(1).unwrap();
        let want = Timestamp::new(1, 0x1, "1234123412341234".to_string());

        assert_eq!(got, want);
    }

    #[test]
    fn test_send_ok_phys() {
        let mut ts = Timestamp::new(1, 0x0, "1234123412341234".to_string());

        let got = ts.send(2).unwrap();
        let want = Timestamp::new(2, 0x0, "1234123412341234".to_string());

        assert_eq!(got, want);
    }
}
