use std::time::Duration;

pub const TICK_INTERVAL: Duration = Duration::from_millis(20);
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(120);
pub const ELECTION_TIMEOUT_MIN_MS: u64 = 350;
pub const ELECTION_TIMEOUT_MAX_MS: u64 = 700;

pub struct RaftConfig {
    pub id: u64,
    pub peeraddrs: Vec<String>,
    pub datadir: String,
}
