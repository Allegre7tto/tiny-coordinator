use super::log::LogEntry;

#[derive(Debug, Clone)]
pub struct SnapshotData {
    pub lastindex: u64,
    pub lastterm: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct HardState {
    pub term: u64,
    pub voted: u64,
    pub base: u64,
    pub anchor: u64,
    pub log: Vec<LogEntry>,
}
