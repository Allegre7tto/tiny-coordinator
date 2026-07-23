pub type LogEntry = proto::raft::v1::LogEntry;

#[derive(Debug, Clone)]
pub struct LogState {
    pub base: u64,
    pub anchor: u64,
    pub entries: Vec<LogEntry>,
}
