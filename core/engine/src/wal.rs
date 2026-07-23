use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use prost::Message;
use proto::raft::v1::{InstallSnapshotReq, PersistState};

use crate::log::LogEntry;
use crate::status::Status;
use crate::storage::{HardState, SnapshotData};

fn writefile(path: &Path, payload: &[u8]) -> Status {
    let tmp = path.with_extension("tmp");

    let mut f = match fs::File::create(&tmp) {
        Ok(f) => f,
        Err(e) => return Status::ioerror(format!("create {:?}: {}", tmp, e)),
    };

    let len = payload.len() as u32;
    let csum = crc32fast::hash(payload);
    let mut header = [0u8; 8];
    header[..4].copy_from_slice(&len.to_le_bytes());
    header[4..].copy_from_slice(&csum.to_le_bytes());

    if f.write_all(&header).is_err() || f.write_all(payload).is_err() || f.flush().is_err() {
        return Status::ioerror(format!("write {:?} failed", tmp));
    }

    if let Err(e) = fs::rename(&tmp, path) {
        return Status::ioerror(format!("rename {:?} -> {:?}: {}", tmp, path, e));
    }

    Status::ok()
}

fn readfile(path: &Path) -> Option<Vec<u8>> {
    let mut f = fs::File::open(path).ok()?;
    let mut header = [0u8; 8];
    f.read_exact(&mut header).ok()?;
    let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    let csum = u32::from_le_bytes(header[4..].try_into().unwrap());
    let mut payload = vec![0u8; len];
    f.read_exact(&mut payload).ok()?;
    if crc32fast::hash(&payload) == csum {
        Some(payload)
    } else {
        None
    }
}

pub struct StateStorage {
    path: PathBuf,
}

impl StateStorage {
    pub fn new(datadir: impl Into<PathBuf>) -> Self {
        let dir: PathBuf = datadir.into();
        fs::create_dir_all(&dir).ok();
        StateStorage { path: dir.join("state") }
    }

    pub fn save(&self, state: &HardState) -> Status {
        let proto = PersistState {
            term: state.term,
            voted: state.voted,
            base: state.base,
            anchor: state.anchor,
            log: state.log.iter().map(|e| LogEntry {
                term: e.term,
                data: e.data.clone(),
            }).collect(),
        };
        let payload = match proto.encode_to_vec() {
            p if p.is_empty() => return Status::internal("serialize PersistState failed"),
            p => p,
        };
        writefile(&self.path, &payload)
    }

    pub fn load(&self) -> Option<HardState> {
        let payload = readfile(&self.path)?;
        let proto = PersistState::decode(payload.as_slice()).ok()?;
        Some(HardState {
            term: proto.term,
            voted: proto.voted,
            base: proto.base,
            anchor: proto.anchor,
            log: proto.log.iter().map(|e| LogEntry {
                term: e.term,
                data: e.data.clone(),
            }).collect(),
        })
    }
}

pub struct SnapshotStorage {
    state: StateStorage,
    path: PathBuf,
}

impl SnapshotStorage {
    pub fn new(datadir: impl Into<PathBuf>) -> Self {
        let dir: PathBuf = datadir.into();
        fs::create_dir_all(&dir).ok();
        SnapshotStorage {
            state: StateStorage { path: dir.join("state") },
            path: dir.join("snapshot"),
        }
    }

    pub fn save(&self, state: &HardState, snap: &SnapshotData) -> Status {
        let s = self.state.save(state);
        if !s.isok() { return s; }

        let proto = InstallSnapshotReq {
            term: 0,
            leaderid: 0,
            lastidx: snap.lastindex,
            lastterm: snap.lastterm,
            data: snap.data.clone(),
        };
        let payload = match proto.encode_to_vec() {
            p if p.is_empty() => return Status::internal("serialize snapshot failed"),
            p => p,
        };
        writefile(&self.path, &payload)
    }

    pub fn load(&self) -> Option<SnapshotData> {
        let payload = readfile(&self.path)?;
        let proto = InstallSnapshotReq::decode(payload.as_slice()).ok()?;
        Some(SnapshotData {
            lastindex: proto.lastidx,
            lastterm: proto.lastterm,
            data: proto.data,
        })
    }
}
