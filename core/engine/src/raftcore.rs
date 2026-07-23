use std::time::{Duration, Instant};

use rand::Rng;

use proto::raft::v1::{
    AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp,
    LogEntry, RequestVoteReq, RequestVoteResp,
};

use super::config::{RaftConfig, ELECTION_TIMEOUT_MAX_MS, ELECTION_TIMEOUT_MIN_MS, HEARTBEAT_INTERVAL};
use super::status::Status;
use super::storage::{HardState, SnapshotData};
use super::wal::{SnapshotStorage, StateStorage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub enum ApplyMsg {
    Command { data: Vec<u8>, index: u64, term: u64 },
    Snapshot { data: Vec<u8>, term: u64, index: u64 },
}

#[derive(Debug, Clone)]
pub enum RaftTask {
    Vote { peer: u32, req: RequestVoteReq, electionterm: u64 },
    Append { peer: u32, req: AppendEntriesReq, sentterm: u64, previndex: u64, sentnum: u32 },
    InstallSnapshot { peer: u32, req: InstallSnapshotReq, sentterm: u64, lastindex: u64 },
}

pub struct RaftCore {
    pub me: u32,
    pub peercount: u32,
        state: StateStorage,
        snapshot: SnapshotStorage,
    oncommit: Box<dyn Fn(ApplyMsg) + Send>,

    role: Role,
    pub term: u64,
    voted: Option<u32>,
    granted: Vec<bool>,
    deadline: Instant,
    heartbeat: Instant,

    log: Vec<LogEntry>,
    base: u64,
    anchor: u64,
    pub commitidx: u64,
    pub applyidx: u64,
    next: Vec<u64>,
    acked: Vec<u64>,
    snap: Vec<u8>,
    pub leaderid: u64,
}

impl RaftCore {
    pub fn new(
        cfg: RaftConfig,
    state: StateStorage,
    snapshot: SnapshotStorage,
        oncommit: Box<dyn Fn(ApplyMsg) + Send>,
    ) -> Self {
        let peercount = cfg.peeraddrs.len() as u32;

        let mut sentinel = LogEntry::default();
        sentinel.term = 0;

        let now = Instant::now();
        let mut raft = RaftCore {
            me: cfg.id as u32,
            peercount,
            state,
            snapshot,
            oncommit,
            role: Role::Follower,
            term: 0,
            voted: None,
            granted: vec![false; peercount as usize],
            deadline: now + Self::randelectiontimeout(),
            heartbeat: now,
            log: vec![sentinel],
            base: 0,
            anchor: 0,
            commitidx: 0,
            applyidx: 0,
            next: vec![1; peercount as usize],
            acked: vec![0; peercount as usize],
            snap: Vec::new(),
            leaderid: 0,
        };

        if let Some(hs) = raft.state.load() {
            raft.restore(&hs);
        }
        if let Some(snap) = raft.snapshot.load() {
            raft.snap = snap.data;
        }

        raft
    }

    fn randelectiontimeout() -> Duration {
        let mut rng = rand::thread_rng();
        let ms = rng.gen_range(ELECTION_TIMEOUT_MIN_MS..=ELECTION_TIMEOUT_MAX_MS);
        Duration::from_millis(ms)
    }

    pub fn tick(&mut self) -> Vec<RaftTask> {
        let mut tasks = Vec::new();
        let now = Instant::now();

        if self.role == Role::Leader {
            if now.duration_since(self.heartbeat) >= HEARTBEAT_INTERVAL {
                self.heartbeat = now;
                for peer in 0..self.peercount {
                    if peer == self.me { continue; }
                    if self.next[peer as usize] <= self.base {
                        tasks.push(self.buildsnapshottask(peer));
                    } else {
                        tasks.push(self.buildappendtask(peer));
                    }
                }
            }
        } else if now >= self.deadline {
            let et = self.beginelection();
            let li = self.lastindex();
            let lt = self.log[self.logoffset(li)].term;

            for peer in 0..self.peercount {
                if peer == self.me { continue; }
                let mut req = RequestVoteReq::default();
                req.term = et;
                req.candidate = self.me as u64;
                req.lastidx = li;
                req.lastterm = lt;
                tasks.push(RaftTask::Vote { peer, req, electionterm: et });
            }
        }
        tasks
    }

    pub fn propose(&mut self, data: &[u8], outindex: &mut u64, outterm: &mut u64) -> Status {
        if self.role != Role::Leader {
            return Status::notleader(0);
        }
        let mut entry = LogEntry::default();
        entry.term = self.term;
        entry.data = data.to_vec();
        self.log.push(entry);
        self.persist();
        *outindex = self.lastindex();
        *outterm = self.term;
        self.acked[self.me as usize] = *outindex;
        self.next[self.me as usize] = *outindex + 1;
        self.heartbeat = Instant::now() - HEARTBEAT_INTERVAL;
        Status::ok()
    }

    pub fn takesnapshot(&mut self, index: u64, snapshot: &[u8]) {
        if index <= self.base || index > self.commitidx || index > self.lastindex() {
            return;
        }
        let t = self.log[self.logoffset(index)].term;
        let mut sentinel = LogEntry::default();
        sentinel.term = t;
        let mut newlog = vec![sentinel];
        if index < self.lastindex() {
            let start = self.logoffset(index + 1);
            newlog.extend_from_slice(&self.log[start..]);
        }
        self.log = newlog;
        self.base = index;
        self.anchor = t;
        self.snap = snapshot.to_vec();
        self.commitidx = self.commitidx.max(self.base);
        self.applyidx = self.applyidx.max(self.base);
        if self.role == Role::Leader {
            self.acked[self.me as usize] = self.base + self.logsize() - 1;
            self.next[self.me as usize] = self.base + self.logsize();
        }
        self.persistwithsnapshot();
    }

    pub fn condinstallsnapshot(
        &mut self, lastterm: u64, lastindex: u64, snapshot: &[u8],
    ) -> bool {
        if lastindex <= self.base || lastindex <= self.commitidx {
            return false;
        }
        let mut sentinel = LogEntry::default();
        sentinel.term = lastterm;
        let mut newlog = vec![sentinel];
        if lastindex < self.lastindex()
            && self.log[self.logoffset(lastindex)].term == lastterm
        {
            let start = self.logoffset(lastindex + 1);
            newlog.extend_from_slice(&self.log[start..]);
        }
        self.log = newlog;
        self.base = lastindex;
        self.anchor = lastterm;
        self.commitidx = self.commitidx.max(self.base);
        self.applyidx = self.applyidx.max(self.base);
        self.snap = snapshot.to_vec();
        let floor = self.base + 1;
        for i in 0..self.peercount as usize {
            if self.next[i] < floor { self.next[i] = floor; }
            if self.acked[i] < self.base { self.acked[i] = self.base; }
        }
        self.persistwithsnapshot();
        true
    }

    pub fn isleader(&self) -> bool { self.role == Role::Leader }
    pub fn getterm(&self) -> u64 { self.term }
    pub fn getme(&self) -> u32 { self.me }
    pub fn getpeercount(&self) -> u32 { self.peercount }

    // ── RPC handlers ──────────────────────────────────────────

    pub fn onrequestvote(&mut self, args: &RequestVoteReq) -> RequestVoteResp {
        let mut resp = RequestVoteResp::default();
        resp.term = self.term;
        resp.vote = false;

        if args.term < self.term { return resp; }
        if args.term > self.term { self.stepdown(args.term); }

        let candidate = args.candidate as u32;
        let fresh = args.lastterm > self.lastterm()
            || (args.lastterm == self.lastterm() && args.lastidx >= self.lastindex());
        let canvote = fresh
            && (self.voted.is_none() || self.voted.unwrap() == candidate);

        if canvote {
            self.voted = Some(candidate);
            self.deadline = Instant::now() + Self::randelectiontimeout();
            self.persist();
        }
        resp.term = self.term;
        resp.vote = canvote;
        resp
    }

    pub fn onappendentries(&mut self, args: &AppendEntriesReq) -> AppendEntriesResp {
        let mut resp = AppendEntriesResp::default();
        resp.term = self.term;
        resp.success = false;

        if args.term < self.term { return resp; }
        if args.term > self.term {
            self.stepdown(args.term);
        } else if self.role != Role::Follower {
            self.role = Role::Follower;
            self.granted.fill(false);
        }
        self.deadline = Instant::now() + Self::randelectiontimeout();
        self.leaderid = args.leaderid;
        resp.term = self.term;

        let pi = args.previdx;
        let pt = args.prevterm;

        if pi < self.base {
            resp.conflictidx = self.base + 1;
            return resp;
        }
        if pi > self.lastindex() {
            resp.conflictidx = self.base + self.logsize();
            return resp;
        }
        if self.log[self.logoffset(pi)].term != pt {
            let ct = self.log[self.logoffset(pi)].term;
            let mut ci = pi;
            while ci > self.base && self.log[self.logoffset(ci - 1)].term == ct {
                ci -= 1;
            }
            resp.conflictterm = ct;
            resp.conflictidx = ci;
            return resp;
        }

        let mut changed = false;
        let mut idx = pi + 1;
        for incoming in &args.entries {
            if idx <= self.lastindex() {
                let e = &self.log[self.logoffset(idx)];
                if e.term != incoming.term || e.data != incoming.data {
                    self.log.truncate(self.logoffset(idx));
                    self.log.push(incoming.clone());
                    changed = true;
                }
            } else {
                self.log.push(incoming.clone());
                changed = true;
            }
            idx += 1;
        }
        if changed { self.persist(); }

        if args.leadercommit > self.commitidx {
            self.commitidx = args.leadercommit.min(self.lastindex());
            self.applyready();
        }
        resp.success = true;
        resp
    }

    pub fn oninstallsnapshot(&mut self, args: &InstallSnapshotReq) -> InstallSnapshotResp {
        let mut resp = InstallSnapshotResp::default();
        resp.term = self.term;
        if args.term < self.term { return resp; }
        self.stepdown(args.term);
        self.deadline = Instant::now() + Self::randelectiontimeout();
        if args.lastidx > self.base {
            let msg = ApplyMsg::Snapshot {
                data: args.data.clone(),
                term: args.lastterm,
                index: args.lastidx,
            };
            (self.oncommit)(msg);
        }
        resp.term = self.term;
        resp
    }

    // ── RPC reply handlers ────────────────────────────────────

    pub fn onvotereply(&mut self, electionterm: u64, peer: u32, reply: &RequestVoteResp) {
        if reply.term > self.term { self.stepdown(reply.term); return; }
        if self.role != Role::Candidate || self.term != electionterm { return; }
        if reply.vote {
            self.granted[peer as usize] = true;
            if self.hasquorum() { self.stepup(); }
        }
    }

    pub fn onappendreply(
        &mut self, sentterm: u64, peer: u32, previndex: u64, sentnum: u32,
        reply: &AppendEntriesResp,
    ) {
        if reply.term > self.term { self.stepdown(reply.term); return; }
        if self.role != Role::Leader || self.term != sentterm { return; }

        if reply.success {
            let matched = previndex + sentnum as u64;
            if matched > self.acked[peer as usize] {
                self.acked[peer as usize] = matched;
            }
            self.next[peer as usize] = self.acked[peer as usize] + 1;
            self.advancecommit();
            if self.next[peer as usize] <= self.lastindex() {
                self.heartbeat = Instant::now() - HEARTBEAT_INTERVAL;
            }
            return;
        }

        let next: u64;
        if reply.conflictterm == 0 {
            next = reply.conflictidx.max(1);
        } else {
            let pos = self.log.iter().rposition(|e| e.term == reply.conflictterm);
            if let Some(off) = pos {
                next = self.base + off as u64 + 1;
            } else {
                next = reply.conflictidx.max(1);
            }
        }
        self.next[peer as usize] = next.min(self.base + self.logsize());
        self.heartbeat = Instant::now() - HEARTBEAT_INTERVAL;
    }

    pub fn onsnapshotreply(
        &mut self, sentterm: u64, peer: u32, lastindex: u64, _reply: &InstallSnapshotResp,
    ) {
        if _reply.term > self.term { self.stepdown(_reply.term); return; }
        if self.role != Role::Leader || self.term != sentterm { return; }
        if self.acked[peer as usize] < lastindex {
            self.acked[peer as usize] = lastindex;
        }
        let nxt = lastindex + 1;
        if self.next[peer as usize] < nxt {
            self.next[peer as usize] = nxt;
        }
        if self.next[peer as usize] <= self.lastindex() {
            self.heartbeat = Instant::now() - HEARTBEAT_INTERVAL;
        }
    }

    pub fn loadsnapshotdata(&self) -> Option<SnapshotData> {
        self.snapshot.load()
    }

    // ── Log helpers ───────────────────────────────────────────

    fn lastindex(&self) -> u64 {
        self.base + self.log.len() as u64 - 1
    }

    fn lastterm(&self) -> u64 {
        self.log.last().unwrap().term
    }

    fn logsize(&self) -> u64 {
        self.log.len() as u64
    }

    fn logoffset(&self, absidx: u64) -> usize {
        (absidx - self.base) as usize
    }

    // ── Role transitions ──────────────────────────────────────

    fn stepdown(&mut self, newterm: u64) {
        if newterm > self.term {
            self.term = newterm;
            self.voted = None;
            self.persist();
        }
        self.role = Role::Follower;
        self.granted.fill(false);
        self.leaderid = 0;
        self.deadline = Instant::now() + Self::randelectiontimeout();
    }

    fn hasquorum(&self) -> bool {
        let count = self.granted.iter().filter(|&&g| g).count();
        count as u32 > self.peercount / 2
    }

    fn beginelection(&mut self) -> u64 {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted = Some(self.me);
        self.granted.fill(false);
        self.granted[self.me as usize] = true;
        self.deadline = Instant::now() + Self::randelectiontimeout();
        self.persist();
        if self.hasquorum() { self.stepup(); }
        self.term
    }

    fn stepup(&mut self) {
        self.role = Role::Leader;
        self.granted.fill(false);
        let nv = self.base + self.logsize();
        self.next.fill(nv);
        self.acked.fill(self.base);
        self.acked[self.me as usize] = self.base + self.logsize() - 1;
        self.next[self.me as usize] = self.base + self.logsize();
        self.heartbeat = Instant::now() - HEARTBEAT_INTERVAL;
    }

    // ── Commit / Apply ────────────────────────────────────────

    fn advancecommit(&mut self) {
        if self.role != Role::Leader { return; }
        let mut target = self.commitidx;
        for idx in (self.commitidx + 1..=self.lastindex()).rev() {
            if self.log[self.logoffset(idx)].term != self.term { continue; }
            let replicated = self.acked.iter().filter(|&&a| a >= idx).count();
            if replicated as u32 > self.peercount / 2 {
                target = idx;
                break;
            }
        }
        if target > self.commitidx {
            self.commitidx = target;
            self.applyready();
        }
    }

    fn applyready(&mut self) {
        while self.applyidx < self.commitidx {
            self.applyidx += 1;
            let e = &self.log[self.logoffset(self.applyidx)];
            let msg = ApplyMsg::Command {
                data: e.data.clone(),
                index: self.applyidx,
                term: e.term,
            };
            (self.oncommit)(msg);
        }
    }

    // ── Build RPC tasks ───────────────────────────────────────

    fn buildappendtask(&self, peer: u32) -> RaftTask {
        let nxt = self.next[peer as usize];
        let prev = nxt - 1;

        let mut req = AppendEntriesReq::default();
        req.term = self.term;
        req.leaderid = self.me as u64;
        req.previdx = prev;
        req.prevterm = self.log[self.logoffset(prev)].term;
        req.leadercommit = self.commitidx;

        let mut sentnum = 0u32;
        if nxt <= self.lastindex() {
            let start = self.logoffset(nxt);
            for e in &self.log[start..] {
                req.entries.push(e.clone());
                sentnum += 1;
            }
        }

        RaftTask::Append { peer, req, sentterm: self.term, previndex: prev, sentnum }
    }

    fn buildsnapshottask(&self, peer: u32) -> RaftTask {
        let mut req = InstallSnapshotReq::default();
        req.term = self.term;
        req.leaderid = self.me as u64;
        req.lastidx = self.base;
        req.lastterm = self.anchor;
        req.data = self.snap.clone();

        RaftTask::InstallSnapshot { peer, req, sentterm: self.term, lastindex: self.base }
    }

    // ── Persistence ───────────────────────────────────────────

    fn persist(&self) {
        let hs = HardState {
            term: self.term,
            voted: self.voted.map_or(0, |v| v as u64 + 1),
            base: self.base,
            anchor: self.anchor,
            log: self.log.clone(),
        };
        let _ = self.state.save(&hs);
    }

    fn persistwithsnapshot(&self) {
        let hs = HardState {
            term: self.term,
            voted: self.voted.map_or(0, |v| v as u64 + 1),
            base: self.base,
            anchor: self.anchor,
            log: self.log.clone(),
        };
        let snap = SnapshotData {
            lastindex: self.base,
            lastterm: self.anchor,
            data: self.snap.clone(),
        };
        let _ = self.snapshot.save(&hs, &snap);
    }

    fn restore(&mut self, hs: &HardState) {
        self.term = hs.term;
        self.voted = if hs.voted == 0 { None } else { Some(hs.voted as u32 - 1) };
        self.base = hs.base;
        self.anchor = hs.anchor;

        if !hs.log.is_empty() { self.log = hs.log.clone(); }
        if self.log.is_empty() {
            let mut sentinel = LogEntry::default();
            sentinel.term = self.anchor;
            self.log.push(sentinel);
        }
        self.log[0].term = self.anchor;
        self.log[0].data.clear();

        self.commitidx = self.base;
        self.applyidx = self.base;
        let nv = self.base + self.logsize();
        self.next.fill(nv);
        self.acked.fill(self.base);
        self.acked[self.me as usize] = self.base + self.logsize() - 1;
    }
}
