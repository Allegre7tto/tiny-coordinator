use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use engine::config::RaftConfig;
use engine::raftcore::{ApplyMsg, RaftCore, RaftTask};
use engine::wal::{SnapshotStorage, StateStorage};

fn addrs(n: u32) -> Vec<String> {
    (0..n).map(|i| format!("127.0.0.1:{}", 7000 + i)).collect()
}

fn makestorages(dir: &str) -> (StateStorage, SnapshotStorage) {
    let _ = std::fs::remove_dir_all(dir);
    (StateStorage::new(dir), SnapshotStorage::new(dir))
}

fn makenode(
    id: u32,
    addrs: Vec<String>,
    applied: Arc<Mutex<Vec<ApplyMsg>>>,
) -> RaftCore {
    let cfg = RaftConfig {
        id: id as u64,
        peeraddrs: addrs,
        datadir: format!("/tmp/rt_{}", id),
    };

    let cb = Box::new(move |msg: ApplyMsg| {
        applied.lock().unwrap().push(msg);
    });

    let (state, snapshot) = makestorages(&cfg.datadir);
    RaftCore::new(cfg, state, snapshot, cb)
}

fn findleader(nodes: &mut [RaftCore], rounds: usize) -> Option<usize> {
    for _ in 0..rounds {
        sleep(Duration::from_millis(10));
        let alltasks: Vec<Vec<RaftTask>> = nodes.iter_mut().map(|n| n.tick()).collect();
        let n = nodes.len();

        for src in 0..n {
            for task in &alltasks[src] {
                let tar = peer_of(task);
                if src == tar as usize { continue; }

                let (srcnode, dstnode) = if src < tar as usize {
                    let (left, right) = nodes.split_at_mut(tar as usize);
                    (&mut left[src], &mut right[0])
                } else {
                    let (left, right) = nodes.split_at_mut(src);
                    (&mut right[0], &mut left[tar as usize])
                };

                match task {
                    RaftTask::Vote { req, electionterm, .. } => {
                        let resp = dstnode.onrequestvote(req);
                        srcnode.onvotereply(*electionterm, tar, &resp);
                    }
                    RaftTask::Append { req, sentterm, previndex, sentnum, .. } => {
                        let resp = dstnode.onappendentries(req);
                        srcnode.onappendreply(*sentterm, tar, *previndex, *sentnum, &resp);
                    }
                    RaftTask::InstallSnapshot { .. } => {}
                }
            }
        }

        for (i, node) in nodes.iter().enumerate() {
            if node.isleader() { return Some(i); }
        }
    }
    None
}

fn peer_of(task: &RaftTask) -> u32 {
    match task {
        RaftTask::Vote { peer, .. } => *peer,
        RaftTask::Append { peer, .. } => *peer,
        RaftTask::InstallSnapshot { peer, .. } => *peer,
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

#[test]
fn elects_leader_in_3_node_cluster() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let a = addrs(3);
    let mut nodes: Vec<RaftCore> = (0..3)
        .map(|i| makenode(i, a.clone(), Arc::clone(&applied)))
        .collect();
    let leader = findleader(&mut nodes, 200);
    assert!(leader.is_some(), "no leader elected");
}

#[test]
fn exactly_one_leader() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let a = addrs(3);
    let mut nodes: Vec<RaftCore> = (0..3)
        .map(|i| makenode(i, a.clone(), Arc::clone(&applied)))
        .collect();
    findleader(&mut nodes, 200);
    let count = nodes.iter().filter(|n| n.isleader()).count();
    assert_eq!(count, 1);
}

#[test]
fn leader_rejects_propose_on_non_leader() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let a = addrs(3);
    let mut nodes: Vec<RaftCore> = (0..3)
        .map(|i| makenode(i, a.clone(), Arc::clone(&applied)))
        .collect();
    let lid = findleader(&mut nodes, 200).expect("no leader");
    let fid = (lid + 1) % 3;

    let mut idx = 0;
    let mut term = 0;
    let s = nodes[fid].propose(&[1, 2, 3], &mut idx, &mut term);
    assert!(!s.isok());
    assert!(s.isnotleader());
}

#[test]
fn leader_accepts_propose() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let a = addrs(3);
    let mut nodes: Vec<RaftCore> = (0..3)
        .map(|i| makenode(i, a.clone(), Arc::clone(&applied)))
        .collect();
    let lid = findleader(&mut nodes, 200).expect("no leader");

    let mut idx = 0;
    let mut term = 0;
    let s = nodes[lid].propose(&[0xDE, 0xAD], &mut idx, &mut term);
    assert!(s.isok());
    assert!(idx > 0);
}

#[test]
fn log_replicated_after_propose() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let a = addrs(3);
    let mut nodes: Vec<RaftCore> = (0..3)
        .map(|i| makenode(i, a.clone(), Arc::clone(&applied)))
        .collect();

    let lid = findleader(&mut nodes, 200).expect("no leader");
    let mut idx = 0;
    let mut term = 0;
    nodes[lid].propose(&[0xBE, 0xEF], &mut idx, &mut term).isok();

    findleader(&mut nodes, 50);
    assert!(!applied.lock().unwrap().is_empty());
}

#[test]
fn persist_and_restore() {
    let a = vec!["127.0.0.1:7099".to_string()];

    {
        let cfg = RaftConfig {
            id: 0, peeraddrs: a.clone(), datadir: "/tmp/rt_persist".into(),
        };
        let applied = Mutex::new(Vec::new());
        let cb = Box::new(move |msg: ApplyMsg| {
            applied.lock().unwrap().push(msg);
        });
        let (state, snapshot) = makestorages(&cfg.datadir);
        let mut r1 = RaftCore::new(cfg, state, snapshot, cb);

        for _ in 0..100 {
            sleep(Duration::from_millis(10));
            r1.tick();
            if r1.isleader() { break; }
        }
        assert!(r1.isleader());

        let mut idx = 0;
        let mut term = 0;
        assert!(r1.propose(&[0xCA, 0xFE], &mut idx, &mut term).isok());
    }
}
