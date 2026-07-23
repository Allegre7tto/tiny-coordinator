use std::time::Duration;

use crate::config::{RaftConfig, TICK_INTERVAL};
use crate::peer::{sendappend, sendsnapshot, sendvote, PeerRpc};
use crate::raftcore::{ApplyMsg, RaftCore, RaftTask};
use crate::storage::SnapshotData;
use crate::wal::{SnapshotStorage, StateStorage};
use proto::raft::v1::raft_client::RaftClient;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;

pub type CommittedFn = Box<dyn Fn(u64, u64, &[u8]) + Send + 'static>;

pub struct ProposeReq {
    pub data: Vec<u8>,
    pub respond: oneshot::Sender<ProposeResult>,
}

pub struct ProposeResult {
    pub status: crate::status::Status,
    pub index: u64,
    pub term: u64,
}

pub struct SnapSaveReq {
    pub index: u64,
    pub data: Vec<u8>,
}

pub type SnapLoadReq = oneshot::Sender<Option<SnapshotData>>;

pub struct RaftNode {
    pub proposetx: mpsc::UnboundedSender<ProposeReq>,
    pub snapsavetx: mpsc::UnboundedSender<SnapSaveReq>,
    pub snaploadtx: mpsc::UnboundedSender<SnapLoadReq>,
    pub isleader: Box<dyn Fn() -> bool + Send + Sync + 'static>,
    pub getterm: Box<dyn Fn() -> u64 + Send + Sync + 'static>,
}

impl RaftNode {
    pub fn start(
        cfg: RaftConfig,
        state: StateStorage,
        snapshot: SnapshotStorage,
        peerclients: Vec<RaftClient<Channel>>,
        mut peerrx: mpsc::UnboundedReceiver<PeerRpc>,
        committed: CommittedFn,
    ) -> Self {
        let (proposetx, mut proposerx) = mpsc::unbounded_channel::<ProposeReq>();
        let (snapsavetx, mut snapsaverx) = mpsc::unbounded_channel::<SnapSaveReq>();
        let (snaploadtx, mut snaploadrx) = mpsc::unbounded_channel::<SnapLoadReq>();

        tokio::spawn(async move {
            let (rpcreplytx, mut rpcreplyrx) = mpsc::unbounded_channel::<RaftTaskReply>();

            let oncommit = move |msg: ApplyMsg| match msg {
                ApplyMsg::Command { data, index, term } => committed(index, term, &data),
                ApplyMsg::Snapshot { index, .. } => {
                    tracing::info!("snapshot applied at index={}", index);
                }
            };

            let mut raft = RaftCore::new(cfg, state, snapshot, Box::new(oncommit));
            let mut ticker = tokio::time::interval(TICK_INTERVAL);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let tasks = raft.tick();
                        for task in tasks {
                            match task {
                                RaftTask::Vote { peer, req, electionterm } => {
                                    let mut client = peerclients[peer as usize].clone();
                                    let tx = rpcreplytx.clone();
                                    tokio::spawn(async move {
                                        let result = sendvote(&mut client, req, Duration::from_millis(500)).await;
                                        tx.send(RaftTaskReply::Vote { peer, electionterm, result }).ok();
                                    });
                                }
                                RaftTask::Append { peer, req, sentterm, previndex, sentnum } => {
                                    let mut client = peerclients[peer as usize].clone();
                                    let tx = rpcreplytx.clone();
                                    tokio::spawn(async move {
                                        let result = sendappend(&mut client, req, Duration::from_millis(500)).await;
                                        tx.send(RaftTaskReply::Append { peer, sentterm, previndex, sentnum, result }).ok();
                                    });
                                }
                                RaftTask::InstallSnapshot { peer, req, sentterm, lastindex } => {
                                    let mut client = peerclients[peer as usize].clone();
                                    let tx = rpcreplytx.clone();
                                    tokio::spawn(async move {
                                        let result = sendsnapshot(&mut client, req, Duration::from_millis(2000)).await;
                                        tx.send(RaftTaskReply::Snapshot { peer, sentterm, lastindex, result }).ok();
                                    });
                                }
                            }
                        }
                    }
                    Some(req) = proposerx.recv() => {
                        let mut index = 0u64;
                        let mut term = 0u64;
                        let status = raft.propose(&req.data, &mut index, &mut term);
                        req.respond.send(ProposeResult { status, index, term }).ok();
                    }
                    Some(rpc) = peerrx.recv() => {
                        match rpc {
                            PeerRpc::Vote { req, respond } => {
                                let resp = raft.onrequestvote(&req);
                                respond.send(Ok(resp)).ok();
                            }
                            PeerRpc::Append { req, respond } => {
                                let resp = raft.onappendentries(&req);
                                respond.send(Ok(resp)).ok();
                            }
                            PeerRpc::Snapshot { req, respond } => {
                                let resp = raft.oninstallsnapshot(&req);
                                respond.send(Ok(resp)).ok();
                            }
                        }
                    }
                    Some(reply) = rpcreplyrx.recv() => {
                        match reply {
                            RaftTaskReply::Vote { peer, electionterm, result } => {
                                if let Ok(resp) = result {
                                    raft.onvotereply(electionterm, peer, &resp);
                                }
                            }
                            RaftTaskReply::Append { peer, sentterm, previndex, sentnum, result } => {
                                if let Ok(resp) = result {
                                    raft.onappendreply(sentterm, peer, previndex, sentnum, &resp);
                                }
                            }
                            RaftTaskReply::Snapshot { peer, sentterm, lastindex, result } => {
                                if let Ok(resp) = result {
                                    raft.onsnapshotreply(sentterm, peer, lastindex, &resp);
                                }
                            }
                        }
                    }
                    Some(req) = snapsaverx.recv() => {
                        raft.takesnapshot(req.index, &req.data);
                    }
                    Some(respond) = snaploadrx.recv() => {
                        let data = raft.loadsnapshotdata();
                        respond.send(data).ok();
                    }
                }
            }
        });

        RaftNode {
            proposetx,
            snapsavetx,
            snaploadtx,
            isleader: Box::new(|| false),
            getterm: Box::new(|| 0),
        }
    }
}

enum RaftTaskReply {
    Vote {
        peer: u32,
        electionterm: u64,
        result: Result<proto::raft::v1::RequestVoteResp, tonic::Status>,
    },
    Append {
        peer: u32,
        sentterm: u64,
        previndex: u64,
        sentnum: u32,
        result: Result<proto::raft::v1::AppendEntriesResp, tonic::Status>,
    },
    Snapshot {
        peer: u32,
        sentterm: u64,
        lastindex: u64,
        result: Result<proto::raft::v1::InstallSnapshotResp, tonic::Status>,
    },
}
