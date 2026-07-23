use std::time::Duration;

use proto::raft::v1::raft_client::RaftClient;
use proto::raft::v1::raft_server::{Raft, RaftServer};
use proto::raft::v1::{
    AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp,
    RequestVoteReq, RequestVoteResp,
};
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

pub type RpcResult<T> = Result<T, Status>;

#[derive(Debug)]
pub enum PeerRpc {
    Vote {
        req: RequestVoteReq,
        respond: oneshot::Sender<RpcResult<RequestVoteResp>>,
    },
    Append {
        req: AppendEntriesReq,
        respond: oneshot::Sender<RpcResult<AppendEntriesResp>>,
    },
    Snapshot {
        req: InstallSnapshotReq,
        respond: oneshot::Sender<RpcResult<InstallSnapshotResp>>,
    },
}

#[derive(Clone)]
pub struct PeerService {
    pub rpctx: mpsc::UnboundedSender<PeerRpc>,
}

#[tonic::async_trait]
impl Raft for PeerService {
    async fn request_vote(
        &self, req: Request<RequestVoteReq>,
    ) -> Result<Response<RequestVoteResp>, Status> {
        let (tx, rx) = oneshot::channel();
        self.rpctx.send(PeerRpc::Vote { req: req.into_inner(), respond: tx }).ok();
        rx.await.unwrap_or_else(|_| Ok(RequestVoteResp::default()))
            .map(Response::new)
    }

    async fn append_entries(
        &self, req: Request<AppendEntriesReq>,
    ) -> Result<Response<AppendEntriesResp>, Status> {
        let (tx, rx) = oneshot::channel();
        self.rpctx.send(PeerRpc::Append { req: req.into_inner(), respond: tx }).ok();
        rx.await.unwrap_or_else(|_| Ok(AppendEntriesResp::default()))
            .map(Response::new)
    }

    async fn install_snapshot(
        &self, req: Request<InstallSnapshotReq>,
    ) -> Result<Response<InstallSnapshotResp>, Status> {
        let (tx, rx) = oneshot::channel();
        self.rpctx.send(PeerRpc::Snapshot { req: req.into_inner(), respond: tx }).ok();
        rx.await.unwrap_or_else(|_| Ok(InstallSnapshotResp::default()))
            .map(Response::new)
    }
}

pub fn peerservice(rpctx: mpsc::UnboundedSender<PeerRpc>) -> RaftServer<PeerService> {
    RaftServer::new(PeerService { rpctx })
}

pub async fn connectpeers(addrs: &[String]) -> Vec<RaftClient<Channel>> {
    let mut clients = Vec::new();
    for addr in addrs {
        let ch = match tonic::transport::Endpoint::from_shared(addr.clone()) {
            Ok(ep) => match ep.connect().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("connect to {} failed: {}", addr, e);
                    continue;
                }
            },
            Err(e) => {
                tracing::warn!("invalid address {}: {}", addr, e);
                continue;
            }
        };
        clients.push(RaftClient::new(ch));
    }
    clients
}

pub async fn sendvote(
    client: &mut RaftClient<Channel>,
    req: RequestVoteReq,
    timeout: Duration,
) -> RpcResult<RequestVoteResp> {
    match tokio::time::timeout(timeout, client.request_vote(req)).await {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(Status::deadline_exceeded("vote timeout")),
    }
}

pub async fn sendappend(
    client: &mut RaftClient<Channel>,
    req: AppendEntriesReq,
    timeout: Duration,
) -> RpcResult<AppendEntriesResp> {
    match tokio::time::timeout(timeout, client.append_entries(req)).await {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(Status::deadline_exceeded("append timeout")),
    }
}

pub async fn sendsnapshot(
    client: &mut RaftClient<Channel>,
    req: InstallSnapshotReq,
    timeout: Duration,
) -> RpcResult<InstallSnapshotResp> {
    match tokio::time::timeout(timeout, client.install_snapshot(req)).await {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(Status::deadline_exceeded("snapshot timeout")),
    }
}
