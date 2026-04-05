#pragma once

#include "common/types.h"
#include "raft/node.h"

#include "log.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

namespace engine::server {

// ─── RaftLogServiceImpl ───────────────────────────────────────────────────────
//
// Implements raft_log.proto RaftLog service.
// C++ knows nothing about KV / Watch / Lease — it only replicates opaque bytes.
//
//   Propose            — submit a log entry through Raft consensus
//   SubscribeCommitted — server-streaming: push committed entries to Java
//   SaveSnapshot       — client-streaming: Java uploads snapshot, C++ stores it
//   LoadSnapshot       — server-streaming: C++ sends stored snapshot to Java
//   Status             — cluster status (is_leader / term)
//
class RaftLogServiceImpl final : public ::engine::log::v1::RaftLog::Service {
public:
    RaftLogServiceImpl();

    void SetNode(raft::RaftNode* node);

    grpc::Status Propose(grpc::ServerContext*,
                         const ::engine::log::v1::ProposeReq*,
                         ::engine::log::v1::ProposeResp*)          override;

    grpc::Status SubscribeCommitted(grpc::ServerContext*,
                                    const ::engine::log::v1::SubscribeReq*,
                                    grpc::ServerWriter<::engine::log::v1::CommittedEntry>*) override;

    grpc::Status SaveSnapshot(grpc::ServerContext*,
                              grpc::ServerReader<::engine::log::v1::SnapshotChunk>*,
                              ::engine::log::v1::SaveSnapshotResp*) override;

    grpc::Status LoadSnapshot(grpc::ServerContext*,
                              const ::engine::log::v1::LoadSnapshotReq*,
                              grpc::ServerWriter<::engine::log::v1::SnapshotChunk>*) override;

    grpc::Status Status(grpc::ServerContext*,
                        const ::engine::log::v1::StatusReq*,
                        ::engine::log::v1::StatusResp*)            override;

    // Called by the Raft apply callback to enqueue committed entries.
    void OnCommitted(uint64 index, uint64 term, std::vector<byte> data);

    void Shutdown();

private:
    raft::RaftNode* node_;

    mutable std::mutex            mu_;
    std::deque<::engine::log::v1::CommittedEntry> committed_queue_;
    std::condition_variable       committed_cv_;
    bool                          shutdown_ = false;
};

// ─── GrpcServer ───────────────────────────────────────────────────────────────
//
// Single port, two services:
//   RaftInternal (raft.proto)     — Raft peer-to-peer consensus
//   RaftLog      (raft_log.proto) — Java ↔ C++ replicated log interface
//
class GrpcServer {
public:
    GrpcServer(const std::string&    listen_addr,
               raft::RaftNode*       raft_node,
               RaftLogServiceImpl*   raft_log_svc);

    void Start();
    void Shutdown();

private:
    std::string                   listen_addr_;
    raft::RaftNode*               raft_node_;
    RaftLogServiceImpl*           raft_log_svc_;
    std::unique_ptr<grpc::Server> server_;
};

} // namespace engine::server
