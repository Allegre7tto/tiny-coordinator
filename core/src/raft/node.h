#pragma once

#include "common/types.h"
#include "raft/raft.h"
#include "raft/config.h"

#include "raft.grpc.pb.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace engine::raft {

// ─── RaftNode ─────────────────────────────────────────────────────────────────
//
// Wraps Raft + a background ticker thread + gRPC stubs to all peers.
// Implements RaftInternal::Service so it can be registered directly with gRPC.
//
class RaftNode final : public ::engine::raft::v1::RaftInternal::Service {
public:
    RaftNode(const RaftConfig&             cfg,
             std::unique_ptr<Storage>      storage,
             std::function<void(ApplyMsg)> apply_cb);

    ~RaftNode();

    RaftNode(const RaftNode&)            = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // ── Public Raft API ───────────────────────────────────────────────────────
    Status Start(const std::vector<byte>& data, uint64& out_index, uint64& out_term);
    bool   IsLeader() const;
    uint64 Term()     const;
    void   TakeSnapshot(uint64 index, const std::vector<byte>& snapshot);
    bool   CondInstallSnapshot(uint64 last_term, uint64 last_index,
                                const std::vector<byte>& snapshot);

    // ── gRPC service (RaftInternal) ───────────────────────────────────────────
    grpc::Status RequestVote(grpc::ServerContext*,
                             const ::engine::raft::v1::RequestVoteReq*,
                             ::engine::raft::v1::RequestVoteResp*)     override;

    grpc::Status AppendEntries(grpc::ServerContext*,
                               const ::engine::raft::v1::AppendEntriesReq*,
                               ::engine::raft::v1::AppendEntriesResp*) override;

    grpc::Status InstallSnapshot(grpc::ServerContext*,
                                 const ::engine::raft::v1::InstallSnapshotReq*,
                                 ::engine::raft::v1::InstallSnapshotResp*) override;

private:
    void DispatchTask(const RaftTask& task);

    mutable std::mutex    mutex_;
    std::unique_ptr<Raft> raft_;

    std::vector<std::unique_ptr<::engine::raft::v1::RaftInternal::Stub>> stubs_;

    std::jthread      ticker_;
    std::atomic<bool> stopped_{false};
};

} // namespace engine::raft
