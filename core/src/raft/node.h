#pragma once

#include "grpcpp/completion_queue.h"
#include "raft/raft.h"
#include "raft/config.h"

#include "raft.grpc.pb.h"

#include <memory>
#include <mutex>
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
             std::function<void(ApplyMsg)> on_commit);

    ~RaftNode();

    RaftNode(const RaftNode&)            = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // ── Public Raft API ───────────────────────────────────────────────────────
    Status Start(const std::vector<std::byte>& data,
                 unsigned long long& out_index, unsigned long long& out_term);
    bool                 IsLeader() const;
    unsigned long long   Term()     const;
    void TakeSnapshot(unsigned long long index, const std::vector<std::byte>& snapshot);
    bool CondInstallSnapshot(unsigned long long last_term, unsigned long long last_index,
                             const std::vector<std::byte>& snapshot);
    bool LoadSnapshot(std::vector<std::byte>& out_data,
                      unsigned long long& out_last_index,
                      unsigned long long& out_last_term);

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
    // ── Async RPC tag types ────────────────────────────────────────────────
    struct RpcTag {
        grpc::ClientContext ctx;
        grpc::Status        status;
        virtual ~RpcTag() = default;
        virtual void OnComplete(RaftNode* node) = 0;
    };

    struct VoteTag final : RpcTag {
        ::engine::raft::v1::RequestVoteResp resp;
        std::unique_ptr<grpc::ClientAsyncResponseReader<
            ::engine::raft::v1::RequestVoteResp>> reader;
        unsigned long long election_term;
        unsigned int peer;

        void OnComplete(RaftNode* node) override {
            if (!status.ok()) return;
            std::lock_guard lock(node->mutex_);
            node->raft_->OnVoteReply(election_term, peer, resp);
        }
    };

    struct AppendTag final : RpcTag {
        ::engine::raft::v1::AppendEntriesResp resp;
        std::unique_ptr<grpc::ClientAsyncResponseReader<
            ::engine::raft::v1::AppendEntriesResp>> reader;
        unsigned long long sent_term;
        unsigned long long prev_index;
        unsigned int       sent_num;
        unsigned int       peer;

        void OnComplete(RaftNode* node) override {
            if (!status.ok()) return;
            std::lock_guard lock(node->mutex_);
            node->raft_->OnAppendReply(sent_term, peer, prev_index, sent_num, resp);
        }
    };

    struct SnapshotTag final : RpcTag {
        ::engine::raft::v1::InstallSnapshotResp resp;
        std::unique_ptr<grpc::ClientAsyncResponseReader<
            ::engine::raft::v1::InstallSnapshotResp>> reader;
        unsigned long long sent_term;
        unsigned long long last_index;
        unsigned int       peer;

        void OnComplete(RaftNode* node) override {
            if (!status.ok()) return;
            std::lock_guard lock(node->mutex_);
            node->raft_->OnSnapshotReply(sent_term, peer, last_index, resp);
        }
    };

    void DispatchTask(const RaftTask& task);

    mutable std::mutex    mutex_;
    std::unique_ptr<Raft> raft_;

    std::vector<std::unique_ptr<::engine::raft::v1::RaftInternal::Stub>> stubs_;

    std::jthread          ticker_;
    grpc::CompletionQueue cq_;
    std::jthread          io_thread_;
    Storage*              storage_ptr_ = nullptr; // owned by Raft
};

} // namespace engine::raft
