#include "raft/node.h"

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

namespace engine::raft {

// ─── Construction / Destruction ───────────────────────────────────────────────

RaftNode::RaftNode(const RaftConfig&             cfg,
                    std::unique_ptr<Storage>      storage,
                    std::function<void(ApplyMsg)> on_commit) {
    storage_ptr_ = storage.get();
    raft_ = std::make_unique<Raft>(cfg, std::move(storage), std::move(on_commit));

    for (const auto& addr : cfg.peer_addrs) {
        auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        stubs_.push_back(::engine::raft::v1::RaftInternal::NewStub(std::move(ch)));
    }

    io_thread_ = std::jthread([this](std::stop_token st) {
        void* tag;
        bool  ok;
        while (cq_.Next(&tag, &ok)) {
            if (!ok || st.stop_requested()) {
                delete static_cast<RpcTag*>(tag);
                continue;
            }
            static_cast<RpcTag*>(tag)->OnComplete(this);
            delete static_cast<RpcTag*>(tag);
        }
    });

    ticker_ = std::jthread([this](std::stop_token st) {
        while (!st.stop_requested()) {
            std::vector<RaftTask> tasks;
            {
                std::lock_guard lock(mutex_);
                tasks = raft_->Tick();
            }
            for (const auto& t : tasks) DispatchTask(t);
            std::this_thread::sleep_for(kTickInterval);
        }
    });
}

RaftNode::~RaftNode() {
    // Stop the ticker first — no more DispatchTask calls
    ticker_.request_stop();
    if (ticker_.joinable()) ticker_.join();

    // Shut down CQ — cancels all in-flight RPCs, IO thread unblocks
    cq_.Shutdown();
    // io_thread_ joins automatically in its destructor (declared after cq_)

    // At this point, all in-flight RPCs have completed or been cancelled
    // It is now safe to destroy RaftNode
}

// ─── Public API ───────────────────────────────────────────────────────────────

Status RaftNode::Start(const std::vector<std::byte>& data,
                       unsigned long long& out_index, unsigned long long& out_term) {
    std::lock_guard lock(mutex_);
    return raft_->Start(data, out_index, out_term);
}

bool RaftNode::IsLeader() const {
    std::lock_guard lock(mutex_);
    return raft_->IsLeader();
}

unsigned long long RaftNode::Term() const {
    std::lock_guard lock(mutex_);
    return raft_->Term();
}

void RaftNode::TakeSnapshot(unsigned long long index,
                            const std::vector<std::byte>& snapshot) {
    std::lock_guard lock(mutex_);
    raft_->TakeSnapshot(index, snapshot);
}

bool RaftNode::CondInstallSnapshot(unsigned long long last_term,
                                   unsigned long long last_index,
                                   const std::vector<std::byte>& snapshot) {
    std::lock_guard lock(mutex_);
    return raft_->CondInstallSnapshot(last_term, last_index, snapshot);
}

bool RaftNode::LoadSnapshot(std::vector<std::byte>& out_data,
                            unsigned long long& out_last_index,
                            unsigned long long& out_last_term) {
    std::lock_guard lock(mutex_);
    SnapshotData snap;
    if (!storage_ptr_->LoadSnapshot(snap)) return false;
    out_data = std::move(snap.data);
    out_last_index = snap.last_index;
    out_last_term = snap.last_term;
    return true;
}

// ─── Task dispatcher ─────────────────────────────────────────────────────────

void RaftNode::DispatchTask(const RaftTask& task) {
    std::visit([this](const auto& t) {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, VoteTask>) {
            auto tag = new VoteTag;
            tag->ctx.set_deadline(std::chrono::system_clock::now() +
                                  std::chrono::milliseconds(500));
            tag->election_term = t.election_term;
            tag->peer          = t.peer;

            auto reader = stubs_[t.peer]->AsyncRequestVote(&tag->ctx, t.req, &cq_);
            tag->reader = std::move(reader);
            tag->reader->Finish(&tag->resp, &tag->status, tag);

        } else if constexpr (std::is_same_v<T, AppendTask>) {
            auto tag = new AppendTag;
            tag->ctx.set_deadline(std::chrono::system_clock::now() +
                                  std::chrono::milliseconds(500));
            tag->sent_term  = t.sent_term;
            tag->peer       = t.peer;
            tag->prev_index = t.prev_index;
            tag->sent_num   = t.sent_num;

            auto reader = stubs_[t.peer]->AsyncAppendEntries(&tag->ctx, t.req, &cq_);
            tag->reader = std::move(reader);
            tag->reader->Finish(&tag->resp, &tag->status, tag);

        } else if constexpr (std::is_same_v<T, SnapshotTask>) {
            auto tag = new SnapshotTag;
            tag->ctx.set_deadline(std::chrono::system_clock::now() +
                                  std::chrono::milliseconds(2000));
            tag->sent_term  = t.sent_term;
            tag->peer       = t.peer;
            tag->last_index = t.last_index;

            auto reader = stubs_[t.peer]->AsyncInstallSnapshot(&tag->ctx, t.req, &cq_);
            tag->reader = std::move(reader);
            tag->reader->Finish(&tag->resp, &tag->status, tag);
        }
    }, task);
}

// ─── gRPC service handlers ────────────────────────────────────────────────────

grpc::Status RaftNode::RequestVote(grpc::ServerContext*,
        const ::engine::raft::v1::RequestVoteReq* req,
        ::engine::raft::v1::RequestVoteResp* resp) {
    std::lock_guard lock(mutex_);
    *resp = raft_->OnRequestVote(*req);
    return grpc::Status::OK;
}

grpc::Status RaftNode::AppendEntries(grpc::ServerContext*,
        const ::engine::raft::v1::AppendEntriesReq* req,
        ::engine::raft::v1::AppendEntriesResp* resp) {
    std::lock_guard lock(mutex_);
    *resp = raft_->OnAppendEntries(*req);
    return grpc::Status::OK;
}

grpc::Status RaftNode::InstallSnapshot(grpc::ServerContext*,
        const ::engine::raft::v1::InstallSnapshotReq* req,
        ::engine::raft::v1::InstallSnapshotResp* resp) {
    std::lock_guard lock(mutex_);
    *resp = raft_->OnInstallSnapshot(*req);
    return grpc::Status::OK;
}

} // namespace engine::raft
