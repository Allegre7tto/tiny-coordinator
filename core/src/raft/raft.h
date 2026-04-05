#pragma once

#include "common/types.h"
#include "raft/config.h"
#include "raft/log.h"
#include "raft/storage.h"
#include "common/status.h"

#include "raft.pb.h"

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_set>
#include <variant>
#include <vector>

namespace engine::raft {

using Clock     = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

// ─── Apply messages ───────────────────────────────────────────────────────────
struct ApplyCommand {
    std::vector<byte> data;
    uint64            index = 0;
};

struct ApplySnapshot {
    std::vector<byte> data;
    uint64            term  = 0;
    uint64            index = 0;
};

using ApplyMsg = std::variant<ApplyCommand, ApplySnapshot>;

// ─── Outbound RPC tasks produced by Raft::Tick() ─────────────────────────────
struct VoteTask {
    size                             peer;
    ::engine::raft::v1::RequestVoteReq req;
    uint64                           election_term;
};

struct AppendTask {
    size                                peer;
    ::engine::raft::v1::AppendEntriesReq req;
    uint64                              sent_term;
    uint64                              prev_index;
    size                                sent_num;
};

struct SnapshotTask {
    size                                  peer;
    ::engine::raft::v1::InstallSnapshotReq req;
    uint64                                sent_term;
    uint64                                last_index;
};

using RaftTask = std::variant<VoteTask, AppendTask, SnapshotTask>;

// ─── Role ─────────────────────────────────────────────────────────────────────
enum class Role { Follower, Candidate, Leader };

// ─── Raft state machine ───────────────────────────────────────────────────────
//
// Pure logic — no I/O, no threads.
// All public methods must be called while holding an external mutex (in Node).
// Tick() returns RPCs that Node should dispatch outside the lock.
//
class Raft {
public:
    Raft(const RaftConfig&             cfg,
         std::unique_ptr<Storage>      storage,
         std::function<void(ApplyMsg)> apply_cb);

    std::vector<RaftTask> Tick();

    Status Start(const std::vector<byte>& data, uint64& out_index, uint64& out_term);

    void TakeSnapshot(uint64 index, const std::vector<byte>& snapshot);
    bool CondInstallSnapshot(uint64 last_term, uint64 last_index,
                             const std::vector<byte>& snapshot);

    ::engine::raft::v1::RequestVoteResp     OnRequestVote    (const ::engine::raft::v1::RequestVoteReq&);
    ::engine::raft::v1::AppendEntriesResp   OnAppendEntries  (const ::engine::raft::v1::AppendEntriesReq&);
    ::engine::raft::v1::InstallSnapshotResp OnInstallSnapshot(const ::engine::raft::v1::InstallSnapshotReq&);

    void OnVoteReply    (uint64 election_term, size peer,
                         const ::engine::raft::v1::RequestVoteResp&);
    void OnAppendReply  (uint64 sent_term, size peer, uint64 prev_index, size sent_num,
                         const ::engine::raft::v1::AppendEntriesResp&);
    void OnSnapshotReply(uint64 sent_term, size peer, uint64 last_index,
                         const ::engine::raft::v1::InstallSnapshotResp&);

    bool   IsLeader()   const { return role_ == Role::Leader; }
    uint64 Term()       const { return term_; }
    size   Me()         const { return me_; }
    size   PeerCount()  const { return peer_count_; }

private:
    static std::chrono::milliseconds RandElectionTimeout();

    void   StepDown(uint64 term);
    void   StepUp();
    uint64 BeginElection();
    bool   HasQuorum() const;

    void AdvanceCommit();
    void ApplyReady();

    AppendTask   BuildAppendTask(size peer);
    SnapshotTask BuildSnapshotTask(size peer);

    void Persist();
    void PersistWithSnapshot();
    void Restore(const HardState& hs);

    uint64 LastIndex() const { return base_ + static_cast<uint64>(log_.size()) - 1; }
    uint64 LastTerm()  const { return log_.back().term(); }
    uint64 LogSize()   const { return static_cast<uint64>(log_.size()); }
    size   LogOffset(uint64 abs_idx) const { return static_cast<size>(abs_idx - base_); }

    size                          me_;
    size                          peer_count_;
    std::unique_ptr<Storage>      storage_;
    std::function<void(ApplyMsg)> apply_cb_;

    Role                          role_    = Role::Follower;
    uint64                        term_    = 0;
    std::optional<size>           voted_;
    std::unordered_set<size>      granted_;
    TimePoint                     deadline_;
    TimePoint                     heartbeat_;

    std::vector<LogEntry>         log_;
    uint64                        base_    = 0;
    uint64                        anchor_  = 0;
    uint64                        commit_  = 0;
    uint64                        applied_ = 0;
    std::vector<uint64>           next_;
    std::vector<uint64>           acked_;
    std::vector<byte>             snap_;
};

} // namespace engine::raft
