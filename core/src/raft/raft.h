#pragma once

#include "raft/config.h"
#include "raft/log.h"
#include "raft/storage.h"
#include "common/status.h"

#include "raft.pb.h"

#include <chrono>
#include <cstddef>
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
    std::vector<std::byte> data;
    unsigned long long     index = 0;
    unsigned long long     term  = 0;
};

struct ApplySnapshot {
    std::vector<std::byte> data;
    unsigned long long     term  = 0;
    unsigned long long     index = 0;
};

using ApplyMsg = std::variant<ApplyCommand, ApplySnapshot>;

// ─── Outbound RPC tasks produced by Raft::Tick() ─────────────────────────────
struct VoteTask {
    unsigned int                       peer;
    ::engine::raft::v1::RequestVoteReq req;
    unsigned long long                 election_term;
};

struct AppendTask {
    unsigned int                          peer;
    ::engine::raft::v1::AppendEntriesReq  req;
    unsigned long long                  sent_term;
    unsigned long long                  prev_index;
    unsigned int                          sent_num;
};

struct SnapshotTask {
    unsigned int                           peer;
    ::engine::raft::v1::InstallSnapshotReq req;
    unsigned long long                   sent_term;
    unsigned long long                   last_index;
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
         std::function<void(ApplyMsg)> on_commit);

    std::vector<RaftTask> Tick();

    Status Start(const std::vector<std::byte>& data,
                 unsigned long long& out_index, unsigned long long& out_term);

    void TakeSnapshot(unsigned long long index, const std::vector<std::byte>& snapshot);
    bool CondInstallSnapshot(unsigned long long last_term, unsigned long long last_index,
                             const std::vector<std::byte>& snapshot);

    ::engine::raft::v1::RequestVoteResp     OnRequestVote    (const ::engine::raft::v1::RequestVoteReq&);
    ::engine::raft::v1::AppendEntriesResp   OnAppendEntries  (const ::engine::raft::v1::AppendEntriesReq&);
    ::engine::raft::v1::InstallSnapshotResp OnInstallSnapshot(const ::engine::raft::v1::InstallSnapshotReq&);

    void OnVoteReply    (unsigned long long election_term, unsigned int peer,
                         const ::engine::raft::v1::RequestVoteResp&);
    void OnAppendReply  (unsigned long long sent_term, unsigned int peer,
                         unsigned long long prev_index, unsigned int sent_num,
                         const ::engine::raft::v1::AppendEntriesResp&);
    void OnSnapshotReply(unsigned long long sent_term, unsigned int peer,
                         unsigned long long last_index,
                         const ::engine::raft::v1::InstallSnapshotResp&);

    bool                 IsLeader()   const { return role_ == Role::Leader; }
    unsigned long long   Term()       const { return term_; }
    unsigned int         Me()         const { return me_; }
    unsigned int         PeerCount()  const { return peer_count_; }

private:
    static std::chrono::milliseconds RandElectionTimeout();

    void               StepDown(unsigned long long term);
    void               StepUp();
    unsigned long long BeginElection();
    bool               HasQuorum() const;

    void AdvanceCommit();
    void ApplyReady();

    AppendTask   BuildAppendTask(unsigned int peer);
    SnapshotTask BuildSnapshotTask(unsigned int peer);

    void Persist();
    void PersistWithSnapshot();
    void Restore(const HardState& hs);

    unsigned long long LastIndex() const {
        return base_ + static_cast<unsigned long long>(log_.size()) - 1;
    }
    unsigned long long LastTerm()  const { return log_.back().term(); }
    unsigned long long LogSize()   const {
        return static_cast<unsigned long long>(log_.size());
    }
    unsigned int LogOffset(unsigned long long abs_idx) const {
        return static_cast<unsigned int>(abs_idx - base_);
    }

    unsigned int                  me_;
    unsigned int                  peer_count_;
    std::unique_ptr<Storage>      storage_;
    std::function<void(ApplyMsg)> on_commit_;

    Role                          role_    = Role::Follower;
    unsigned long long            term_    = 0;
    std::optional<unsigned int>   voted_;
    std::unordered_set<unsigned int> granted_;
    TimePoint                     deadline_;
    TimePoint                     heartbeat_;

    std::vector<LogEntry>         log_;
    unsigned long long            base_    = 0;
    unsigned long long            anchor_  = 0;
    unsigned long long            commit_  = 0;
    unsigned long long            applied_ = 0;
    std::vector<unsigned long long> next_;
    std::vector<unsigned long long> acked_;
    std::vector<std::byte>        snap_;
};

} // namespace engine::raft
