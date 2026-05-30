#include "raft/raft.h"

#include <algorithm>
#include <random>

namespace engine::raft {

// ─── Construction ─────────────────────────────────────────────────────────────

Raft::Raft(const RaftConfig&             cfg,
           std::unique_ptr<Storage>      storage,
           std::function<void(ApplyMsg)> on_commit)
    : me_(static_cast<unsigned int>(cfg.id))
    , peer_count_(static_cast<unsigned int>(cfg.peer_addrs.size()))
    , storage_(std::move(storage))
    , on_commit_(std::move(on_commit))
{
    LogEntry sentinel;
    sentinel.set_term(0);
    log_.push_back(std::move(sentinel));

    next_.assign(peer_count_, 1);
    acked_.assign(peer_count_, 0);

    auto now   = Clock::now();
    deadline_  = now + RandElectionTimeout();
    heartbeat_ = now;

    HardState hs;
    if (storage_->LoadState(hs)) Restore(hs);

    SnapshotData snap;
    if (storage_->LoadSnapshot(snap))
        snap_.assign(snap.data.begin(), snap.data.end());
}

// ─── Timing ───────────────────────────────────────────────────────────────────

std::chrono::milliseconds Raft::RandElectionTimeout() {
    static thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<long long> dist(
        kElectionTimeoutMin.count(), kElectionTimeoutMax.count());
    return std::chrono::milliseconds(dist(rng));
}

// ─── Persist / Restore ────────────────────────────────────────────────────────

void Raft::Persist() {
    HardState hs;
    hs.term   = term_;
    hs.voted  = voted_.has_value() ? static_cast<unsigned long long>(*voted_ + 1) : 0;
    hs.base   = base_;
    hs.anchor = anchor_;
    hs.log    = log_;
    storage_->SaveState(hs);
}

void Raft::PersistWithSnapshot() {
    HardState hs;
    hs.term   = term_;
    hs.voted  = voted_.has_value() ? static_cast<unsigned long long>(*voted_ + 1) : 0;
    hs.base   = base_;
    hs.anchor = anchor_;
    hs.log    = log_;

    SnapshotData snap;
    snap.last_index = base_;
    snap.last_term  = anchor_;
    snap.data.assign(snap_.begin(), snap_.end());
    storage_->SaveSnapshot(hs, snap);
}

void Raft::Restore(const HardState& hs) {
    term_   = hs.term;
    voted_  = hs.voted == 0
        ? std::nullopt
        : std::optional<unsigned int>(static_cast<unsigned int>(hs.voted - 1));
    base_   = hs.base;
    anchor_ = hs.anchor;

    if (!hs.log.empty()) log_ = hs.log;
    if (log_.empty()) {
        LogEntry sentinel;
        sentinel.set_term(anchor_);
        log_.push_back(std::move(sentinel));
    }
    log_[0].set_term(anchor_);
    log_[0].clear_data();

    commit_  = base_;
    applied_ = base_;
    unsigned long long next_val = base_ + LogSize();
    next_.assign(peer_count_, next_val);
    acked_.assign(peer_count_, base_);
    acked_[me_] = base_ + LogSize() - 1;
}

// ─── Role transitions ─────────────────────────────────────────────────────────

void Raft::StepDown(unsigned long long term) {
    if (term > term_) { term_ = term; voted_ = std::nullopt; Persist(); }
    role_ = Role::Follower;
    granted_.clear();
    deadline_ = Clock::now() + RandElectionTimeout();
}

bool Raft::HasQuorum() const { return granted_.size() > peer_count_ / 2; }

unsigned long long Raft::BeginElection() {
    role_  = Role::Candidate;
    term_ += 1;
    voted_ = me_;
    granted_.clear();
    granted_.insert(me_);
    deadline_ = Clock::now() + RandElectionTimeout();
    Persist();
    if (HasQuorum()) StepUp();
    return term_;
}

void Raft::StepUp() {
    role_ = Role::Leader;
    granted_.clear();
    unsigned long long nv = base_ + LogSize();
    std::fill(next_.begin(),  next_.end(),  nv);
    std::fill(acked_.begin(), acked_.end(), base_);
    acked_[me_] = base_ + LogSize() - 1;
    next_[me_]  = base_ + LogSize();
    heartbeat_  = Clock::now() - kHeartbeatInterval;
}

// ─── Tick ─────────────────────────────────────────────────────────────────────

std::vector<RaftTask> Raft::Tick() {
    std::vector<RaftTask> tasks;
    auto now = Clock::now();

    if (role_ == Role::Leader) {
        if (now - heartbeat_ >= kHeartbeatInterval) {
            heartbeat_ = now;
            for (unsigned int peer = 0; peer < peer_count_; ++peer) {
                if (peer == me_) continue;
                if (next_[peer] <= base_)
                    tasks.push_back(BuildSnapshotTask(peer));
                else
                    tasks.push_back(BuildAppendTask(peer));
            }
        }
    } else if (now >= deadline_) {
        unsigned long long et = BeginElection();
        unsigned long long li = LastIndex();
        unsigned long long lt = log_[LogOffset(li)].term();
        for (unsigned int peer = 0; peer < peer_count_; ++peer) {
            if (peer == me_) continue;
            ::engine::raft::v1::RequestVoteReq req;
            req.set_term(et);
            req.set_candidate(static_cast<unsigned long long>(me_));
            req.set_last_index(li);
            req.set_last_term(lt);
            tasks.push_back(VoteTask{peer, req, et});
        }
    }
    return tasks;
}

// ─── Build RPC args ───────────────────────────────────────────────────────────

AppendTask Raft::BuildAppendTask(unsigned int peer) {
    unsigned long long next = next_[peer];
    unsigned long long prev = next - 1;

    ::engine::raft::v1::AppendEntriesReq req;
    req.set_term(term_);
    req.set_leader_id(static_cast<unsigned long long>(me_));
    req.set_prev_index(prev);
    req.set_prev_term(log_[LogOffset(prev)].term());
    req.set_leader_commit(commit_);

    unsigned int sent_num = 0;
    if (next <= LastIndex()) {
        for (unsigned int i = LogOffset(next); i < log_.size(); ++i) {
            *req.add_entries() = log_[i];
            ++sent_num;
        }
    }
    return AppendTask{peer, std::move(req), term_, prev, sent_num};
}

SnapshotTask Raft::BuildSnapshotTask(unsigned int peer) {
    ::engine::raft::v1::InstallSnapshotReq req;
    req.set_term(term_);
    req.set_leader_id(static_cast<unsigned long long>(me_));
    req.set_last_index(base_);
    req.set_last_term(anchor_);
    req.set_data(
        reinterpret_cast<const char*>(snap_.data()),
        snap_.size());
    return SnapshotTask{peer, std::move(req), term_, base_};
}

// ─── Commit / Apply ───────────────────────────────────────────────────────────

void Raft::AdvanceCommit() {
    if (role_ != Role::Leader) return;
    unsigned long long target = commit_;
    for (long long idx = static_cast<long long>(LastIndex());
         idx > static_cast<long long>(commit_); --idx) {
        unsigned long long uidx = static_cast<unsigned long long>(idx);
        if (log_[LogOffset(uidx)].term() != term_) continue;
        unsigned int replicated = 0;
        for (unsigned long long a : acked_) if (a >= uidx) ++replicated;
        if (replicated > peer_count_ / 2) { target = uidx; break; }
    }
    if (target > commit_) { commit_ = target; ApplyReady(); }
}

void Raft::ApplyReady() {
    while (applied_ < commit_) {
        ++applied_;
        const auto& e = log_[LogOffset(applied_)];
        std::vector<std::byte> data(
            reinterpret_cast<const std::byte*>(e.data().data()),
            reinterpret_cast<const std::byte*>(e.data().data() + e.data().size()));
        on_commit_(ApplyCommand{std::move(data), applied_, e.term()});
    }
}

// ─── Start ────────────────────────────────────────────────────────────────────

Status Raft::Start(const std::vector<std::byte>& data,
                   unsigned long long& out_index, unsigned long long& out_term) {
    if (role_ != Role::Leader) return Status::NotLeader();
    LogEntry entry;
    entry.set_term(term_);
    entry.set_data(
        reinterpret_cast<const char*>(data.data()),
        data.size());
    log_.push_back(std::move(entry));
    Persist();
    out_index   = LastIndex();
    out_term    = term_;
    acked_[me_] = out_index;
    next_[me_]  = out_index + 1;
    heartbeat_  = Clock::now() - kHeartbeatInterval;
    return Status::OK();
}

// ─── Snapshot ─────────────────────────────────────────────────────────────────

void Raft::TakeSnapshot(unsigned long long index, const std::vector<std::byte>& snapshot) {
    if (index <= base_ || index > commit_ || index > LastIndex()) return;
    unsigned long long t = log_[LogOffset(index)].term();
    std::vector<LogEntry> new_log;
    LogEntry sentinel; sentinel.set_term(t); new_log.push_back(std::move(sentinel));
    if (index < LastIndex())
        new_log.insert(new_log.end(),
                       log_.begin() + LogOffset(index + 1),
                       log_.end());
    log_    = std::move(new_log);
    base_   = index;
    anchor_ = t;
    snap_   = snapshot;
    commit_ = std::max(commit_, base_);
    applied_= std::max(applied_, base_);
    if (role_ == Role::Leader) {
        acked_[me_] = base_ + LogSize() - 1;
        next_[me_]  = base_ + LogSize();
    }
    PersistWithSnapshot();
}

bool Raft::CondInstallSnapshot(unsigned long long last_term, unsigned long long last_index,
                                const std::vector<std::byte>& snapshot) {
    if (last_index <= base_ || last_index <= commit_) return false;
    std::vector<LogEntry> new_log;
    LogEntry sentinel; sentinel.set_term(last_term); new_log.push_back(std::move(sentinel));
    if (last_index < LastIndex()
        && log_[LogOffset(last_index)].term() == last_term)
        new_log.insert(new_log.end(),
                       log_.begin() + LogOffset(last_index + 1),
                       log_.end());
    log_    = std::move(new_log);
    base_   = last_index;
    anchor_ = last_term;
    commit_ = std::max(commit_, base_);
    applied_= std::max(applied_, base_);
    snap_   = snapshot;
    unsigned long long floor = base_ + 1;
    for (unsigned int i = 0; i < peer_count_; ++i) {
        if (next_[i]  < floor) next_[i]  = floor;
        if (acked_[i] < base_) acked_[i] = base_;
    }
    PersistWithSnapshot();
    return true;
}

// ─── RPC handlers ─────────────────────────────────────────────────────────────

::engine::raft::v1::RequestVoteResp
Raft::OnRequestVote(const ::engine::raft::v1::RequestVoteReq& args) {
    ::engine::raft::v1::RequestVoteResp resp;
    resp.set_term(term_); resp.set_vote(false);
    if (args.term() < term_) return resp;
    if (args.term() > term_) StepDown(args.term());

    unsigned int candidate = static_cast<unsigned int>(args.candidate());
    bool fresh = args.last_term() > LastTerm() ||
                 (args.last_term() == LastTerm() && args.last_index() >= LastIndex());
    bool can_vote = fresh && (!voted_.has_value() || *voted_ == candidate);
    if (can_vote) { voted_ = candidate; deadline_ = Clock::now() + RandElectionTimeout(); Persist(); }
    resp.set_term(term_); resp.set_vote(can_vote);
    return resp;
}

::engine::raft::v1::AppendEntriesResp
Raft::OnAppendEntries(const ::engine::raft::v1::AppendEntriesReq& args) {
    ::engine::raft::v1::AppendEntriesResp resp;
    resp.set_term(term_); resp.set_success(false);
    resp.set_conflict_term(0); resp.set_conflict_index(0);

    if (args.term() < term_) return resp;
    if (args.term() > term_) StepDown(args.term());
    else if (role_ != Role::Follower) { role_ = Role::Follower; granted_.clear(); }
    deadline_ = Clock::now() + RandElectionTimeout();
    resp.set_term(term_);

    unsigned long long pi = args.prev_index(), pt = args.prev_term();
    if (pi < base_) { resp.set_conflict_index(base_ + 1); return resp; }
    if (pi > LastIndex()) { resp.set_conflict_index(base_ + LogSize()); return resp; }
    if (log_[LogOffset(pi)].term() != pt) {
        unsigned long long ct = log_[LogOffset(pi)].term(), ci = pi;
        while (ci > base_ && log_[LogOffset(ci - 1)].term() == ct) --ci;
        resp.set_conflict_term(ct); resp.set_conflict_index(ci);
        return resp;
    }

    bool changed = false;
    unsigned long long idx = pi + 1;
    for (const auto& incoming : args.entries()) {
        if (idx <= LastIndex()) {
            auto& e = log_[LogOffset(idx)];
            if (e.term() != incoming.term() || e.data() != incoming.data()) {
                log_.resize(LogOffset(idx));
                log_.push_back(incoming);
                changed = true;
            }
        } else { log_.push_back(incoming); changed = true; }
        ++idx;
    }
    if (changed) Persist();

    if (args.leader_commit() > commit_) {
        commit_ = std::min(args.leader_commit(), LastIndex());
        ApplyReady();
    }
    resp.set_success(true);
    return resp;
}

::engine::raft::v1::InstallSnapshotResp
Raft::OnInstallSnapshot(const ::engine::raft::v1::InstallSnapshotReq& args) {
    ::engine::raft::v1::InstallSnapshotResp resp;
    resp.set_term(term_);
    if (args.term() < term_) return resp;
    StepDown(args.term());
    deadline_ = Clock::now() + RandElectionTimeout();
    if (args.last_index() > base_) {
        std::vector<std::byte> data(
            reinterpret_cast<const std::byte*>(args.data().data()),
            reinterpret_cast<const std::byte*>(args.data().data() + args.data().size()));
        on_commit_(ApplySnapshot{std::move(data), args.last_term(), args.last_index()});
    }
    resp.set_term(term_);
    return resp;
}

// ─── RPC reply handlers ───────────────────────────────────────────────────────

void Raft::OnVoteReply(unsigned long long election_term, unsigned int peer,
                       const ::engine::raft::v1::RequestVoteResp& reply) {
    if (reply.term() > term_) { StepDown(reply.term()); return; }
    if (role_ != Role::Candidate || term_ != election_term) return;
    if (reply.vote()) { granted_.insert(peer); if (HasQuorum()) StepUp(); }
}

void Raft::OnAppendReply(unsigned long long sent_term, unsigned int peer,
                         unsigned long long prev_index, unsigned int sent_num,
                         const ::engine::raft::v1::AppendEntriesResp& reply) {
    if (reply.term() > term_) { StepDown(reply.term()); return; }
    if (role_ != Role::Leader || term_ != sent_term) return;

    if (reply.success()) {
        unsigned long long matched = prev_index + static_cast<unsigned long long>(sent_num);
        if (matched > acked_[peer]) acked_[peer] = matched;
        next_[peer] = acked_[peer] + 1;
        AdvanceCommit();
        if (next_[peer] <= LastIndex())
            heartbeat_ = Clock::now() - kHeartbeatInterval;
        return;
    }

    unsigned long long next;
    if (reply.conflict_term() == 0) {
        next = std::max<unsigned long long>(reply.conflict_index(), 1);
    } else {
        auto it = std::find_if(log_.rbegin(), log_.rend(),
            [&](const LogEntry& e){ return e.term() == reply.conflict_term(); });
        if (it != log_.rend()) {
            unsigned int off = static_cast<unsigned int>(std::distance(it, log_.rend()) - 1);
            next = base_ + static_cast<unsigned long long>(off) + 1;
        } else {
            next = std::max<unsigned long long>(reply.conflict_index(), 1);
        }
    }
    next_[peer] = std::min(next, base_ + LogSize());
    heartbeat_  = Clock::now() - kHeartbeatInterval;
}

void Raft::OnSnapshotReply(unsigned long long sent_term, unsigned int peer,
                           unsigned long long last_index,
                           const ::engine::raft::v1::InstallSnapshotResp& reply) {
    if (reply.term() > term_) { StepDown(reply.term()); return; }
    if (role_ != Role::Leader || term_ != sent_term) return;
    if (acked_[peer] < last_index) acked_[peer] = last_index;
    unsigned long long nxt = last_index + 1;
    if (next_[peer] < nxt) next_[peer] = nxt;
    if (next_[peer] <= LastIndex())
        heartbeat_ = Clock::now() - kHeartbeatInterval;
}

} // namespace engine::raft
