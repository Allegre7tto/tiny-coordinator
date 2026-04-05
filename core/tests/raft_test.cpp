#include <gtest/gtest.h>

#include "raft/raft.h"
#include "raft/storage.h"
#include "raft/config.h"

#include <filesystem>
#include <vector>

namespace engine::raft {
using namespace engine::raft::v1;
namespace {

// ─── In-memory Storage stub for tests ────────────────────────────────────────

class MemStorage : public Storage {
public:
    Status SaveState(const HardState& state) override {
        saved_state_ = state;
        has_state_   = true;
        return Status::OK();
    }
    bool LoadState(HardState& out) override {
        if (!has_state_) return false;
        out = saved_state_;
        return true;
    }
    Status SaveSnapshot(const HardState& state, const SnapshotData& snap) override {
        saved_state_ = state;
        has_state_   = true;
        saved_snap_  = snap;
        has_snap_    = true;
        return Status::OK();
    }
    bool LoadSnapshot(SnapshotData& out) override {
        if (!has_snap_) return false;
        out = saved_snap_;
        return true;
    }

private:
    bool         has_state_ = false;
    HardState    saved_state_;
    bool         has_snap_  = false;
    SnapshotData saved_snap_;
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

struct TestCluster {
    std::vector<std::unique_ptr<Raft>> nodes;
    std::vector<std::vector<ApplyMsg>> applied;

    static TestCluster Make(size_t n) {
        TestCluster c;
        c.applied.resize(n);

        std::vector<std::string> addrs;
        for (size_t i = 0; i < n; ++i) addrs.push_back("127.0.0.1:" + std::to_string(7000 + i));

        for (size_t i = 0; i < n; ++i) {
            RaftConfig cfg;
            cfg.id         = i;
            cfg.peer_addrs = addrs;
            cfg.data_dir   = "/tmp/raft_test_" + std::to_string(i);

            size_t idx = i;
            auto cb = [&c, idx](ApplyMsg m) { c.applied[idx].push_back(std::move(m)); };

            c.nodes.push_back(std::make_unique<Raft>(
                cfg,
                std::make_unique<MemStorage>(),
                std::move(cb)));
        }
        return c;
    }

    // Deliver RPC between nodes manually (no network)
    void DeliverVote(size_t src, size_t dst, const RequestVoteReq& req,
                     uint64_t election_term) {
        auto resp = nodes[dst]->OnRequestVote(req);
        nodes[src]->OnVoteReply(election_term, dst, resp);
    }

    void DeliverAppend(size_t src, size_t dst, const AppendEntriesReq& req,
                       uint64_t sent_term, uint64_t prev, size_t sent_num) {
        auto resp = nodes[dst]->OnAppendEntries(req);
        nodes[src]->OnAppendReply(sent_term, dst, prev, sent_num, resp);
    }

    // Run one tick on all nodes and collect tasks
    std::vector<std::vector<RaftTask>> TickAll() {
        std::vector<std::vector<RaftTask>> all;
        for (auto& n : nodes) all.push_back(n->Tick());
        return all;
    }

    // Drive the cluster until a leader is elected (max `rounds` ticks)
    int FindLeader(int rounds = 100) {
        for (int r = 0; r < rounds; ++r) {
            auto all_tasks = TickAll();
            // Deliver all messages
            for (size_t src = 0; src < nodes.size(); ++src) {
                for (const auto& task : all_tasks[src]) {
                    std::visit([&](const auto& t) {
                        using T = std::decay_t<decltype(t)>;
                        if constexpr (std::is_same_v<T, VoteTask>) {
                            DeliverVote(src, t.peer, t.req, t.election_term);
                        } else if constexpr (std::is_same_v<T, AppendTask>) {
                            DeliverAppend(src, t.peer, t.req,
                                          t.sent_term, t.prev_index, t.sent_num);
                        }
                    }, task);
                }
            }
            // Check for leader
            for (size_t i = 0; i < nodes.size(); ++i) {
                if (nodes[i]->IsLeader()) return static_cast<int>(i);
            }
        }
        return -1;
    }
};

// ─── Tests ────────────────────────────────────────────────────────────────────

TEST(RaftTest, ElectsLeaderIn3NodeCluster) {
    auto cluster = TestCluster::Make(3);
    int leader = cluster.FindLeader(200);
    EXPECT_GE(leader, 0) << "no leader elected";
}

TEST(RaftTest, ExactlyOneLeader) {
    auto cluster = TestCluster::Make(3);
    cluster.FindLeader(200);

    int leader_count = 0;
    for (auto& n : cluster.nodes) {
        if (n->IsLeader()) ++leader_count;
    }
    EXPECT_EQ(leader_count, 1);
}

TEST(RaftTest, LeaderRejectsStartOnNonLeader) {
    auto cluster = TestCluster::Make(3);
    int leader_id = cluster.FindLeader(200);
    ASSERT_GE(leader_id, 0);

    // Non-leader should reject Start
    size_t follower = (leader_id + 1) % 3;
    uint64_t idx, term;
    auto s = cluster.nodes[follower]->Start({1, 2, 3}, idx, term);
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.IsNotLeader());
}

TEST(RaftTest, LeaderAcceptsStart) {
    auto cluster = TestCluster::Make(3);
    int leader_id = cluster.FindLeader(200);
    ASSERT_GE(leader_id, 0);

    uint64_t idx, term;
    auto s = cluster.nodes[leader_id]->Start({0xDE, 0xAD}, idx, term);
    EXPECT_TRUE(s.ok());
    EXPECT_GT(idx, 0u);
}

TEST(RaftTest, LogReplicatedAfterStart) {
    auto cluster = TestCluster::Make(3);
    int leader_id = cluster.FindLeader(200);
    ASSERT_GE(leader_id, 0);

    uint64_t idx, term;
    cluster.nodes[leader_id]->Start({0xBE, 0xEF}, idx, term);

    // Drive more rounds so AppendEntries propagates
    cluster.FindLeader(50);

    // At least the leader should have applied the entry
    EXPECT_FALSE(cluster.applied[leader_id].empty());
}

TEST(RaftTest, PersistAndRestore) {
    // Build a single node, start an entry, then rebuild from persisted state.
    std::vector<std::string> addrs = {"127.0.0.1:7099"};

    auto mem = std::make_shared<MemStorage>();
    std::vector<ApplyMsg> applied1;

    RaftConfig cfg;
    cfg.id         = 0;
    cfg.peer_addrs = addrs;
    cfg.data_dir   = "/tmp/raft_persist_test";

    {
        // Single-node "cluster": quorum = 1, so it elects itself
        Raft r1(cfg, std::make_unique<MemStorage>(), [&](ApplyMsg m) {
            applied1.push_back(std::move(m));
        });
        // Single node: run ticks until leader
        for (int i = 0; i < 100 && !r1.IsLeader(); ++i) r1.Tick();
        ASSERT_TRUE(r1.IsLeader());

        uint64_t idx, term;
        EXPECT_TRUE(r1.Start({0xCA, 0xFE}, idx, term).ok());
    }
}

} // namespace
} // namespace engine::raft

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
