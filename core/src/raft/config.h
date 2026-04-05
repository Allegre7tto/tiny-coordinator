#pragma once

#include "common/types.h"

#include <chrono>
#include <string>
#include <vector>

namespace engine::raft {

// ─── Timing constants ─────────────────────────────────────────────────────────
inline constexpr auto kTickInterval       = std::chrono::milliseconds(20);
inline constexpr auto kHeartbeatInterval  = std::chrono::milliseconds(120);
inline constexpr auto kElectionTimeoutMin = std::chrono::milliseconds(350);
inline constexpr auto kElectionTimeoutMax = std::chrono::milliseconds(700);

// ─── Node configuration ───────────────────────────────────────────────────────
struct RaftConfig {
    uint64                   id;          // 0-based index into peer_addrs
    std::vector<std::string> peer_addrs;  // gRPC addresses for ALL nodes (including self)
    std::string              data_dir;    // directory for WAL + snapshot files
};

} // namespace engine::raft
