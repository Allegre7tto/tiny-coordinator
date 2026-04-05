#pragma once

#include "common/types.h"

#include <vector>

namespace engine::raft::v1 { class LogEntry; }

namespace engine::raft {

// ─── In-memory log ────────────────────────────────────────────────────────────
//
// 1-indexed.  log_[0] is a sentinel whose term equals `anchor`.
// Absolute index of log_[i] = base + i.
//
using LogEntry = ::engine::raft::v1::LogEntry;

struct LogState {
    uint64               base   = 0;  // absolute index of log_[0] (snapshot boundary)
    uint64               anchor = 0;  // term at index base
    std::vector<LogEntry> entries;
};

} // namespace engine::raft
