#pragma once

#include <cstddef>
#include <cstdint>

// ─── Integer type aliases ─────────────────────────────────────────────────────
// Short names in the style of Rust; use these throughout C++ code.

using int8  = std::int8_t;
using int16 = std::int16_t;
using int32 = std::int32_t;
using int64 = std::int64_t;

using uint8  = std::uint8_t;
using uint16 = std::uint16_t;
using uint32 = std::uint32_t;
using uint64 = std::uint64_t;

using size = std::size_t;       // replaces size_t
using byte = std::uint8_t;      // replaces uint8_t / unsigned char in buffer contexts
