#pragma once

#include "common/types.h"

#include <cstring>
#include <string>
#include <vector>

namespace engine {

// Little-endian integer serialization helpers used by the WAL layer.

inline void EncodeUint32(byte* buf, uint32 v) {
    buf[0] = static_cast<byte>(v);
    buf[1] = static_cast<byte>(v >> 8);
    buf[2] = static_cast<byte>(v >> 16);
    buf[3] = static_cast<byte>(v >> 24);
}

inline uint32 DecodeUint32(const byte* buf) {
    return static_cast<uint32>(buf[0])
         | static_cast<uint32>(buf[1]) << 8
         | static_cast<uint32>(buf[2]) << 16
         | static_cast<uint32>(buf[3]) << 24;
}

inline void EncodeUint64(byte* buf, uint64 v) {
    for (int i = 0; i < 8; ++i)
        buf[i] = static_cast<byte>(v >> (i * 8));
}

inline uint64 DecodeUint64(const byte* buf) {
    uint64 v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64>(buf[i]) << (i * 8);
    return v;
}

// Naive CRC-32 (polynomial 0xEDB88320) — good enough for data integrity checks.
inline uint32 Crc32(const byte* data, size len) {
    uint32 crc = 0xFFFFFFFFu;
    for (size i = 0; i < len; ++i) {
        crc ^= data[i];
        for (int j = 0; j < 8; ++j)
            crc = (crc >> 1) ^ (0xEDB88320u & -(crc & 1u));
    }
    return ~crc;
}

inline uint32 Crc32(const std::string& s) {
    return Crc32(reinterpret_cast<const byte*>(s.data()), s.size());
}

} // namespace engine
