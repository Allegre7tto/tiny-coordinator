#include "raft/storage.h"
#include "common/encoding.h"

#include "raft.pb.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>

namespace engine::raft {

namespace fs = std::filesystem;

// ─── Proto ↔ HardState conversion ────────────────────────────────────────────

static HardState ProtoToHardState(const ::engine::raft::v1::PersistState& p) {
    HardState hs;
    hs.term   = p.term();
    hs.voted  = p.voted();
    hs.base   = p.base();
    hs.anchor = p.anchor();
    for (const auto& e : p.log()) hs.log.push_back(e);
    return hs;
}

static ::engine::raft::v1::PersistState HardStateToProto(const HardState& hs) {
    ::engine::raft::v1::PersistState p;
    p.set_term(hs.term);
    p.set_voted(hs.voted);
    p.set_base(hs.base);
    p.set_anchor(hs.anchor);
    for (const auto& e : hs.log) *p.add_log() = e;
    return p;
}

// ─── FileStorage ──────────────────────────────────────────────────────────────

FileStorage::FileStorage(std::string data_dir) : data_dir_(std::move(data_dir)) {
    fs::create_directories(data_dir_);
}

Status FileStorage::WriteFile(const std::string& path, const std::string& payload) {
    std::string tmp = path + ".tmp";
    std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
    if (!f) return Status::IOError("open " + tmp + ": " + std::strerror(errno));

    byte header[8];
    uint32 len = static_cast<uint32>(payload.size());
    uint32 crc = Crc32(payload);
    EncodeUint32(header,     len);
    EncodeUint32(header + 4, crc);

    f.write(reinterpret_cast<const char*>(header), 8);
    f.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    f.flush();
    if (!f) return Status::IOError("write " + tmp + ": " + std::strerror(errno));
    f.close();

    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (ec) return Status::IOError("rename to " + path + ": " + ec.message());
    return Status::OK();
}

bool FileStorage::ReadFile(const std::string& path, std::string& payload) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return false;

    byte header[8];
    f.read(reinterpret_cast<char*>(header), 8);
    if (f.gcount() != 8) return false;

    uint32 len = DecodeUint32(header);
    uint32 crc = DecodeUint32(header + 4);
    payload.resize(len);
    f.read(payload.data(), len);
    if (static_cast<uint32>(f.gcount()) != len) return false;
    return Crc32(payload) == crc;
}

Status FileStorage::SaveState(const HardState& state) {
    std::string payload;
    if (!HardStateToProto(state).SerializeToString(&payload))
        return Status::Internal("serialize HardState failed");
    return WriteFile(data_dir_ + "/raft_state.bin", payload);
}

bool FileStorage::LoadState(HardState& out) {
    std::string payload;
    if (!ReadFile(data_dir_ + "/raft_state.bin", payload)) return false;
    ::engine::raft::v1::PersistState proto;
    if (!proto.ParseFromString(payload)) return false;
    out = ProtoToHardState(proto);
    return true;
}

Status FileStorage::SaveSnapshot(const HardState& state, const SnapshotData& snap) {
    if (auto s = SaveState(state); !s.ok()) return s;

    ::engine::raft::v1::InstallSnapshotReq proto;
    proto.set_last_index(snap.last_index);
    proto.set_last_term(snap.last_term);
    proto.set_data(snap.data.data(), snap.data.size());

    std::string payload;
    if (!proto.SerializeToString(&payload))
        return Status::Internal("serialize SnapshotData failed");
    return WriteFile(data_dir_ + "/snapshot.bin", payload);
}

bool FileStorage::LoadSnapshot(SnapshotData& out) {
    std::string payload;
    if (!ReadFile(data_dir_ + "/snapshot.bin", payload)) return false;
    ::engine::raft::v1::InstallSnapshotReq proto;
    if (!proto.ParseFromString(payload)) return false;
    out.last_index = proto.last_index();
    out.last_term  = proto.last_term();
    const auto& d  = proto.data();
    out.data.assign(d.begin(), d.end());
    return true;
}

} // namespace engine::raft
