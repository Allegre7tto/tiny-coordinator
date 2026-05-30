#include "server/server.h"

#include <spdlog/spdlog.h>

namespace engine::server {

// ─── RaftLogServiceImpl ───────────────────────────────────────────────────────

RaftLogServiceImpl::RaftLogServiceImpl() : node_(nullptr) {}

void RaftLogServiceImpl::SetNode(raft::RaftNode* node) { node_ = node; }

void RaftLogServiceImpl::OnCommitted(unsigned long long index, unsigned long long term,
                                     std::vector<std::byte> data) {
    ::engine::log::v1::CommittedEntry entry;
    entry.set_index(index);
    entry.set_term(term);
    entry.set_data(
        reinterpret_cast<const char*>(data.data()),
        data.size());

    std::lock_guard lock(mu_);
    // If queue is full, drop oldest entries to prevent memory exhaustion
    while (committed_queue_.size() >= kMaxCommittedQueueSize) {
        committed_queue_.pop_front();
    }
    committed_queue_.push_back(std::move(entry));
    committed_cv_.notify_all();
}

void RaftLogServiceImpl::Shutdown() {
    std::lock_guard lock(mu_);
    shutdown_ = true;
    committed_cv_.notify_all();
}

grpc::Status RaftLogServiceImpl::Propose(
        grpc::ServerContext*,
        const ::engine::log::v1::ProposeReq* req,
        ::engine::log::v1::ProposeResp* resp) {
    if (!node_->IsLeader())
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "not leader");

    const auto& d = req->data();
    std::vector<std::byte> data(
        reinterpret_cast<const std::byte*>(d.data()),
        reinterpret_cast<const std::byte*>(d.data() + d.size()));
    unsigned long long index, term;
    auto s = node_->Start(data, index, term);
    if (!s.ok())
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, s.message());

    resp->set_index(index);
    resp->set_term(term);
    return grpc::Status::OK;
}

grpc::Status RaftLogServiceImpl::SubscribeCommitted(
        grpc::ServerContext* ctx,
        const ::engine::log::v1::SubscribeReq* req,
        grpc::ServerWriter<::engine::log::v1::CommittedEntry>* writer) {
    spdlog::info("RaftLog: Java subscribed to committed stream (start_index={})",
                 req->start_index());

    while (!ctx->IsCancelled()) {
        ::engine::log::v1::CommittedEntry entry;
        {
            std::unique_lock lock(mu_);
            committed_cv_.wait(lock, [this] {
                return !committed_queue_.empty() || shutdown_;
            });
            if (shutdown_ && committed_queue_.empty()) break;
            entry = std::move(committed_queue_.front());
            committed_queue_.pop_front();
        }
        if (!writer->Write(entry)) break;
    }

    spdlog::info("RaftLog: committed stream closed");
    return grpc::Status::OK;
}

grpc::Status RaftLogServiceImpl::SaveSnapshot(
        grpc::ServerContext*,
        grpc::ServerReader<::engine::log::v1::SnapshotChunk>* reader,
        ::engine::log::v1::SaveSnapshotResp* resp) {
    std::vector<std::byte> snapshot_data;
    unsigned long long last_index = 0, last_term = 0;

    ::engine::log::v1::SnapshotChunk chunk;
    while (reader->Read(&chunk)) {
        last_index = chunk.last_index();
        last_term  = chunk.last_term();
        const auto& d = chunk.data();
        snapshot_data.insert(
            snapshot_data.end(),
            reinterpret_cast<const std::byte*>(d.data()),
            reinterpret_cast<const std::byte*>(d.data() + d.size()));
        if (chunk.done()) break;
    }

    node_->TakeSnapshot(last_index, snapshot_data);
    spdlog::info("RaftLog: saved snapshot at index={} term={} size={}",
                 last_index, last_term, snapshot_data.size());

    resp->set_ok(true);
    return grpc::Status::OK;
}

grpc::Status RaftLogServiceImpl::LoadSnapshot(
        grpc::ServerContext*,
        const ::engine::log::v1::LoadSnapshotReq*,
        grpc::ServerWriter<::engine::log::v1::SnapshotChunk>* writer) {
    spdlog::info("RaftLog: Java requested snapshot load");

    std::vector<std::byte> data;
    unsigned long long last_index = 0, last_term = 0;
    if (!node_->LoadSnapshot(data, last_index, last_term)) {
        ::engine::log::v1::SnapshotChunk empty_chunk;
        empty_chunk.set_done(true);
        writer->Write(empty_chunk);
        return grpc::Status::OK;
    }

    const size_t chunk_size = 64 * 1024; // 64KB chunks
    size_t offset = 0;
    while (offset < data.size()) {
        size_t end = std::min(offset + chunk_size, data.size());
        ::engine::log::v1::SnapshotChunk chunk;
        chunk.set_last_index(last_index);
        chunk.set_last_term(last_term);
        chunk.set_data(
            reinterpret_cast<const char*>(data.data() + offset),
            end - offset);
        chunk.set_done(end >= data.size());
        if (!writer->Write(chunk)) break;
        offset = end;
    }

    spdlog::info("RaftLog: snapshot load complete at index={} term={} size={}",
                 last_index, last_term, data.size());
    return grpc::Status::OK;
}

grpc::Status RaftLogServiceImpl::Status(
        grpc::ServerContext*,
        const ::engine::log::v1::StatusReq*,
        ::engine::log::v1::StatusResp* resp) {
    resp->set_is_leader(node_->IsLeader());
    resp->set_term(node_->Term());
    return grpc::Status::OK;
}

// ─── GrpcServer ───────────────────────────────────────────────────────────────

GrpcServer::GrpcServer(const std::string&    listen_addr,
                       raft::RaftNode*       raft_node,
                       RaftLogServiceImpl*   raft_log_svc)
    : listen_addr_(listen_addr), raft_node_(raft_node), raft_log_svc_(raft_log_svc) {}

void GrpcServer::Start() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_addr_, grpc::InsecureServerCredentials());
    builder.RegisterService(raft_node_);      // RaftInternal::Service (peer-to-peer)
    builder.RegisterService(raft_log_svc_);   // RaftLog::Service (Java ↔ C++)
    server_ = builder.BuildAndStart();
    spdlog::info("gRPC server listening on {}", listen_addr_);
    server_->Wait();
}

void GrpcServer::Shutdown() {
    if (server_) server_->Shutdown();
}

} // namespace engine::server
