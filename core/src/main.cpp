#include "raft/config.h"
#include "raft/storage.h"
#include "raft/node.h"
#include "server/server.h"

#include <spdlog/spdlog.h>

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

static engine::server::GrpcServer*        g_server   = nullptr;
static engine::server::RaftLogServiceImpl* g_raft_log = nullptr;
static engine::raft::RaftNode*             g_raft_node = nullptr;

static void OnSignal(int) {
    spdlog::info("shutting down...");
    // Shutdown gRPC server first to stop accepting new RPCs
    if (g_server) g_server->Shutdown();
    // Shutdown RaftLogService to unblock committed stream writers
    if (g_raft_log) g_raft_log->Shutdown();
    // RaftNode will be destructed when main returns (stack unwinding)
}

//   engine <node_id> <data_dir> <peer0_addr> [<peer1_addr> ...]
int main(int argc, char* argv[]) {
    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("[%H:%M:%S.%e] [%^%l%$] [engine] %v");

    if (argc < 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <node_id> <data_dir> <peer0> [peer1 ...]\n";
        return 1;
    }

    unsigned long long node_id = std::stoull(argv[1]);
    std::string data_dir = argv[2];

    std::vector<std::string> peer_addrs;
    for (int i = 3; i < argc; ++i) peer_addrs.emplace_back(argv[i]);

    if (node_id >= peer_addrs.size()) {
        std::cerr << "node_id " << node_id
                  << " out of range (" << peer_addrs.size() << " peers)\n";
        return 1;
    }

    std::string listen_addr = peer_addrs[node_id];
    spdlog::info("node {} starting on {} ({} peers)",
                 node_id, listen_addr, peer_addrs.size());

    // ── RaftLogService (created first; node set after RaftNode construction) ──
    auto raft_log_svc = std::make_unique<engine::server::RaftLogServiceImpl>();
    auto* raft_log_ptr = raft_log_svc.get();

    // ── Raft config + storage ─────────────────────────────────────────────────
    engine::raft::RaftConfig cfg;
    cfg.id         = node_id;
    cfg.peer_addrs = peer_addrs;
    cfg.data_dir   = data_dir;

    auto storage = std::make_unique<engine::raft::FileStorage>(data_dir);

    // ── Apply callback: committed entries → RaftLogService → Java ─────────────
    auto on_commit = [raft_log_ptr](engine::raft::ApplyMsg msg) {
        std::visit([raft_log_ptr](const auto& m) {
            using T = std::decay_t<decltype(m)>;
            if constexpr (std::is_same_v<T, engine::raft::ApplyCommand>) {
                raft_log_ptr->OnCommitted(m.index, m.term, m.data);
            }
        }, msg);
    };

    // ── RaftNode ──────────────────────────────────────────────────────────────
    auto raft_node = std::make_unique<engine::raft::RaftNode>(
        cfg, std::move(storage), std::move(on_commit));

    raft_log_svc->SetNode(raft_node.get());

    // ── gRPC server ───────────────────────────────────────────────────────────
    engine::server::GrpcServer grpc_server(
        listen_addr, raft_node.get(), raft_log_svc.get());
    g_server    = &grpc_server;
    g_raft_log  = raft_log_svc.get();
    g_raft_node = raft_node.get();

    std::signal(SIGINT,  OnSignal);
    std::signal(SIGTERM, OnSignal);

    grpc_server.Start();
    return 0;
}
