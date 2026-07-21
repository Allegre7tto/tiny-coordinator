# C++ → Rust 迁移设计

## 概述

将 `core/` 目录的 C++ Raft 引擎全部替换为 Rust 实现。Rust 编译为 `.so`（动态库），Java coordinator 通过 JNI 直接进程内调用，替代当前 `log.proto` gRPC 进程间通信。

## 动机

1. **消除进程间通信**：当前每个 propose 需要序列化→gRPC→反序列化，延迟高。JNI + DirectByteBuffer 实现零拷贝传递
2. **简化部署**：3 个 C++ 容器 + 1 个 Java 容器 → 3 个 Java 容器（每个内嵌 Rust Raft 节点）
3. **消除 `log.proto`**：5 个 gRPC 接口变成 5 个 JNI 函数
4. **统一线程模型**：C++ 的 mutex + ticker 线程 + async CQ + gRPC handler → Rust 单个 tokio actor

## 系统架构

```
┌──────────────────────────────────────────────────┐
│                  Java Coordinator                 │
│  ┌─────────────────────────────────────────────┐ │
│  │  StateMachineDriver                         │ │
│  │  KvStore / WatchManager / LeaseManager      │ │
│  └──────────────┬──────────────────────────────┘ │
│                 │ JNI (RustRaftLib)               │
│  ┌──────────────▼──────────────────────────────┐ │
│  │  libraft.so                                 │ │
│  │  ┌────────┐  ┌────────┐  ┌──────────────┐  │ │
│  │  │raftCore│  │raftWal │  │raftPeer (gRPC)│  │ │
│  │  └───┬────┘  └───┬────┘  └──────┬───────┘  │ │
│  │      └───────────┼──────────────┘           │ │
│  │           ┌──────▼──────┐                   │ │
│  │           │  raftNode   │ (tokio actor)     │ │
│  │           └─────────────┘                   │ │
│  └─────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────┘
         │                    │
    gRPC (raft.proto)    gRPC (coordinator.proto)
    node ↔ node          外部客户端
```

## Crate 拆分

### 1. `raft-proto` — Proto 类型

用 `prost` 从 `raft.proto` 生成 Rust struct（`LogEntry`、`RequestVoteReq`、`AppendEntriesReq` 等），被 `raft-core` 和 `raft-peer` 共同依赖。

### 2. `raft-core` — 纯 Raft 状态机

无 I/O，无线程。所有方法在外部锁或 actor 保护下调用。依赖 `raft-proto` 获取 proto 类型。

**类型：**
- `Role` enum: `Follower`, `Candidate`, `Leader`
- `RaftCore` struct: 保存 term、vote、log、commit 等全部 Raft 状态
- `ApplyMsg` enum: `Command(Vec<u8>, u64, u64)` / `Snapshot(Vec<u8>, u64, u64)`
- `RaftTask` enum: `Vote(peer, RequestVoteReq, electionTerm)` / `Append(peer, AppendEntriesReq, sentTerm, prevIndex, sentNum)` / `Snapshot(peer, InstallSnapshotReq, sentTerm, lastIndex)`

**公开方法（与 C++ `raft.h` 一致）：**
- `tick() -> Vec<RaftTask>` — leader 发心跳 / follower 超时选举
- `propose(data, &mut index, &mut term) -> Status` — leader 追加 log entry
- `onRequestVote(req) -> RequestVoteResp`
- `onAppendEntries(req) -> AppendEntriesResp`
- `onInstallSnapshot(req) -> InstallSnapshotResp`
- `onVoteReply(electionTerm, peer, reply)`
- `onAppendReply(sentTerm, peer, prevIndex, sentNum, reply)`
- `onSnapshotReply(sentTerm, peer, lastIndex, reply)`
- `takeSnapshot(index, snapshot)`
- `condInstallSnapshot(lastTerm, lastIndex, snapshot) -> bool`

**内部：**
- `stepdown(term)`, `stepup()`, `beginelection()`, `hasquorum()`, `advancecommit()`, `applyready()`
- `persist()`, `persistWithSnapshot()`, `restore(hs)`
- `buildAppendTask(peer)`, `buildSnapshotTask(peer)`

### 3. `raft-wal` — WAL + Snapshot 持久化

**文件格式（与 C++ 一致）：**
```
[4B length LE] [4B CRC-32] [N bytes protobuf payload]
```

**文件：**
- `raftState.bin` — protobuf `PersistState`（term、votedFor、base、anchor、log）
- `snapshot.bin` — protobuf `InstallSnapshotReq`

**原子写入**：先写 `.tmp`，再 `fs::rename`。

**接口：**
- `FileStorage::new(dataDir) -> Self`
- `saveState(&self, hs) -> Result<()>`
- `loadState(&self) -> Option<HardState>`
- `saveSnapshot(&self, hs, snap) -> Result<()>`
- `loadSnapshot(&self) -> Option<SnapshotData>`

### 4. `raft-peer` — 节点间 gRPC

用 `tonic` 实现 `raft.proto` 的 `RaftInternal` 服务，同时提供异步客户端方法给 `raftNode` 使用。

**服务端（接收 peer RPC）：**
- `RequestVote`, `AppendEntries`, `InstallSnapshot` — 通过 `tokio::mpsc::Sender` 发给 actor

**客户端（发送 peer RPC）：**
- `sendVote(addr, req)`, `sendAppend(addr, req)`, `sendSnapshot(addr, req)` — 返回 `JoinHandle`-like future

### 5. `raft-node` — RaftNode actor

**核心事件循环（`tokio::select!`）：**
```
loop {
    select! {
        _ = ticker.tick() => { let tasks = raft.tick(); dispatch(tasks); }
        Some(req) = proposeRx.recv() => { raft.propose(req.data, &mut req.index, &mut req.term); }
        Some(rpc) = peerRx.recv() => { handleIncomingRpc(rpc); }
        Some(done) = rpcDoneRx.recv() => { handleRpcReply(done); }
    }
    raft.applyready(&mut committed); // push to committed queue
    notifyJava(committed);           // JNI callback
}
```

无锁——所有 Raft 状态由 actor 独占。

**Cargo 依赖：**
- `tokio`（actor + 计时器 + 异步）
- `tonic` + `prost`（gRPC 节点间通信）
- `bytes`（零拷贝字节缓冲）
- `tracing`（结构化日志）

### 6. `raft-jni` — JNI 桥接层

用 `jni` crate 暴露 5 个 native 方法给 Java：

```
Java_engine_client_RustRaftLib_propose(env, obj, bytebuf) -> jlong
Java_engine_client_RustRaftLib_status(env, obj) -> jobject
Java_engine_client_RustRaftLib_start(env, obj, config)
Java_engine_client_RustRaftLib_saveSnapshot(env, obj, index, term, bytebuf)
Java_engine_client_RustRaftLib_loadSnapshot(env, obj) -> jobject
```

**Committed 回调**：
1. Java 端：`RustRaftLib` 提供 `setCommittedCallback(CommittedCallback cb)` 注册回调，`CommittedCallback` 是单方法接口 `void onEntry(long index, long term, ByteBuffer data)`
2. Rust 端：`raft-jni` 持有一个 `JniCallback` 对象（`GlobalRef` 包装 Java 回调对象），actor 循环中 `applyready()` 后立即通过 JNI 调用 `CommittedCallback.onEntry()`
3. `data` 通过 `DirectByteBuffer` 传递，零拷贝。`StateMachineDriver` 不再需要 `SubscribeCommitted` gRPC 流

**初始化**：`start()` 接收 `RaftConfig`（id、peerAddrs、dataDir），启动 tokio runtime + RaftNode actor。

**数据传递**：`DirectByteBuffer` 零拷贝——Rust 直接读取 Java 堆外内存指针，无需序列化。

## Proto 变更

| 文件 | 操作 | 说明 |
|------|------|------|
| `raft.proto` | **保留** | Rust 节点间 peer gRPC |
| `log.proto` | **删除** | JNI 替代 |
| `coordinator.proto` | **不变** | Java 对外服务 |

## Java 侧改动

### 删除
- `engine/client/RaftLogClient.java` — gRPC 客户端
- `application.properties` 中的 `ENGINE_CPP_HOST` / `ENGINE_CPP_PORT`

### 新增
- `engine/client/RustRaftLib.java` — JNI 包装类
  - `native long propose(ByteBuffer data)`
  - `native StatusResult status()`
  - `native void start(RaftConfig config)`
  - `native boolean saveSnapshot(long index, long term, ByteBuffer data)`
  - `native ByteBuffer loadSnapshot()`
  - Java 回调接口 `CommittedCallback`

### 修改
- `StateMachineDriver.java` — `RaftLogClient` → `RustRaftLib`
- `application.properties` — 新增 Rust 相关配置（`ENGINE_RAFT_DATADIR`、`ENGINE_RAFT_PEERS` 等）

## 命名约定

- **函数/变量**：优先使用短连词，少用下划线。单短词直接用（`term`、`base`、`peer`），必须组合时用连续小写（`peercount`、`lastindex`、`nextindex`、`votedfor`、`leaderid`）
- **类型/Struct**：PascalCase（`RaftCore`、`FileStorage`、`RaftNode`）
- **Enum 成员**：PascalCase（`Follower`、`Candidate`、`Leader`）
- **常量**：同 PascalCase
- **crate 名**：短横线连接（`raft-core`、`raft-wal`）

## 构建系统

### Rust 侧（`core/` 改为 `raft/`）
```
raft/
  Cargo.toml (workspace)
  raft-proto/Cargo.toml
  raft-core/Cargo.toml
  raft-wal/Cargo.toml
  raft-peer/Cargo.toml
  raft-node/Cargo.toml
  raft-jni/Cargo.toml
  tests/
```

`raft-jni` 用 `cargo` + `jni` crate 编译为 `.so`（macOS `.dylib`，Linux `.so`）。

### Java 侧
- `pom.xml` 新增 `java.library.path` 指向 Rust 构建输出目录
- 删除 `protobuf-maven-plugin` 对 `log.proto` 的编译

### 总体
- `Makefile` 的 `build-cpp` → `build-rust`（`cargo build --release`）
- Docker：Java 镜像中加入 `.so` 文件

## 测试

### Rust 侧
- `raft-core` 单元测试：选举、log 复制、snapshot、冲突回退（复刻 C++ 6 个测试）
- `raft-wal` 测试：CRC 校验、原子写入、崩溃恢复
- `raft-node` 集成测试：2-3 节点集群的端到端测试

### Java 侧
- `StateMachineDriver` 集成测试：用 Mock RustRaftLib 验证 propose → commit 流程
- 现有 MVCC 测试保持不变

## 迁移顺序

1. `raft-proto` — proto 编译
2. `raft-core` — 纯算法，无外部依赖
3. `raft-wal` — 文件持久化
4. `raft-peer` — gRPC 通信
5. `raft-node` — actor 组装
6. `raft-jni` — JNI 桥接
7. Java 侧 `RustRaftLib` + `StateMachineDriver` 改造
8. 删除 C++ `core/`、`log.proto`
9. Docker / 构建系统更新

## 删除清单

- `core/` 整个目录
- `proto/log.proto`
- `Dockerfile.cpp`
- `core/CMakeLists.txt`、`core/conanfile.txt`、`core/CMakePresets.json`
- `coordinator/src/main/java/engine/client/RaftLogClient.java`
- `Makefile` 中 `build-cpp` / `test-cpp` 目标
- `docker-compose.yml` 中 3 个 `engine-cpp-*` 服务
