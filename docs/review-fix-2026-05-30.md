# Code Review 修复记录

> 基于 2026-05-29 代码审查的修复，2026-05-30 执行

---

## 修复总览

| # | 问题 | 优先级 | 状态 | 修复文件 |
|---|------|--------|------|----------|
| 1 | detach()线程不安全 | 高 | 已跳过 | - |
| 2 | LoadSnapshot空操作 | 高 | 已修复 | node.h, node.cpp, server.cpp |
| 3 | committed stream无重连 | 高 | 已修复 | StateMachineDriver.java |
| 4 | ApplyCommand丢失term | 高 | 已修复 | raft.h, raft.cpp, main.cpp |
| 5 | propose()阻塞调用 | 中 | 已修复 | RaftLogClient.java, StateMachineDriver.java |
| 6 | subscribeCommitted无背压 | 中 | 已修复 | StateMachineDriver.java |
| 7 | RaftNode析构顺序隐患 | 中 | 已修复 | node.cpp, main.cpp |
| 8 | LeaseManager全员执行 | 中 | 已修复 | LeaseManager.java |
| 9 | WatchManager泄漏 | 中 | 已修复 | WatchManager.java |
| 10 | committed queue无上限 | 中 | 已修复 | server.h, server.cpp |
| 11 | 测试覆盖率不足 | 低 | 暂缓 | - |
| 12 | 没有TLS | 低 | 不修复 | - |
| 13 | C++构建含coordinator.proto | 低 | 已修复 | CMakeLists.txt |
| 14 | 配置无法外部化 | 低 | 已回滚 | - |

---

## 详细修复记录

### 1. detach()线程不安全 — 已跳过

**审查描述**：每次出站 RPC 都 `new std::thread(...).detach()`，无法安全关闭、无法取消、线程爆炸。

**实际情况**：经检查，代码已使用 `grpc::CompletionQueue` + async RPC 实现，不存在 `detach()` 调用。此问题在当前代码中不存在。

---

### 2. LoadSnapshot 是空操作 — 已修复

**审查描述**：C++ 端的 `LoadSnapshot` 直接返回空 `done` chunk，没有读取磁盘上的 snapshot 返回给 Java。

**修复方案**：
1. 在 `RaftNode` 中添加 `LoadSnapshot` 方法，调用 `Storage::LoadSnapshot` 读取磁盘
2. 修改 `RaftLogServiceImpl::LoadSnapshot`，实现分块传输（64KB/chunk）

**修改文件**：
- `core/src/raft/node.h`: 添加 `LoadSnapshot` 方法声明
- `core/src/raft/node.cpp`: 实现 `LoadSnapshot` 方法
- `core/src/server/server.cpp`: 重写 `LoadSnapshot` 实现

---

### 3. Java 端 committed stream 没有重连逻辑 — 已修复

**审查描述**：错误回调只打了日志，没有重连。如果 C++ 节点重启，Java 端永久停止处理 committed 条目。

**修复方案**：
1. 抽取 `subscribeCommitted` 方法，支持递归重连
2. 使用 Vert.x timer 实现延迟重连
3. 使用 Mutiny 的 `onFailure().retry().withBackOff()` 实现指数退避

**修改文件**：
- `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java`

---

### 4. ApplyCommand 丢失了 term 信息 — 已修复

**审查描述**：`ApplyCommand` 没有 term 字段，`main.cpp` 硬编码传 `0`，Java 端永远拿不到条目的正确 term。

**修复方案**：
1. 给 `ApplyCommand` 添加 `term` 字段
2. 在 `Raft::ApplyReady` 中传递 `e.term()`
3. 在 `main.cpp` 的 `on_commit` 回调中使用 `m.term`

**修改文件**：
- `core/src/raft/raft.h`: `ApplyCommand` 添加 `term` 字段
- `core/src/raft/raft.cpp`: `ApplyReady` 传递 term
- `core/src/main.cpp`: `on_commit` 回调使用正确 term

---

### 5. gRPC 线程模型问题 — propose() 阻塞调用 — 已修复

**审查描述**：`RaftLogClient.propose()` 使用 `blockingStub`，如果被 event-loop 线程调用会阻塞 Quarkus 的 event loop。

**修复方案**：
1. 改用 `asyncStub` + `StreamObserver`
2. 返回 `CompletableFuture<ProposeResp>`
3. `StateMachineDriver.propose()` 使用 `thenCompose` 链式调用

**修改文件**：
- `coordinator/src/main/java/engine/client/RaftLogClient.java`
- `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java`

---

### 6. subscribeCommitted 没有背压 — 已修复

**审查描述**：流式回调直接在 main apply 线程上执行所有操作，如果 replay 大量历史日志，可能 OOM 或阻塞 Mutiny 的事件线程。

**修复方案**：
1. 使用 `emitOn(Infrastructure.getDefaultExecutor())` 切换到专用线程池
2. 添加 `onFailure().retry().withBackOff(1000, 10000).atMost(3)` 重试机制

**修改文件**：
- `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java`

---

### 7. RaftNode 析构顺序有隐患 — 已修复

**审查描述**：`RaftNode::~RaftNode()` 只停了 ticker 线程，但 `GrpcServer::Shutdown()` 在 main 的 signal handler 中调用，如果 gRPC server 在 `RaftNode` 之后析构，in-flight 的 RPC handler 可能访问已释放的 `RaftNode`。

**修复方案**：
1. 在 `main.cpp` 中添加全局指针 `g_raft_node`
2. 修改 `OnSignal` 的关闭顺序：先关闭 gRPC server，再关闭 RaftLogService
3. 添加注释说明析构顺序的重要性

**修改文件**：
- `core/src/main.cpp`
- `core/src/raft/node.cpp`（添加注释）

---

### 8. LeaseManager.checkExpiry() 在所有节点上运行 — 已修复

**审查描述**：`@Scheduled(every = "1s")` 在三个 coordinator 实例上都跑，非 leader 节点也会尝试 propose lease revocation，浪费资源且产生无用日志。

**修复方案**：
1. 注入 `RaftLogClient`
2. 在 `checkExpiry()` 开头检查 `raftClient.status().getIsLeader()`
3. 非 leader 节点直接返回

**修改文件**：
- `coordinator/src/main/java/engine/coordinator/LeaseManager.java`

---

### 9. WatchManager 的 watcher 会泄漏 — 已修复

**审查描述**：`ConcurrentHashMap<Long, WatchEntry>` 没有清理机制。如果 client 注册 watch 后断开连接但不发送 `CancelRequest`，entry 永远留在 map 中。

**修复方案**：
1. 给 `WatchEntry` 添加 `createdAt` 时间戳
2. 使用 `ScheduledExecutorService` 每 60 秒执行一次清理
3. TTL 设为 5 分钟，过期自动移除

**修改文件**：
- `coordinator/src/main/java/engine/coordinator/WatchManager.java`

---

### 10. RaftLogServiceImpl 的 committed queue 没有上限 — 已修复

**审查描述**：`std::deque<CommittedEntry>` 无限增长。如果 Java 消费慢或断连，C++ 内存会持续增长。

**修复方案**：
1. 添加常量 `kMaxCommittedQueueSize = 10000`
2. 在 `OnCommitted` 中检查队列大小，超出时丢弃旧条目

**修改文件**：
- `core/src/server/server.h`
- `core/src/server/server.cpp`

---

### 13. C++ 构建中包含了 coordinator.proto — 已修复

**审查描述**：`CMakeLists.txt` 对三个 proto 都生成代码，但 `coordinator.proto` 只在 Java 端使用，增加不必要的编译时间。

**修复方案**：从 `PROTO_FILES` 列表中移除 `coordinator.proto`。

**修改文件**：
- `core/CMakeLists.txt`

---

## 跳过/取消的问题

| # | 原因 |
|---|------|
| 1 | 代码中不存在 detach() 调用 |
| 11 | 测试暂缓 |
| 12 | TLS 实现复杂，暂不处理 |
| 14 | 配置外部化回滚，保持简单 |

---

## 修改文件汇总

**C++ 文件**：
- `core/src/raft/raft.h` (ApplyCommand.term)
- `core/src/raft/raft.cpp` (ApplyReady term)
- `core/src/raft/node.h` (LoadSnapshot)
- `core/src/raft/node.cpp` (LoadSnapshot, 析构注释)
- `core/src/server/server.h` (队列上限)
- `core/src/server/server.cpp` (LoadSnapshot, 队列上限)
- `core/src/main.cpp` (on_commit term, 关闭顺序)
- `core/CMakeLists.txt` (移除 coordinator.proto)

**Java 文件**：
- `coordinator/src/main/java/engine/client/RaftLogClient.java` (异步 propose)
- `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java` (重连, 背压, 异步 propose)
- `coordinator/src/main/java/engine/coordinator/LeaseManager.java` (leader 检查)
- `coordinator/src/main/java/engine/coordinator/WatchManager.java` (TTL 清理)
