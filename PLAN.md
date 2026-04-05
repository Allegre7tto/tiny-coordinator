# Engine — 分布式协调器开发计划

## Context

本目录已完成 C++ Raft 共识核心（leader election、log replication、persistence、snapshot）和 WAL 持久化层。在此基础上进行了两次架构演进：

1. **第一版**：纯 C++ 实现全部逻辑（Raft + KV + Watch + Lease + Client SDK）
2. **第二版**：C++ 保留 Raft + KV 状态机，Java 负责 Watch / Lease / Coordinator API
3. **当前版（第三版）**：C++ 降级为**纯 Raft 复制日志层**（不包含任何业务语义），Java 承担全部业务逻辑（KV 状态机 + Watch + Lease + Coordinator API）

第三版的核心思想来自 etcd 架构：`etcd/raft` 库是纯状态机，上层应用驱动并 apply 已提交日志。我们的 C++ 等价于 `etcd/raft`，Java 等价于 `etcdserver` 包。ZooKeeper 也证明了全部业务逻辑放在 Java 中是可行的。

---

## 核心决策

### 语言分工

| 层 | 语言 | 理由 |
|---|---|---|
| Raft 共识 / WAL / Snapshot 存储 | **C++20** | 零 GC、可预测尾延迟、直接控制磁盘 I/O |
| MVCC KV 状态机 / Watch / Lease / Coordinator API / Client SDK | **Java（Quarkus）** | 业务逻辑可读性强、云原生生态成熟、崩溃恢复简单 |
| 跨层通信 | **gRPC（log.proto）** | 类型安全、streaming、C++ 不感知业务语义 |

### 设计哲学

- **C++ 只复制 opaque bytes**：不理解 KV / Watch / Lease，只保证日志有序复制到多数节点
- **Java 拥有全部业务状态**：KvStore、WatchManager、LeaseManager 都在 Java 内存中
- **崩溃恢复**：Java 重启后通过 `LoadSnapshot` + `SubscribeCommitted` 从 C++ 重放，完全恢复状态
- **Lease 持久化**：Grant / Revoke 序列化为 Raft 日志条目，apply 时重建 Lease 状态，不会因 Java 崩溃而丢失

### 键空间：etcd 风格（扁平 + 前缀范围查询）

- key 是字节序列，支持 range scan
- 带 MVCC revision，Watch 基于 revision 推送事件

### 并发模型

- **C++ 侧**：单 Raft 线程（tick loop）+ gRPC 线程池；committed entries 通过 gRPC stream 推给 Java
- **Java 侧**：Quarkus + Mutiny 响应式；StateMachineDriver 单线程顺序 apply，Watch 扇出使用 `BroadcastProcessor`，Lease 检测用 `@Scheduled`

---

## 技术栈

| 组件 | 选择 | 理由 |
|---|---|---|
| C++ 构建 & 包管理 | **CMake + Conan 2** | 工业标准构建系统 + 成熟包管理器 |
| Java 框架 | **Quarkus** | GraalVM native image、内置健康探针、Mutiny 响应式 |
| Java 构建 | **Maven** | Quarkus 官方首选，插件生态完善 |
| RPC | **gRPC** | 跨语言、高性能、streaming 支持 |
| 序列化 | **Protocol Buffers 3** | gRPC 标配 |
| 存储 | 自定义 WAL + snapshot 文件 | 保持简单，避免 RocksDB 重依赖 |
| 测试（C++） | Google Test | C++ 测试标准 |
| 测试（Java） | Quarkus Test + RestAssured | 集成 Quarkus DI，支持 `@QuarkusTest` |
| 日志（C++） | spdlog | 轻量、高性能 |
| 日志（Java） | quarkus-logging-json | 结构化 JSON，对接 Fluent Bit / Loki |
| 指标 | quarkus-micrometer-registry-prometheus | `/q/metrics` 供 Prometheus 抓取 |
| 追踪 | quarkus-opentelemetry | 自动 gRPC span，导出到 Jaeger/Tempo |

---

## 分层架构

```
┌────────────────────────────────────────────────────────────────┐
│  Java / Quarkus — 全部业务逻辑                                  │
│                                                                │
│  ┌────────────────────────────────────────────────────────┐    │
│  │  EngineClient (对外 Java SDK)                          │    │
│  ├────────────────────────────────────────────────────────┤    │
│  │  CoordinatorGrpcService (实现 coordinator.proto)       │    │
│  │  ├── 写请求 → StateMachineDriver.propose()            │    │
│  │  ├── 读请求 → KvStore.get() (本地内存读)               │    │
│  │  ├── Watch  → WatchManager.register()                 │    │
│  │  └── Lease  → LeaseManager.grant/revoke/keepAlive()   │    │
│  ├────────────────────────────────────────────────────────┤    │
│  │  StateMachineDriver (核心驱动)                         │    │
│  │  ├── SubscribeCommitted → 顺序 apply                  │    │
│  │  ├── apply → KvStore + LeaseManager                   │    │
│  │  ├── apply → WatchManager.notify()                    │    │
│  │  └── 定期 SaveSnapshot → C++                          │    │
│  ├────────────────────────────────────────────────────────┤    │
│  │  KvStore (MVCC)  │ WatchManager │ LeaseManager        │    │
│  └──────────────────┴──────────────┴─────────────────────┘    │
│                         │ gRPC (log.proto)                │
└─────────────────────────┼──────────────────────────────────────┘
                          │
┌─────────────────────────▼──────────────────────────────────────┐
│  C++ — 纯 Raft 复制日志层 (不含任何业务语义)                     │
│                                                                │
│  RaftLogService (log.proto 服务端)                         │
│  ├── Propose(bytes) → RaftNode.Start()                         │
│  ├── SubscribeCommitted → 流推送已提交日志条目                   │
│  ├── SaveSnapshot / LoadSnapshot → 快照持久化                    │
│  └── Status → is_leader / term                                 │
│                                                                │
│  RaftNode (共识 + ticker 线程 + gRPC transport)                 │
│  FileStorage (WAL + Snapshot 持久化)                             │
└────────────────────────────────────────────────────────────────┘
```

### 关键数据流

#### 写入 `Put("/key", "val")`

```
Client
  │ coordinator.proto Put
  ▼
Java CoordinatorGrpcService.put()
  │ 序列化为 bytes: [OP_PUT | PutRequest]
  │ StateMachineDriver.propose(bytes)
  │   │
  │   │ gRPC: RaftLog.Propose(bytes)
  │   ▼
  │ C++ RaftLogService
  │   │ RaftNode.Start(bytes)
  │   │ ... Raft 复制到多数节点 ...
  │   │ committed
  │   │ gRPC stream: CommittedEntry
  │   ▼
  │ Java StateMachineDriver (收到 committed entry)
  │   │ 反序列化 → OP_PUT
  │   ├── KvStore.applyPut()
  │   ├── WatchManager.notify(PUT event)
  │   └── 如果有 lease → LeaseManager.attach()
  │
  ▼ 响应客户端
```

#### 读取 `Get("/key")`

```
Client
  │ coordinator.proto Get
  ▼
Java CoordinatorGrpcService.get()
  │ KvStore.get(key)  ← 纯内存读，不经过 C++
  ▼ 响应客户端
```

#### 崩溃恢复

```
Java 重启
  │
  ├── RaftLogClient.loadSnapshot() → 获取最新快照
  │   └── KvStore.restore(snapshot)
  │       LeaseManager.restore(snapshot)
  │
  └── RaftLogClient.subscribeCommitted(lastAppliedIndex + 1)
      └── 重放后续已提交日志 → 状态完全恢复
```

---

## Proto 文件

| 文件 | 服务方 | 调用方 | 用途 |
|---|---|---|---|
| `proto/raft.proto` | C++ RaftNode | C++ peer nodes | Raft 节点间 RPC（选举、日志复制、快照安装） |
| `proto/log.proto` | C++ RaftLogService | Java StateMachineDriver | 提交日志 + 订阅已提交流 + 快照管理 |
| `proto/coordinator.proto` | Java CoordinatorGrpcService | 外部客户端 | 完整对外 API（KV + Watch + Lease） |

---

## 项目结构

```
Engine/
├── Makefile                           # 顶层统一构建入口
├── Dockerfile.cpp                     # C++ 容器构建
├── Dockerfile.java                    # Java 容器构建（GraalVM Native）
├── docker-compose.yml                 # 3 C++ 节点 + 1 Java coordinator
├── proto/                             # 共享 Proto 定义
│   ├── raft.proto                     # Raft 节点间 RPC
│   ├── log.proto                      # C++ ↔ Java：复制日志接口
│   └── coordinator.proto              # 对外客户端 API（Java 实现）
├── core/                              # C++ 共识核心（纯 Raft 层）
│   ├── CMakeLists.txt                  # C++ 构建脚本
│   ├── conanfile.txt                  # C++ 依赖声明（Conan 2）
│   ├── src/
│   │   ├── raft/
│   │   │   ├── raft.h / raft.cpp      # Raft 核心状态机（纯逻辑，无 I/O）
│   │   │   ├── storage.h / storage.cpp # WAL + Snapshot 持久化
│   │   │   ├── node.h / node.cpp      # RaftNode：Raft + gRPC transport
│   │   │   ├── config.h               # Raft 配置（超时、集群成员）
│   │   │   └── log.h                  # LogEntry 类型定义
│   │   ├── server/
│   │   │   └── server.h / server.cpp  # RaftLogService + GrpcServer
│   │   ├── common/
│   │   │   ├── types.h                # 类型别名（int64 / uint64 / size / byte）
│   │   │   ├── status.h               # 错误码定义
│   │   │   └── encoding.h             # CRC32 / 编解码工具
│   │   └── main.cpp                   # C++ 入口
│   └── tests/
│       └── raft_test.cpp              # C++ 单元测试
├── coordinator/                       # Java / Quarkus 协调层
│   ├── pom.xml
│   └── src/
│       ├── main/
│       │   ├── proto/ → ../../proto/  # symlink，复用 proto 文件
│       │   ├── java/engine/
│       │   │   ├── coordinator/
│       │   │   │   ├── CoordinatorGrpcService.java
│       │   │   │   ├── StateMachineDriver.java
│       │   │   │   ├── KvStore.java
│       │   │   │   ├── WatchManager.java
│       │   │   │   └── LeaseManager.java
│       │   │   ├── client/
│       │   │   │   ├── RaftLogClient.java
│       │   │   │   └── EngineClient.java
│       │   │   └── health/
│       │   │       └── CoordinatorHealthCheck.java
│       │   └── resources/
│       │       └── application.properties
│       └── test/
│           └── java/engine/coordinator/
│               └── CoordinatorTest.java
```

---

## C++ 类型约定

所有 C++ 文件统一 `#include "common/types.h"`，使用以下别名：

| 别名 | 原始类型 |
|---|---|
| `int8` `int16` `int32` `int64` | `std::int8_t` … `std::int64_t` |
| `uint8` `uint16` `uint32` `uint64` | `std::uint8_t` … `std::uint64_t` |
| `size` | `std::size_t` |
| `byte` | `std::uint8_t` |

---

## 关键接口设计

### C++：log.proto（纯复制日志接口）

```protobuf
service RaftLog {
  // Java 提交日志条目（opaque bytes），由 C++ Leader 复制到多数节点
  rpc Propose (ProposeReq) returns (ProposeResp);

  // Java 订阅已提交日志流，从指定 index 开始（用于启动重放 + 实时 apply）
  rpc SubscribeCommitted (SubscribeReq) returns (stream CommittedEntry);

  // Java 生成快照后上传给 C++，C++ 截断日志
  rpc SaveSnapshot (stream SnapshotChunk) returns (SaveSnapshotResp);

  // Java 启动时获取最新快照（落后的 follower 也走这个接口）
  rpc LoadSnapshot (LoadSnapshotReq) returns (stream SnapshotChunk);

  // 集群状态：is_leader / term / leader_id
  rpc Status (StatusReq) returns (StatusResp);
}
```

C++ 不包含 `Put` / `Get` / `Delete` 等业务 RPC — 它只复制 bytes。

### Java：StateMachineDriver（核心驱动）

```java
@ApplicationScoped
public class StateMachineDriver {
    @Inject RaftLogClient raftClient;
    @Inject KvStore       kvStore;
    @Inject WatchManager  watchManager;
    @Inject LeaseManager  leaseManager;

    // 启动：LoadSnapshot → SubscribeCommitted → apply loop
    @PostConstruct void start() { ... }

    // 写请求：序列化 → Propose → 等待 apply
    public CompletableFuture<Long> propose(byte opType, Message msg) { ... }

    // apply 一条已提交的日志
    private void applyEntry(CommittedEntry entry) {
        // 反序列化 → 分发给 KvStore / LeaseManager
        // → WatchManager 通知
    }
}
```

### Java：KvStore（MVCC 状态机）

```java
@ApplicationScoped
public class KvStore {
    // 内存中的有序 map
    private TreeMap<String, MvccValue> store;
    private long revision;

    public void applyPut(PutRequest req)       { ... }
    public void applyDelete(DeleteRequest req) { ... }
    public GetResponse get(GetRequest req)     { ... }  // 纯内存读
    public byte[] snapshot()                   { ... }
    public void restore(byte[] data)           { ... }
}
```

### Java：CoordinatorGrpcService（对外 API）

```java
@GrpcService
public class CoordinatorGrpcService extends CoordinatorImplBase {
    @Inject StateMachineDriver driver;
    @Inject KvStore            kvStore;
    @Inject WatchManager       watchManager;
    @Inject LeaseManager       leaseManager;

    // 写 → driver.propose()
    // 读 → kvStore.get()（纯内存，不经过 C++）
    // Watch → watchManager.register()
    // Lease → leaseManager.grant() → driver.propose(LEASE_GRANT)
}
```

---

## 云原生特性（Java 侧）

| 特性 | 实现 | Kubernetes 集成 |
|---|---|---|
| 存活探针 | `@Liveness` → `/q/health/live` | `livenessProbe.httpGet` |
| 就绪探针 | `@Readiness` → `/q/health/ready` | `readinessProbe.httpGet` |
| 指标 | Micrometer → `/q/metrics` | `ServiceMonitor` (Prometheus Operator) |
| 链路追踪 | OpenTelemetry → 自动 gRPC span | Jaeger / Tempo sidecar |
| 结构化日志 | JSON → stdout | Fluent Bit DaemonSet → Loki |
| 配置注入 | `@ConfigProperty` + env var | ConfigMap / Secret |

---

## 实施阶段

### Phase 1：骨架 + 构建系统 ✅

- CMakeLists.txt + conanfile.txt 配置，proto 文件定义
- 目录结构建立

### Phase 2：Raft 共识核心 ✅

- Raft 状态机（从 Rust 移植）：选举 / 日志复制 / 冲突解决 / Snapshot
- WAL FileStorage（CRC32 帧格式）
- RaftNode：ticker 线程 + gRPC transport

### Phase 3：C++ RaftLogService ← 当前

- 新增 `log.proto`，C++ 实现 `RaftLogService`
- 移除 C++ 侧 KvStore / StateMachine（业务逻辑移入 Java）
- committed entry 通过 gRPC stream 推送给 Java
- 快照接收 / 发送接口

### Phase 4：Java 状态机 + Coordinator

- `StateMachineDriver`：LoadSnapshot → SubscribeCommitted → apply loop
- `KvStore`：Java 版 MVCC KV（从 C++ 移植）
- `WatchManager`：直接观察 apply 过程，无需 C++ 事件流
- `LeaseManager`：Grant / Revoke 序列化为 Raft 日志，apply 时重建状态
- `CoordinatorGrpcService`：实现完整 coordinator.proto
- `RaftLogClient`：封装 log.proto 的 gRPC stub
- `CoordinatorHealthCheck`：调 RaftLogClient.status()

### Phase 5：客户端 SDK + 完善测试

- `EngineClient.java`：封装 gRPC stub + 自动重试 + leader 发现
- C++ 单元测试：Raft 选举 / 日志复制 / 持久化
- Java 集成测试：`@QuarkusTest` Put/Get/Watch/Lease 全链路
- 基准测试（吞吐量、延迟）

### Phase 6：分布式原语（为计算引擎准备）

- 分布式锁（基于 Lease + 特定 key）
- Leader Election 原语（基于 Lease + Watch）
- Service Discovery（注册/注销 + Watch）

---

## 与计算引擎（Java）对接

| 场景 | 使用的 Engine 特性 |
|---|---|
| Worker 注册 | `Put("/workers/{id}", ...) + LeaseGrant` |
| Master 选举 | `LeaseGrant + Put("/master/lock") + Watch` |
| 任务分配 | `Put("/tasks/{id}") + Watch("/tasks/")` |
| 配置管理 | `Put("/config/...") + Watch("/config/")` |

> 计算引擎直接使用 `EngineClient.java`，与 Quarkus Coordinator 通过 `coordinator.proto` 通信，无需感知底层 C++ Raft。
