# Engine — AI Agent 指引

## 项目概述

分布式协调器，类似 etcd，采用 C++ + Java 混合架构：

- **core/**（C++）：纯 Raft 共识复制日志，不包含任何业务语义，只复制 opaque bytes
- **coordinator/**（Java）：Quarkus 框架，承载全部业务逻辑（MVCC KV、Watch、Lease、对外 gRPC API）
- **通信**：core ↔ coordinator 通过 `log.proto` gRPC 接口（Propose / SubscribeCommitted / Snapshot）

## 架构边界（关键约束）

core **禁止**包含任何 KV / Watch / Lease 业务概念。它只做三件事：
1. 接收 opaque bytes，通过 Raft 复制到多数节点
2. 将已提交的日志条目流式推送给 coordinator
3. 存储和返回 coordinator 上传的 snapshot

coordinator 拥有全部业务状态。崩溃恢复通过 LoadSnapshot + SubscribeCommitted 重放。

## 目录结构

```
proto/                       → raft.proto（节点间）、log.proto（core↔coordinator）、coordinator.proto（对外）
core/src/raft/               → Raft 核心：状态机、WAL、RaftNode
core/src/server/             → RaftLogService + GrpcServer
core/src/common/             → 类型别名 (types.h)、错误码 (status.h)、编码工具 (encoding.h)
core/tests/                  → C++ 单元测试
core/CMakeLists.txt           → C++ 构建脚本
core/conanfile.txt            → C++ 依赖声明（Conan 2）
coordinator/                 → Java Quarkus 项目
  └ engine/coordinator/      → KvStore, WatchManager, LeaseManager, StateMachineDriver
  └ engine/client/           → RaftLogClient (调core), EngineClient (对外SDK)
  └ engine/health/           → Kubernetes 健康检查
```

## C++ 约定（core/）

- 构建系统：CMake + Conan 2（构建目录在 `core/build/`）
- 标准：C++20
- 类型：使用 `types.h` 中的别名（`int64` / `uint64` / `size` / `byte`），不用 `int64_t` / `uint64_t` / `size_t`
- `float` 和 `double` 保持原样，不做别名
- 源文件后缀：`.cpp`（不用 `.cc`）
- Proto 生成的头文件：`raft.pb.h` / `log.grpc.pb.h` 等

## Java 约定（coordinator/）

- 框架：Quarkus 3.x
- 构建：Maven（构建目录在 `coordinator/`）
- JDK：21
- 依赖注入：CDI（`@Inject` / `@ApplicationScoped`）
- gRPC 客户端：`@GrpcClient("cpp-raft")`
- 响应式：Mutiny `Multi<T>`
- 定时任务：`@Scheduled`
- 配置：`application.properties`，所有参数可通过环境变量覆盖（12-factor）

## Proto 文件角色

| 文件 | 服务方 | 调用方 | 说明 |
|------|--------|--------|------|
| `raft.proto` | core | core peers | Raft 内部通信，不要修改接口 |
| `log.proto` | core | coordinator | 纯日志复制接口，不要加业务字段 |
| `coordinator.proto` | coordinator | 外部客户端 | 完整业务 API，扩展功能加在这里 |

## 构建与运行

```bash
make build          # cd core && conan install + cmake + cd coordinator && mvn
make test           # C++ 单测 + Java 集成测试
docker compose up   # 3 core 节点 + 1 coordinator
```

## 注意事项

- 修改 proto 后两侧都要重新构建
- core 的 apply callback 只向 committed queue 推送，不做任何解析
- coordinator 的 StateMachineDriver 是唯一的 apply 入口，单线程顺序执行
- Watch 通过 KvStore listener 机制通知，不走独立的事件流
- Lease 的 Grant/Revoke 通过 Raft 日志持久化，KeepAlive 是 leader-local（不复制）
