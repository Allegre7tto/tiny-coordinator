# Engine — AI Agent 指引

## 项目概述

分布式协调器，类似 etcd，采用 Rust + Java 混合架构：

- **core/**（Rust）：纯 Raft 共识复制层，负责选举、日志复制、WAL、snapshot 和节点间 gRPC，不包含任何业务语义
- **coordinator/**（Java）：基于 Quarkus，承载全部业务逻辑（MVCC KV、Txn、Compact、Watch、Lease、对外 gRPC API）
- **进程内通信**：Java 通过 JNI 调用 Rust `cdylib`；提案和 snapshot 使用 direct `ByteBuffer`，已提交日志通过 JNI `recv` 回传
- **节点间通信**：各节点内的 Rust Raft engine 通过 `raft.proto` 定义的 gRPC 服务通信

每个部署节点都是 Java coordinator 与 Rust Raft engine 的组合体，不再单独部署 core 服务。

## 架构边界（关键约束）

core **禁止**包含任何 KV / Txn / Watch / Lease 业务概念。它只做以下事情：

1. 接收 opaque bytes，通过 Raft 复制到多数节点
2. 按 index/term 将已提交日志条目回传给 Java
3. 持久化 Raft hard state、日志和 Java 上传的 snapshot
4. 处理 Raft 节点间的 RequestVote、AppendEntries 和 InstallSnapshot RPC

coordinator 拥有全部业务状态和命令编解码逻辑。`StateMachineDriver` 负责：

1. 将业务写请求编码为 `[op_type | protobuf payload]` 后通过 JNI propose
2. 单线程、严格按提交顺序 apply 日志
3. 生成业务 snapshot 并交给 Rust 持久化
4. 启动时加载 snapshot，再继续接收后续 committed entries

不要在 Rust 中解析 `coordinator.proto` 的业务命令，也不要绕过 Raft 直接修改需要复制的 Java 业务状态。

## 目录结构

```text
proto/
  raft.proto                   → Rust Raft 节点间 RPC
  coordinator.proto            → Java 对外业务 API
core/                          → Cargo workspace
  engine/
    src/raftcore.rs            → 纯 Raft 状态机
    src/node.rs                → Tokio 驱动、提案、提交回调、snapshot
    src/peer.rs                → tonic Raft client/server
    src/wal.rs                 → WAL 与 snapshot 文件存储
    tests/integration_test.rs  → Rust Raft 集成测试
  proto/                       → raft.proto 的 prost/tonic 生成 crate
  jni/
    src/lib.rs                 → Java ↔ Rust JNI 边界，生成 libjni
coordinator/                   → Java 21 / Quarkus 项目
  src/main/java/engine/
    coordinator/               → gRPC 服务、StateMachineDriver、Watch、Lease
    mvcc/                      → MVCC、Revision、Txn、Compact
    client/                    → RaftLib JNI wrapper、EngineClient
    health/                    → Kubernetes 健康检查
Makefile                       → Rust + Java 统一构建和测试入口
Dockerfile.java                → Rust/JNI 与 Java 多阶段构建
docker-compose.yml             → 3 个组合节点
```

## Rust 约定（core/）

- 构建系统：Cargo workspace，crate 为 `engine`、`proto`、`jni`
- Edition：Rust 2021
- 异步运行时：Tokio
- 节点间 RPC：tonic + prost
- JNI 产物：`core/jni` 的 `cdylib`（Linux 为 `libjni.so`，macOS 为 `libjni.dylib`）
- Raft node id、index、term 使用 `u64`；peer 下标使用 `u32`
- opaque 日志和 snapshot 使用 `Vec<u8>` / `&[u8]`
- Raft 状态只在 engine 的 Tokio 任务中串行修改；跨任务通过 channel 传递请求
- JNI 接口必须校验 direct buffer 容量和 native handle 生命周期，避免跨 FFI unwind
- 提交回调只转发 index、term 和 opaque bytes，不解析业务内容

## Java 约定（coordinator/）

- 框架：Quarkus 3.x
- 构建：Maven（在 `coordinator/` 目录执行）
- JDK：21
- 依赖注入：CDI（`@Inject` / `@ApplicationScoped`）
- 对外服务：Quarkus gRPC，实现 `coordinator.proto`
- Rust 调用：`engine.client.RaftLib` native methods；传输 payload 时使用 direct `ByteBuffer`
- 状态机入口：`StateMachineDriver`，所有需要复制的写操作必须先 propose，提交后再 apply
- Watch 通过 `KvStore` listener 机制通知，不建立独立复制流
- 定时任务：Quarkus `@Scheduled`
- 配置：`application.properties`，参数通过环境变量覆盖（12-factor）

## Proto 文件角色

| 文件 | 服务方 | 调用方 | 说明 |
|------|--------|--------|------|
| `raft.proto` | 每个节点的 Rust engine | Rust peer nodes | Raft 内部通信；修改后重新生成 Rust proto |
| `coordinator.proto` | Java coordinator | 外部客户端 | 完整业务 API；业务功能在这里扩展 |

项目中已没有 `log.proto`。Java 与 Rust 之间通过 JNI 传递 opaque bytes，而不是通过 gRPC。

## 构建与运行

```bash
make build          # cargo build --release + mvn package
make test           # Rust 测试 + Java 测试
make build-rust     # 仅构建 Rust workspace
make build-java     # 仅构建 Java coordinator
docker compose up   # 启动 3 个 Java + Rust/JNI 组合节点
```

本地构建 Java 必须使用 JDK 21。容器构建会先生成 Rust JNI 动态库，再将它与 Quarkus 应用一起放入 JRE 21 运行时镜像。

## 注意事项

- 修改 `raft.proto` 后重新构建 Rust `proto` crate；修改 `coordinator.proto` 后重新生成 Java gRPC/protobuf 代码
- Rust engine 只复制 opaque bytes，不得依赖或解析 `coordinator.proto`
- `StateMachineDriver` 是唯一的 replicated state apply 入口，提交日志必须单线程顺序执行
- Txn、Compact、Lease Grant/Revoke 等会改变集群状态的操作也必须经过 Raft
- Watch 由已提交的 KV 变更触发，不参与 Raft 复制
- Lease Grant/Revoke 通过 Raft 持久化；KeepAlive 当前是节点本地操作
- snapshot 的 Raft index 与 MVCC revision 是不同概念，不要混用
- JNI 两侧必须显式约定整数的字节序、buffer 布局、最大尺寸和错误码编码
- core 的 Raft peer gRPC 端口与 coordinator 对外 gRPC/management 端口相互独立
