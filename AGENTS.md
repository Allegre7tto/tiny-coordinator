# Tiny Coordinator — AI Agent 指引

## 项目概述

这是一个用于学习 Raft 的 Java 21 分布式协调器。技术栈为 Maven reactor、
Quarkus、gRPC 和 Protobuf。

```text
proto/    唯一的 .proto 来源和生成代码
raft/     不依赖网络、磁盘、线程和业务语义的确定性 Raft 状态机
runtime/  单 mailbox actor、gRPC transport、WAL 和 snapshot
server/   MVCC、Txn、Watch、Lease、Compact、公共 API 和 Quarkus 生命周期
testkit/  虚拟时钟、可控网络、故障注入和集群不变量测试
```

依赖方向为 `server -> runtime -> raft`，协议依赖统一指向 `proto`；
`testkit` 只用于测试 Raft 和 runtime。

## 架构边界

- `raft` 只复制 opaque bytes，不得依赖或解析业务 command。
- 只有 `RaftRuntime` 的串行 mailbox 能调用 `RaftCore.handle()`。
- scheduler、gRPC callback 和客户端线程只能投递事件。
- 所有 committed command 都必须在每个节点顺序 apply，本地 pending request
  只负责返回结果，不能决定是否 apply。
- `CoordinatorStateMachine.apply()` 是业务状态的唯一写入口。
- Raft index 与 MVCC revision 是两个独立概念。
- Txn、Compact、Lease Grant/Revoke/KeepAlive/Expire 必须通过 Raft。
- Watch 由 committed KV 事件派生，不参与复制，并且在状态锁外投递。
- snapshot 必须显式区分于普通 command。

## Raft 约定

- 节点 id、term 和 index 使用 `long`，配置中的节点 id 必须为正数。
- 日志类型为 `COMMAND`、`NOOP`、`JOINT_CONFIG`、`STABLE_CONFIG`。
- 成员新增先作为 learner 追平，再依次提交 joint 和 stable configuration。
- joint 阶段的选举和提交必须同时满足旧、新配置多数派。
- 线性一致读使用 ReadIndex；本地陈旧读必须通过显式 API 表达。
- raft 使用事件输入/effect 输出，不读取真实时钟或执行 I/O。

## 构建与测试

仓库根目录的 `.mvn/settings.xml` 固定使用 Maven Central/Quarkus 官方源。

```bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.11.fx-zulu
mvn test
mvn -pl server -am package
docker compose up --build
```

新 Raft 行为必须优先增加 `raft` 确定性测试；网络、时钟、重启、snapshot
和成员变更场景放在 `testkit`。业务规则测试放在 `server`。
