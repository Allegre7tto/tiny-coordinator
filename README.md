# Tiny Coordinator

一个用于学习 Raft 的 Java 21 分布式协调器，提供 MVCC KV、事务、Watch、
Lease、压缩、snapshot、线性一致读和 joint-consensus 成员变更。

## 模块

```text
proto/    Protobuf 与 gRPC 的唯一协议源
raft/     确定性的事件驱动 Raft 状态机
runtime/  串行 actor、异步 transport、WAL 与 snapshot
server/   Quarkus API 和协调器业务状态机
testkit/  虚拟时间、可控网络与故障注入
```

`raft` 模块只复制 opaque bytes。MVCC、Txn、Watch、Lease 和 Compact 全部位于
`server`，并通过 `CoordinatorStateMachine.apply()` 串行执行。

## 构建

仓库要求 Java 21。项目级 Maven 设置位于 `.mvn/`，从仓库根目录执行即可：

```bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.11.fx-zulu
mvn test
mvn -pl server -am package
```

也可以使用：

```bash
make test
make build
```

## 单节点运行

```bash
RAFT_NODE_ID=1 \
RAFT_MEMBERS='1=127.0.0.1:9000' \
RAFT_DATA_DIR=data/node-1 \
java -jar server/target/quarkus-app/quarkus-run.jar
```

- gRPC：`9000`
- liveness：`http://127.0.0.1:9001/q/health/live`
- readiness：`http://127.0.0.1:9001/q/health/ready`

三节点本地集群：

```bash
docker compose up --build
```

## 一致性模型

- 写请求先进入 Raft，所有节点按 index 顺序 apply。
- Range 默认经过 ReadIndex，确认当前 leader 的多数派后才读取。
- Raft index 与 MVCC revision 分离；非 KV 日志不会消耗 MVCC revision。
- Lease ID 和绝对过期时间在 leader 提案前确定，apply 不读取本地时钟。
- 成员新增先作为 learner 追平，再提交 joint configuration 和 stable
  configuration；joint 阶段同时要求新旧配置多数派。

完整设计见
[`docs/superpowers/specs/2026-07-23-java-raft-learning-architecture-design.md`](docs/superpowers/specs/2026-07-23-java-raft-learning-architecture-design.md)。
