# Tiny Coordinator 能力补强设计

## 1. 愿景

Tiny Coordinator 的主体定位是一个职责完整、边界清晰的 etcd-like 分布式协调器，而不是某个业务场景的专用状态服务。

它应当向上层应用提供可靠的通用协调原语：

- 线性一致 KV 读写
- MVCC 历史读取与压缩
- 原子事务与 Compare-And-Swap
- 可恢复的 Watch
- Lease 与临时键
- Leader 发现、故障转移和客户端重试
- WAL、Snapshot 与崩溃恢复
- 集群状态、健康检查、指标和可验证的故障语义

LLM inference router 是验证这些原语的首个上层应用，但任何 LLM、GPU、KV Cache 或路由语义都不得进入 coordinator 的通用状态机。

## 2. 当前架构边界

系统维持两层结构：

```text
Java / Quarkus
Coordinator gRPC API、MVCC、Txn、Watch、Lease、状态机
                    │
                    │ JNI：opaque command / committed entry / snapshot
                    ▼
Rust
Raft、节点间 gRPC、WAL、Snapshot 存储
```

架构约束：

1. Rust core 只复制 opaque bytes，不解析业务命令。
2. Java `StateMachineDriver` 是 replicated state 的唯一 apply 入口。
3. 所有影响集群状态的操作都必须先经过 Raft commit，再对客户端成功返回。
4. MVCC revision 与 Raft log index 是两个独立概念，不得混用。
5. 相同日志序列必须在所有节点产生完全一致的状态与响应。

## 3. 设计原则

### 3.1 正确性优先

展示项目首先要能明确说明其一致性、原子性、恢复和失败行为。吞吐优化不能以模糊语义为代价。

### 3.2 完成协调内核，不追求完整复刻 etcd

项目实现支撑真实协调应用所需的核心闭环，但不以二进制兼容、API 全覆盖或生产级运维功能为目标。

### 3.3 控制状态与高频软状态分离

适合共识复制的是体积小、更新频率可控、需要强一致的控制状态。请求级指标、缓存索引等可重建高频状态应留在上层应用本地。

### 3.4 故障行为可测试

每项能力都必须有单节点单元测试、三节点集成测试和对应的故障注入场景。

## 4. 需要补齐的能力

### 4.1 统一复制命令与确定性 Apply

定义带版本的状态机命令封装：

```text
StateMachineCommand
  schema_version
  proposal_id
  client_id
  request_id
  operation
```

`operation` 覆盖：

- Put
- DeleteRange
- Txn
- Compact
- LeaseGrant
- LeaseRenew
- LeaseRevoke
- LeaseExpire

要求：

1. Txn、Compact 和全部 Lease 状态变更与 Put/Delete 一样经过 Raft。
2. 一个已提交命令在 apply 阶段一次性完成，不允许 gRPC handler 直接修改状态。
3. apply 失败不得静默推进并向客户端报告成功；非法命令产生确定性的失败结果。
4. `proposal_id` 用于关联异步提交结果，避免仅靠 log index 产生注册与回调竞态。
5. `client_id + request_id` 用于有限窗口的幂等去重，使客户端可以安全重试。

### 4.2 MVCC 与 Revision

MVCC revision 表示业务状态变化序列，不等同于 Raft index。

语义要求：

- 每个产生写入的单命令只分配一个 revision。
- 一个 Txn 中的多项写入共享同一个 revision。
- DeleteRange 删除多个键时共享一个 revision。
- 纯读 Txn 不增加 revision。
- 历史读取可以查询尚未 compact 的 revision。
- 已 compact revision 返回明确的 `COMPACTED` 错误和当前 compact revision。
- Watch 看到同一事务产生的事件具有相同 revision，并按稳定顺序交付。

### 4.3 原始字节键与有序 Range

Proto 中的 key/value 保持 bytes 语义，Java 内部不得先转换为 UTF-8 `String`。

键比较采用 unsigned lexicographical ordering，并支持：

- 单键查询
- `[key, range_end)` 范围查询
- 前缀查询
- 全键空间查询
- 稳定排序
- limit、more 和不受 limit 影响的 count

内部结构必须提供稳定的有序索引。具体实现可以是带字节比较器的有序 Map；引入磁盘 B+Tree 或 RocksDB 不属于本阶段目标。

### 4.4 原子 Txn/CAS

Txn 在状态机 apply 线程中完成 Compare 与 Then/Else 分支，期间不会观察或暴露中间状态。

首期支持：

- VALUE
- CREATE_REVISION
- MOD_REVISION
- VERSION
- 键是否存在
- EQUAL、NOT_EQUAL、GREATER、LESS
- Put、DeleteRange、Range 子操作

所有比较值在 Proto 中使用匹配的数据类型，revision/version 不通过 UTF-8 字符串解析。嵌套 Txn 不属于首期范围。

### 4.5 读一致性与 Leader 感知

Range 默认提供线性一致读：

1. 请求由 Leader 处理。
2. Leader 通过 ReadIndex 或等价的 quorum barrier 获得安全 read index。
3. Java 状态机等待 `lastApplied >= readIndex` 后读取。

客户端可显式请求 serializable read，此时任意已就绪节点可以返回本地状态，结果允许落后。

Rust 通过 JNI 暴露：

- role
- term
- leader_id
- commit_index
- applied_index

Follower 对需要 Leader 的请求返回结构化 `NOT_LEADER`，并在已知时附带 Leader endpoint。客户端缓存 Leader、切换 endpoint，并对幂等请求执行有界重试。

### 4.6 可恢复 Watch

Watch 提供从全量快照平滑切换到增量事件的能力：

```text
Range(prefix) -> revision R
Watch(prefix, start_revision = R + 1)
```

保证：

- 从指定 revision 开始无缺口交付。
- 单个 watch 内事件有序且不重复。
- 注册历史回放与实时事件切换期间不乱序。
- 请求已 compact revision 时返回 `compact_revision` 并取消 watch。
- Cancel 返回 canceled acknowledgment。
- `prev_kv` 按请求填充。
- 支持 progress notification，便于客户端记录恢复点。
- 长连接不因固定存活时长被强制删除。
- 对慢消费者使用有界队列；超过限制时以明确错误断开，客户端从最后确认 revision 恢复。

客户端 SDK 保存最后处理 revision，Watch 断开后自动重连。

### 4.7 完整 Lease 生命周期

Lease 用于临时键和客户端存活检测。

要求：

- `id = 0` 时由服务端生成唯一 Lease ID。
- LeaseGrant、Renew、Revoke、Expire 都通过 Raft 形成确定性状态。
- 首期 `LeaseKeepAlive` 每次提交一个 `LeaseRenew` 命令；这牺牲部分吞吐，但使 Leader 切换后的 TTL 行为简单、可解释且可恢复。
- 只有 Leader 扫描过期 Lease，并通过 `LeaseExpire` 命令提交过期结果。
- Revoke/Expire 与全部附属键删除在一个命令、一个 revision 中完成。
- 每个键最多绑定一个 Lease；换绑和删除时更新反向索引。
- Lease 删除附属键时产生正常的 Watch DELETE 事件。
- Lease 和键绑定关系进入状态机 Snapshot。

未来可以将逐次 Renew 优化为 leader-local keepalive 加周期性 Lease checkpoint，但不改变 API 语义。

### 4.8 Snapshot 与崩溃恢复

定义版本化的组合 Snapshot：

```text
StateMachineSnapshot
  schema_version
  last_applied_index
  mvcc_revision
  compact_revision
  kv_versions_after_compaction
  lease_state
  lease_key_index
  dedup_state
```

要求：

- Snapshot 对应明确的 Raft index/term。
- Snapshot 在 apply 串行语义下生成一致视图，异步持久化不能混入更晚状态。
- Rust 保存并返回 `last_index + last_term + data`，JNI 不只返回 payload。
- Java 恢复 Snapshot 后从 `last_applied_index + 1` 重放 committed entries。
- Snapshot 包含 compact revision 之后仍需支持历史读取和 Watch replay 的版本。
- 恢复错误使节点保持 not-ready，不允许以空状态继续提供服务。
- Snapshot schema 版本不兼容时返回明确错误。

### 4.9 JNI 协议安全

JNI 边界必须显式定义：

- ABI/schema version
- 字节序
- buffer layout
- payload length
- 最大消息尺寸
- 错误码
- native handle 生命周期

优先使用 protobuf 封装复杂消息。保留固定布局时，两侧都必须显式设置同一字节序并做容量检查。Rust FFI 入口不得让 panic 穿越 JNI 边界。

### 4.10 集群与运维可见性

补齐：

- ResponseHeader：cluster ID、member ID、MVCC revision、Raft term
- MemberList：首期返回静态配置成员
- Status：role、leader、term、commit/applied index、apply lag
- Maintenance Snapshot 导出与状态查询
- readiness：Raft 已启动、已发现 Leader、状态机已恢复并追平
- liveness：进程和本地事件循环可响应

首期不支持运行时成员增删。

### 4.11 可观测性

至少暴露以下指标：

- Raft leader changes
- proposal commit latency
- committed/applied index 与 apply lag
- WAL 和 Snapshot 大小
- MVCC current/compact revision
- active watch 数量与慢消费者断开数
- active lease 数量与过期数
- gRPC 请求延迟和错误码
- Snapshot 保存/恢复次数与耗时

日志携带 member ID、term、log index、revision 和 proposal ID，便于跨层定位问题。

## 5. 面向 Router 的关键使用路径

### 5.1 Worker 注册

```text
LeaseGrant
Txn: worker key 不存在或旧 epoch 已失效
Put(worker metadata, lease)
LeaseKeepAlive stream
```

### 5.2 Router 启动

```text
Linearizable Range("/inference/v1/workers/") -> revision R
Watch("/inference/v1/workers/", R + 1)
```

Router 必须获得一个无缺口的 worker 视图。

### 5.3 Worker 故障

```text
KeepAlive 停止
Leader 提交 LeaseExpire
原子删除 worker key
Watch 交付 DELETE
Router 停止选择该 worker
```

### 5.4 配置 CAS

Router 或控制器使用 `mod_revision` Compare 原子更新模型部署和 routing policy，避免旧配置覆盖新配置。

## 6. 不在本阶段范围

- 完整 etcd API/协议兼容
- Auth、RBAC 和多租户权限
- 客户端与 peer TLS
- 运行时集群成员增删
- Kubernetes Operator
- 磁盘 B+Tree、RocksDB 或分布式 SQL 存储
- 跨大版本在线升级
- Jepsen 完整验证框架

这些能力可以在协调内核稳定后独立演进。

## 7. 验收标准

### 7.1 功能验收

- Put、DeleteRange、Txn、Compact、Lease 状态在三个节点一致。
- 默认 Range 不返回 Leader 尚未提交或尚未 apply 的状态。
- 多操作 Txn 原子执行并共享 revision。
- bytes key 按稳定字典序进行范围查询。
- Watch 可以从 Range 返回的 revision 无缝衔接。
- Lease 过期原子删除附属键并产生 Watch 事件。
- 全量重启后 KV、MVCC、Lease、compact revision 和 dedup 状态恢复。

### 7.2 故障验收

- Kill Leader 后集群重新选主，客户端恢复写入。
- Router 风格客户端的 Watch 从最后 revision 重连，无缺口、无重复。
- Follower 落后、重启并安装 Snapshot 后能够追平。
- Leader 在 Lease 活跃期间故障，新 Leader 不会永久保留幽灵临时键。
- JNI 非法 buffer、超长 payload 和关闭中的 native handle 不导致进程崩溃。

### 7.3 测试验收

- Rust Raft 单元与三节点网络模拟测试。
- Java MVCC/Txn/Watch/Lease 确定性单元测试。
- Rust + JNI + Java 三节点端到端测试。
- Leader crash、Follower restart、网络隔离、Snapshot restore 和慢 Watch 消费者测试。
- 测试输出可以报告选主耗时、Watch 恢复耗时和 Lease 故障摘除耗时。

## 8. 完成定义

当 Router 可以仅依赖公开 Coordinator API，在 Leader 切换、Router 重启和 Worker Lease 过期场景下维持一个无缺口、无幽灵节点的 worker 视图时，协调器能力补强阶段完成。
