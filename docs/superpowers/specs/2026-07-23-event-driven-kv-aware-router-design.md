# Event-driven KV-aware Inference Router 设计

## 1. 愿景

在 Tiny Coordinator 之上构建一个独立的 LLM inference router，展示通用分布式协调原语如何支撑真实推理基础设施。

Router 的目标是：

- 发现并管理推理 Worker
- 在 Worker 故障时快速停止路由
- 根据真实 KV cache 生命周期事件维护 Prefix Index
- 综合缓存命中与实时负载选择 Worker
- 在无 GPU 环境中完成可重复 Benchmark
- 通过适配器接入真实 vLLM 类推理引擎

Router 是 Tiny Coordinator 的上层应用。任何模型、token、KV block、GPU 或路由算法语义都不进入协调器核心。

## 2. 项目边界

本设计实现到 **Event-driven KV-aware Router** 层级：

```text
OpenAI-compatible Client
           │
           ▼
Active-Active Routers
  - Worker Registry View
  - Token/Block Processor
  - Local Prefix Index
  - KV + Load Scorer
  - Streaming Proxy
           │
     ┌─────┴───────────┐
     ▼                 ▼
Tiny Coordinator    Inference Workers
durable control     inference + KV events
state
```

本阶段处理 KV cache 的**元数据与路由决策**，不传输、复制或持久化真实 KV Tensor。

## 3. 与 Tiny Coordinator 的职责划分

### 3.1 Coordinator 保存的可靠控制状态

- Worker membership
- Worker Lease
- Worker instance epoch
- Worker endpoint 与模型能力
- Model deployment metadata
- Routing policy
- Router instance registration
- 可选 Controller Leader key

### 3.2 Router 本地保存的可重建软状态

- KV block Prefix Index
- Worker queue depth 和 cache utilization
- Cache event sequence watermark
- 请求级统计
- 短期熔断状态

KV block add/remove 等高频事件不逐条写入 Raft。Router 重启后从 Worker snapshot 与 event stream 重建索引。

## 4. 组件设计

### 4.1 Router Proxy

提供简化的 OpenAI-compatible HTTP 接口，首期至少支持一个文本生成 endpoint 和 streaming response 透传。

职责：

- 校验模型与请求
- 调用 Token/Block Processor
- 选择候选 Worker
- 执行路由评分
- 将请求代理到 Worker
- 记录 TTFT、完成状态和错误
- 在请求尚未开始返回 token 时执行有限重试

已经开始向客户端输出 token 的请求不透明重试，避免重复 token。

### 4.2 Worker Registry View

Router 启动时执行：

```text
Linearizable Range("/inference/v1/workers/") -> revision R
Watch("/inference/v1/workers/", R + 1)
```

本地视图记录：

- worker ID
- instance epoch
- endpoint
- model IDs
- tokenizer identity
- KV block size
- max concurrency
- cache event endpoint
- registration revision

Watch 断开后从最后确认 revision 自动恢复。

### 4.3 Token/Block Processor

接口负责：

1. 按模型选择 tokenizer。
2. 将 prompt 转换为 token IDs。
3. 按 Worker KV block size 切块。
4. 计算与推理引擎一致的连续 block hash。

Block hash 至少绑定：

- model identity
- tokenizer identity
- block size
- parent block hash
- 当前 block token IDs
- 可选 LoRA/adapter identity

Mock 模式使用确定性 tokenizer 与 hash 实现。真实引擎适配器必须验证 hash 算法与 Worker 事件一致；无法保证一致时，Router 降级为 prefix-affinity 或 least-load，而不声称 precise KV hit。

### 4.4 KV Event Adapter

统一事件协议：

```text
CacheEvent
  worker_id
  worker_epoch
  sequence
  model_id
  event_type       // ADDED, REMOVED, CLEARED
  block_hash
  parent_block_hash
```

适配器来源：

- Mock Worker event stream
- 录制的 KV event trace
- vLLM-compatible KV event stream

事件要求：

- 每个 `worker_id + epoch` 内 sequence 单调递增。
- Router 忽略旧 epoch 事件。
- 出现 sequence gap 时，标记该 Worker 索引不可信并触发 resync。
- CLEARED 立即移除该 Worker 的全部 block ownership。

### 4.5 Prefix Index

Prefix Index 将连续 block hash 映射到持有该前缀的 Worker 集合。

必须支持：

- 添加和移除 block ownership
- 按 Worker epoch 清空
- 查询请求在各 Worker 上连续命中的 block 数
- Worker 故障时批量移除
- 并发查询与事件更新
- 暴露索引大小、更新延迟和查询耗时

索引是可重建状态，不要求写入 Coordinator Snapshot。

### 4.6 Load View

Worker 周期性报告：

- running requests
- pending requests
- max concurrency
- KV cache utilization
- 可选平均 prefill/decode latency

高频负载指标直接由 Router 拉取或订阅，不写入 Raft。Worker Registry 中只保留低频能力描述。

### 4.7 Routing Scorer

提供至少四种可切换策略：

1. Round Robin
2. Least Load
3. Prefix Aware
4. KV + Load Hybrid

Hybrid 的基础评分：

```text
cache_score = matched_prefix_blocks / request_prefix_blocks
load_score = pending_requests / max_concurrency
pressure_score = kv_cache_utilization

final_score =
    cache_weight * cache_score
  - load_weight * load_score
  - pressure_weight * pressure_score
  + bounded_jitter
```

约束：

- 未完成 KV index 同步的 Worker 不获得 precise cache bonus。
- Worker 过载时，负载惩罚可以覆盖缓存命中奖励。
- 所有权重来自 Coordinator 中的 versioned routing policy。
- 配置更新使用 Txn/CAS，并通过 Watch 分发到所有 Router。

## 5. Coordinator 键空间

建议使用版本化前缀：

```text
/inference/v1/workers/{worker_id}
/inference/v1/models/{model_id}
/inference/v1/policies/{model_id}
/inference/v1/routers/{router_id}
/inference/v1/controllers/router/leader
```

Worker value 包含：

```text
WorkerRegistration
  worker_id
  instance_epoch
  inference_endpoint
  cache_event_endpoint
  model_capabilities[]
  tokenizer_id
  block_size
  max_concurrency
```

Worker registration key 绑定 Lease。进程每次启动获得新的 instance epoch；旧 epoch 的缓存事件和健康报告不能影响新实例。

## 6. 关键数据流

### 6.1 Worker 注册

```text
Worker -> LeaseGrant
Worker -> Txn/CAS registration key
Worker -> LeaseKeepAlive
Router <- Watch PUT
Router -> connect metrics/cache event endpoints
```

同一个 worker ID 的并发实例通过 registration revision 和 instance epoch fencing，防止旧进程覆盖新进程。

### 6.2 Router 启动

Router 有两个就绪阶段：

1. `routing_ready`：Worker Registry 和 Routing Policy 已恢复，可以 Least Load 路由。
2. `kv_index_ready`：相关 Worker 的 Cache Snapshot 已安装且事件流已衔接，可以启用 precise KV-aware 评分。

启动期间采用渐进降级：

```text
未恢复 Worker View -> not ready
Worker View 已恢复 -> Least Load
部分 KV Index 已恢复 -> Hybrid，降低 cache 权重
KV Index 已恢复 -> 正常 Hybrid
```

### 6.3 Cache Snapshot 与事件无缝衔接

Worker/Adapter 提供：

```text
GetCacheSnapshot() -> epoch, sequence S, blocks[]
SubscribeCacheEvents()
```

Router 恢复单个 Worker 索引：

1. 先订阅并缓冲 Cache Events。
2. 请求 Cache Snapshot，获得 sequence `S`。
3. 原子安装 Snapshot。
4. 丢弃缓冲区中 `sequence <= S` 的事件。
5. 按顺序应用 `sequence > S` 的事件。
6. 发现 sequence gap 或 epoch 变化时重新同步。

该流程保证 Router 重启后不会因为 Snapshot 与实时事件并发而漏掉 block 更新。

### 6.4 请求路由

```text
Request
  -> model candidate workers
  -> tokenize and block-hash prompt
  -> query Prefix Index
  -> read Load View
  -> score candidates
  -> select worker
  -> proxy streaming response
  -> record routing decision and metrics
```

路由日志记录：

- request ID
- model ID
- selected worker
- matched blocks/tokens
- cache/load/pressure 分数
- fallback reason
- routing overhead

## 7. 高可用与故障恢复

### 7.1 Router 采用 Active-Active

多个 Router 同时服务请求，各自维护本地 Prefix Index。请求路径不选 Leader。

Router 崩溃时：

- 外部负载均衡通过 readiness 摘除实例。
- 其他 Router 继续服务。
- 已开始输出 token 的 in-flight stream 失败并由客户端决定是否重试。
- 重启 Router 重新执行 Worker Range/Watch 和 Cache Snapshot/Event 恢复。

### 7.2 Coordinator Leader 故障

- Raft 重新选主。
- Router 暂时使用最后已知 Worker View 继续服务。
- Watch 从最后确认 revision + 1 重连。
- 控制面不可用超过配置的最大陈旧窗口后，Router 标记 degraded。
- 请求路径通过 Worker timeout/circuit breaker 避免完全依赖 Lease 才发现故障。

### 7.3 Worker 故障

两级保护：

1. 数据面 timeout/circuit breaker 快速停止向故障 Worker 发送请求。
2. LeaseExpire 提供全局一致的成员删除，并通过 Watch 通知所有 Router。

Worker 使用相同 ID 重启时必须获得新 epoch，Router 清除旧索引并拒绝延迟到达的旧事件。

### 7.4 可选单 Leader Controller

请求 Router 不需要选主。未来如果出现必须单写的后台任务，可以使用：

```text
Lease + Txn(create_revision == 0) + fencing revision
```

选出 Router Controller Leader。

Controller 只负责全局策略计算、配置迁移或周期性单写任务，不进入请求转发路径。首期没有真实单写任务时不实现该角色。

## 8. 错误处理与降级

| 故障 | 行为 |
|---|---|
| Prefix Index 未就绪 | 降级 Least Load |
| Tokenizer/hash 不匹配 | 禁用 precise cache bonus |
| Cache event sequence gap | 单 Worker resync |
| Worker epoch 变化 | 清空旧 Worker index |
| Worker 负载数据陈旧 | 提高负载惩罚或临时摘除 |
| Coordinator Watch 断开 | 使用最后 revision 重连 |
| Coordinator 短暂不可用 | 有界时间内使用最后控制视图 |
| Worker 请求超时 | 熔断并选择其他 Worker |
| Streaming 已开始后 Worker 失败 | 终止 stream，不透明重试 |

## 9. Mock Worker 与可重复 Benchmark

Mock Worker 在普通笔记本上模拟：

- KV block 分配与淘汰
- Cache Snapshot 与 Cache Events
- 并发队列
- Prefix 命中后的 prefill 节省
- TTFT 和生成耗时
- Worker crash/restart/epoch change

基础延迟模型：

```text
prefill_time = uncached_tokens * prefill_cost_per_token
queue_time = pending_requests * queue_cost
simulated_ttft = prefill_time + queue_time
```

Benchmark 对比四种路由策略，并覆盖：

- 共享 system prompt
- 多轮会话
- RAG 公共上下文
- 热点 prefix
- 随机无共享 prompt
- Worker 负载不均衡
- Router 重启
- Worker 故障与同 ID 重启
- Coordinator Leader 切换

指标：

- prefix cache hit ratio
- matched/avoided prefill tokens
- simulated TTFT 的平均值与 P95/P99
- request throughput
- load skew
- routing decision latency
- Router recovery time
- KV index warm-up time
- Worker failure removal time
- Watch reconnect gap/duplicate count

## 10. 真实推理引擎接入

Event-driven Router 的核心通过接口与具体引擎解耦：

- `TokenizerAdapter`
- `BlockHashAdapter`
- `CacheEventAdapter`
- `WorkerMetricsAdapter`
- `InferenceProxyAdapter`

项目首先以 Mock Worker 完成确定性 CI 和故障测试。随后提供一个 vLLM-compatible KV event adapter；没有 GPU 的 CI 使用录制事件 trace 验证协议和索引，有 GPU 环境时补充真实 TTFT、cache hit 和 throughput Benchmark。

## 11. 不在本阶段范围

- KV Tensor 跨 Worker 传输
- GPU HBM、CPU RAM、SSD 分级缓存
- 全局 KV block replication
- KV cache eviction/placement planner
- Prefill/Decode disaggregation
- RDMA、NIXL 或 GPUDirect
- GPU autoscaling
- 请求迁移与 token stream resume
- 多租户计费和复杂优先级调度
- 语义路由

## 12. 更完整的分布式 KV Cache 项目参考

如果未来扩展为完整分布式 KV Cache 管理平台，需要在本 Router 之下增加独立数据面：

```text
KV-aware Router
      │ placement / reuse decision
      ▼
Global KV Metadata & Planner
      │
      ├── HBM Cache
      ├── Host Memory Cache
      ├── SSD/Object Storage Cache
      └── Cross-node Transfer Layer
```

新增职责包括：

- 全局 block ownership 与副本状态
- 热度预测和 eviction policy
- 跨层 offload/recall
- 跨 Worker cache transfer
- 拓扑、带宽和传输成本感知
- Prefill/Decode KV handoff
- 数据完整性校验
- 失败传输重试与副本修复

这类高频元数据和数据传输不直接通过 Tiny Coordinator Raft 日志完成。Coordinator 仍负责集群成员、配置、epoch、策略和控制器选主；专用 KV metadata/event plane 负责高吞吐缓存状态。

## 13. 验收标准

### 13.1 Router 功能

- Worker 可以通过 Lease 注册，并被所有 Router 发现。
- Router 使用真实事件语义维护 Prefix Index，而不是仅依赖历史请求推测。
- Hybrid 算法同时考虑连续 prefix 命中、queue load 和 cache pressure。
- Routing Policy 可以通过 Coordinator Txn/CAS 更新并被所有 Router Watch 到。
- Prefix Index 不可用时安全降级，不影响请求正确性。

### 13.2 恢复与高可用

- 任一 Router 崩溃不影响其他 Router 接收新请求。
- Router 重启后先恢复 Worker View，再无缺口衔接 Cache Snapshot/Event。
- Coordinator Leader 切换后 Worker Watch 从 revision 断点恢复。
- Worker Lease 过期后所有 Router 最终删除该 Worker。
- Worker 同 ID 重启后旧 epoch 事件不会污染新索引。
- Cache event sequence gap 可以自动检测并修复。

### 13.3 性能展示

- 同一 workload 可以重复运行 Round Robin、Least Load、Prefix Aware 和 Hybrid。
- Benchmark 报告 cache hit、avoided prefill tokens、TTFT、吞吐、负载倾斜和路由开销。
- 在共享前缀 workload 中，Hybrid 相比 Round Robin 展示可解释的 prefill 节省。
- 在热点与过载 workload 中，Hybrid 不会为了 cache hit 无限集中到单个 Worker。

## 14. 完成定义

当系统可以在普通笔记本上通过 Mock Worker 完整演示 Event-driven KV-aware routing，并能以同一接口消费 vLLM-compatible KV event trace，同时在 Router、Worker 和 Coordinator Leader 故障下保持可解释的恢复行为时，本阶段完成。
