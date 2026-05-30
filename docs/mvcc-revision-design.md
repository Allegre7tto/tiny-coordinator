# MVCC + Revision 设计方案

> 目标：实现类似 etcd 的多版本并发控制，每次写操作递增全局 revision，支持历史查询、从特定 revision 开始 Watch、以及 compact 释放旧版本。

---

## 1. 背景与目标

### 当前状态

- KvStore：简单的 `Map<String, byte[]>`，覆盖写，无历史版本
- Watch：只能监听当前 key 的变更，无法从历史 revision 回放
- 事务：无

### 设计目标

| 能力 | 说明 |
|------|------|
| 全局 Revision | 每次写操作（Put/Delete）递增全局 revision |
| 多版本存储 | 同一 key 保留多个版本 `(revision → value)` |
| 历史查询 | 支持 `get(key, revision)` 查询历史版本 |
| Range 查询 | 支持 `range(key, end, revision)` 查询某个 revision 的快照 |
| Watch from revision | 支持从指定 revision 开始接收事件 |
| Compact | 支持手动/自动清理历史版本，释放内存 |

---

## 2. 核心数据结构

### 2.1 Revision 设计

```java
// 全局单调递增的 revision，每次写操作 +1
// 可复用 Raft 的 applied index 作为 revision
public record Revision(long revision) implements Comparable<Revision> {
    public static final Revision ZERO = new Revision(0);
    
    @Override
    public int compareTo(Revision other) {
        return Long.compare(this.revision, other.revision);
    }
}
```

**Revision 来源**：复用 Raft 的 `applied index`，因为：
- 每个已提交的 log entry 有唯一的 index
- 所有节点的 applied index 顺序一致
- 不需要额外生成全局递增 ID

### 2.2 Key-Value 版本记录

```java
// 每个版本的 KV 记录
public record KeyValue(
    String key,
    ByteString value,
    long createRevision,    // 创建时的 revision
    long modRevision,       // 最后修改时的 revision
    long version            // 该 key 的版本号（从 1 开始）
) {}

// 一个 key 的所有历史版本
public record KeyValueVersions(
    String key,
    TreeMap<Long, KeyValue> versions  // revision → KeyValue
) {}
```

### 2.3 存储结构

```java
// 内存存储（后续可替换为持久化存储）
public class MvccStore {
    // key → 所有版本，按 revision 排序
    private final ConcurrentHashMap<String, TreeMap<Long, KeyValue>> store;
    
    // 当前全局 revision
    private final AtomicLong currentRevision;
    
    // 最近 compact 的 revision
    private volatile long compactRevision;
    
    // Watcher 列表
    private final CopyOnWriteArrayList<Watcher> watchers;
}
```

---

## 3. 核心操作

### 3.1 Put 操作

```java
public PutResponse put(PutRequest req) {
    String key = req.getKey().toStringUtf8();
    ByteString value = req.getValue();
    long revision = currentRevision.incrementAndGet();
    
    // 获取该 key 当前的最新版本
    TreeMap<Long, KeyValue> versions = store.computeIfAbsent(key, k -> new TreeMap<>());
    KeyValue last = versions.lastEntry() != null ? versions.lastEntry().getValue() : null;
    
    long newVersion = (last != null) ? last.version() + 1 : 1;
    long createRevision = (last != null) ? last.createRevision() : revision;
    
    KeyValue kv = new KeyValue(key, value, createRevision, revision, newVersion);
    versions.put(revision, kv);
    
    // 发送 Watch 事件
    notifyWatchers(new WatchEvent(WatchEvent.EventType.PUT, kv, revision));
    
    return new PutResponse(revision);
}
```

### 3.2 Delete 操作

```java
public DeleteResponse delete(DeleteRequest req) {
    String key = req.getKey().toStringUtf8();
    long revision = currentRevision.incrementAndGet();
    
    TreeMap<Long, KeyValue> versions = store.get(key);
    if (versions == null || versions.isEmpty()) {
        return new DeleteResponse(revision, 0);
    }
    
    KeyValue last = versions.lastEntry().getValue();
    
    // 软删除：添加一个 tombstone 版本
    KeyValue tombstone = new KeyValue(key, ByteString.EMPTY, 
        last.createRevision(), revision, last.version() + 1);
    versions.put(revision, tombstone);
    
    // 发送 Watch 事件
    notifyWatchers(new WatchEvent(WatchEvent.EventType.DELETE, tombstone, revision));
    
    return new DeleteResponse(revision, 1);
}
```

### 3.3 Get 操作

```java
public GetResponse get(GetRequest req) {
    String key = req.getKey().toStringUtf8();
    long revision = req.getRevision(); // 0 表示当前
    
    TreeMap<Long, KeyValue> versions = store.get(key);
    if (versions == null || versions.isEmpty()) {
        return GetResponse.EMPTY;
    }
    
    // 查询指定 revision 的版本
    if (revision > 0) {
        if (revision < compactRevision) {
            throw new ErrorCompactedException();
        }
        Map.Entry<Long, KeyValue> entry = versions.floorEntry(revision);
        if (entry == null || entry.getValue().isTombstone()) {
            return GetResponse.EMPTY;
        }
        return new GetResponse(List.of(entry.getValue()), revision);
    }
    
    // 查询当前最新版本
    KeyValue latest = versions.lastEntry().getValue();
    if (latest.isTombstone()) {
        return GetResponse.EMPTY;
    }
    return new GetResponse(List.of(latest), currentRevision.get());
}
```

### 3.4 Range 操作

```java
public RangeResponse range(RangeRequest req) {
    String start = req.getStartKey().toStringUtf8();
    String end = req.getEndKey().toStringUtf8();
    long revision = req.getRevision();
    
    List<KeyValue> results = new ArrayList<>();
    long revision = currentRevision.get();
    
    for (Map.Entry<String, TreeMap<Long, KeyValue>> entry : store.entrySet()) {
        String key = entry.getKey();
        if (key.compareTo(start) >= 0 && (end.isEmpty() || key.compareTo(end) < 0)) {
            TreeMap<Long, KeyValue> versions = entry.getValue();
            Map.Entry<Long, KeyValue> kv = (revision > 0) 
                ? versions.floorEntry(revision)
                : versions.lastEntry();
            if (kv != null && !kv.getValue().isTombstone()) {
                results.add(kv.getValue());
            }
        }
    }
    
    return new RangeResponse(results, revision);
}
```

---

## 4. Watch 实现

### 4.1 Watch 数据结构

```java
public class Watcher {
    private final long watchId;
    private final String key;
    private final String rangeEnd;      // 空字符串表示单 key
    private final long startRevision;   // 从这个 revision 开始监听
    private final Consumer<WatchResponse> callback;
    private final BlockingQueue<WatchEvent> eventQueue;
    
    // 已发送到客户端的最新 revision（用于进度追踪）
    private long lastSentRevision;
}
```

### 4.2 Watch 注册与事件推送

```java
public long watch(String key, String rangeEnd, long startRevision, 
                  Consumer<WatchResponse> callback) {
    long watchId = nextWatchId.incrementAndGet();
    Watcher watcher = new Watcher(watchId, key, rangeEnd, startRevision, callback);
    watchers.add(watcher);
    
    // 如果指定了 startRevision，先推送该 revision 之后的历史事件
    if (startRevision > 0) {
        replayHistory(watcher);
    }
    
    return watchId;
}

private void replayHistory(Watcher watcher) {
    // 遍历所有 key，找到 revision > watcher.startRevision 的事件
    for (Map.Entry<String, TreeMap<Long, KeyValue>> entry : store.entrySet()) {
        TreeMap<Long, KeyValue> versions = entry.getValue();
        NavigableMap<Long, KeyValue> tail = versions.tailMap(watcher.startRevision, false);
        for (Map.Entry<Long, KeyValue> kv : tail.entrySet()) {
            WatchEvent event = createEvent(kv.getValue(), kv.getKey());
            watcher.sendEvent(event);
        }
    }
}
```

### 4.3 事件通知

```java
private void notifyWatchers(WatchEvent event) {
    for (Watcher watcher : watchers) {
        if (watcher.matches(event)) {
            watcher.sendEvent(event);
        }
    }
}
```

---

## 5. Compact 操作

### 5.1 触发方式

```java
// 手动触发
public CompactResponse compact(long revision) {
    if (revision < compactRevision) {
        throw new ErrorCompactedException();
    }
    
    int removed = 0;
    for (TreeMap<Long, KeyValue> versions : store.values()) {
        // 移除所有 revision < 指定 revision 的旧版本
        NavigableMap<Long, KeyValue> toRemove = versions.headMap(revision, false);
        removed += toRemove.size();
        toRemove.clear();
    }
    
    this.compactRevision = revision;
    return new CompactResponse(revision, removed);
}

// 自动 compact（基于保留版本数或时间）
@Scheduled(every = "1m")
void autoCompact() {
    long current = currentRevision.get();
    long compactRevision = current - MAX_REVISIONS_TO_KEEP;
    if (compactRevision > this.compactRevision) {
        compact(compactRevision);
    }
}
```

### 5.2 Compact 后的处理

当客户端请求一个已被 compact 的 revision 时：

```java
if (revision < compactRevision) {
    throw new ErrorCompactedException("revision " + revision + " has been compacted");
}
```

---

## 6. 事务支持（If/Then/Else）

### 6.1 事务请求结构

```java
public record TxnRequest(
    List<Compare> compares,   // If 条件
    List<Op> successOps,      // Then 操作
    List<Op> failureOps       // Else 操作
) {}

public record Compare(
    CompareTarget target,     // KEY / MOD_REVISION / CREATE_REVISION / VERSION / VALUE
    CompareResult result,     // EQUAL / GREATER / LESS
    ByteString key,
    ByteString value          // 比较值
) {}
```

### 6.2 事务执行

```java
public TxnResponse txn(TxnRequest req) {
    long revision = currentRevision.incrementAndGet();
    
    // 评估条件
    boolean conditionMet = evaluateCompares(req.compares());
    
    List<Op> ops = conditionMet ? req.successOps() : req.failureOps();
    List<Long> executedRevisions = new ArrayList<>();
    
    for (Op op : ops) {
        long opRevision = executeOp(op, revision);
        executedRevisions.add(opRevision);
    }
    
    return new TxnResponse(conditionMet, executedRevisions);
}

private boolean evaluateCompares(List<Compare> compares) {
    for (Compare compare : compares) {
        KeyValue kv = getKeyValue(compare.key());
        if (!compare.evaluate(kv)) {
            return false;
        }
    }
    return true;
}
```

---

## 7. API 设计

### 7.1 Proto 定义

```protobuf
service Coordinator {
    // MVCC KV 操作
    rpc Put(PutRequest) returns (PutResponse);
    rpc Get(GetRequest) returns (GetResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc Range(RangeRequest) returns (RangeResponse);
    
    // 事务
    rpc Txn(TxnRequest) returns (TxnResponse);
    
    // Watch
    rpc Watch(WatchRequest) returns (stream WatchResponse);
    
    // Compact
    rpc Compact(CompactRequest) returns (CompactResponse);
}

message PutRequest {
    bytes key = 1;
    bytes value = 2;
    int64 lease = 3;  // 可选：绑定 lease
}

message PutResponse {
    int64 revision = 1;
}

message GetRequest {
    bytes key = 1;
    int64 revision = 2;  // 0 表示当前版本
}

message GetResponse {
    repeated KeyValue kvs = 1;
    int64 revision = 2;
}

message RangeRequest {
    bytes key = 1;
    bytes range_end = 2;
    int64 revision = 3;  // 0 表示当前快照
}

message RangeResponse {
    repeated KeyValue kvs = 1;
    int64 revision = 2;
}

message DeleteResponse {
    int64 revision = 1;
    int64 deleted = 2;  // 删除的 key 数量
}

message KeyValue {
    bytes key = 1;
    bytes value = 2;
    int64 create_revision = 3;
    int64 mod_revision = 4;
    int64 version = 5;
}

// Watch
message WatchRequest {
    oneof request_union {
        WatchCreateRequest create_request = 1;
        WatchCancelRequest cancel_request = 2;
    }
}

message WatchCreateRequest {
    bytes key = 1;
    bytes range_end = 2;
    int64 start_revision = 3;
    bool progress_notify = 4;
}

message WatchCancelRequest {
    int64 watch_id = 1;
}

message WatchResponse {
    ResponseHeader header = 1;
    int64 watch_id = 2;
    repeated WatchEvent events = 3;
}

message WatchEvent {
    enum EventType {
        PUT = 0;
        DELETE = 1;
    }
    EventType type = 1;
    KeyValue kv = 2;
}

message CompactRequest {
    int64 revision = 1;
}

message CompactResponse {
    int64 revision = 1;
    int64 removed_count = 2;
}
```

---

## 8. 与 Raft 的集成

### 8.1 Revision 生成

```
客户端请求 → Coordinator → Raft Propose(data) → 
    Leader 应用 → ApplyCommand{index, term, data} → 
    StateMachineDriver.applyEntry(index, data)
```

**关键点**：`index` 就是 revision，直接复用 Raft 的 applied index。

### 8.2 修改 StateMachineDriver

```java
private void applyEntry(CommittedEntry entry) {
    long revision = entry.getIndex();  // revision = Raft index
    
    byte[] data = entry.getData().toByteArray();
    if (data.length == 0) {
        lastApplied.set(revision);
        return;
    }
    
    byte opType = data[0];
    byte[] payload = Arrays.copyOfRange(data, 1, data.length);
    
    switch (opType) {
        case OP_PUT -> {
            PutRequest req = PutRequest.parseFrom(payload);
            mvccStore.put(req, revision);  // 传入 revision
        }
        case OP_DELETE -> {
            DeleteRequest req = DeleteRequest.parseFrom(payload);
            mvccStore.delete(req, revision);
        }
        // ...
    }
    
    lastApplied.set(revision);
}
```

---

## 9. 存储优化

### 9.1 内存存储（当前阶段）

```
ConcurrentHashMap<String, TreeMap<Long, KeyValue>>
├── "key1" → {1: v1, 3: v2, 5: v3}
├── "key2" → {2: v1, 4: v2}
└── "key3" → {6: v1}
```

### 9.2 持久化（后续优化）

- 使用 RocksDB/BadgerDB 作为后端
- Key 格式：`{key}/{revision}` → `value`
- 支持 prefix scan 和 range scan

### 9.3 历史版本保留策略

| 策略 | 说明 |
|------|------|
| 按数量 | 保留最近 N 个版本 |
| 按时间 | 保留最近 T 天的版本 |
| 按空间 | 限制历史版本占用的最大内存 |
| 手动 | 调用 Compact API |

---

## 10. 性能考虑

### 10.1 写放大

每个 Put/Delete 都会创建新版本，历史版本会持续增长。

**对策**：
- 自动/手动 Compact
- 限制历史版本数量
- 使用更紧凑的存储格式

### 10.2 Range 查询性能

当前实现遍历所有 key，O(N) 复杂度。

**对策**：
- 使用跳表或 B+Tree 索引
- 分片存储
- 缓存热点 Range 结果

### 10.3 Watch 事件丢失

如果 watcher 事件队列满了，可能丢事件。

**对策**：
- 使用无界队列 + 背压（限流慢消费者）
- 通知客户端重新订阅
- 实现 WatchProgress 机制

---

## 11. 测试计划

### 11.1 单元测试

- [ ] Put/Get/Delete 基本操作
- [ ] 多版本读写
- [ ] 历史查询
- [ ] Range 查询
- [ ] Compact 操作
- [ ] 事务 If/Then/Else
- [ ] Watch 事件通知
- [ ] Watch 历史回放

### 11.2 集成测试

- [ ] 多节点一致性
- [ ] Leader 切换后数据一致性
- [ ] 节点重启后数据恢复
- [ ] 大量历史版本下的性能

### 11.3 压力测试

- [ ] 高并发写入
- [ ] 大量 Watcher 并发
- [ ] Range 查询性能

---

## 12. 实现顺序

### Phase 1：基础 MVCC（2-3天）

1. 修改 KvStore，支持多版本存储
2. 实现 Put/Get/Delete 操作
3. 修改 StateMachineDriver，使用 revision
4. 基本测试

### Phase 2：Watch + Range（1-2天）

1. 实现 Watch 历史回放
2. 实现 Range 查询
3. 修复 Watch 事件推送

### Phase 3：Compact（1天）

1. 实现手动 Compact
2. 实现自动 Compact（基于数量/时间）
3. 测试 Compact 后的查询行为

### Phase 4：事务（2-3天）

1. 实现 Compare 评估
2. 实现 Txn 操作
3. 测试原子性

### Phase 5：优化（持续）

1. 持久化存储
2. 性能优化
3. 监控指标

---

## 13. 参考

- etcd MVCC 设计：https://etcd.io/docs/v3.4/dev-guide/architecture/
- CockroachDB MVCC：https://www.cockroachlabs.com/docs/stable/architecture/storage-layer.html
- TiKV MVCC：https://tikv.org/deep-dive/mvcc/
