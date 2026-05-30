# MVCC + Revision 详细设计

> 基于当前代码结构的详细实现方案

---

## 1. 项目结构变更

### 1.1 当前结构

```
coordinator/src/main/java/engine/
├── client/
│   ├── RaftLogClient.java      # C++ Raft 客户端
│   └── EngineClient.java       # 对外 SDK
├── coordinator/
│   ├── CoordinatorGrpcService.java  # gRPC 服务实现
│   ├── KvStore.java                 # KV 存储（只存最新版本）
│   ├── WatchManager.java            # Watch 管理
│   ├── LeaseManager.java            # Lease 管理
│   └── StateMachineDriver.java      # 状态机驱动
└── health/
    └── CoordinatorHealthCheck.java   # 健康检查
```

### 1.2 变更后结构

```
coordinator/src/main/java/engine/
├── client/
│   ├── RaftLogClient.java      # 不变
│   └── EngineClient.java       # 不变
├── coordinator/
│   ├── CoordinatorGrpcService.java  # 需修改：添加 Txn/Compact 接口
│   ├── KvStore.java                 # 需重构：委托给 MvccStore
│   ├── WatchManager.java            # 需修改：支持历史回放
│   ├── LeaseManager.java            # 不变
│   └── StateMachineDriver.java      # 需修改：添加新操作码
├── mvcc/                              # 新增目录
│   ├── MvccStore.java               # MVCC 核心存储
│   ├── Revision.java                # Revision 类
│   ├── VersionedKeyValue.java       # 多版本 KV 记录
│   ├── TxnManager.java              # 事务管理器
│   ├── CompactManager.java          # Compact 管理器
│   └── HistoryIndex.java            # 历史版本索引
└── health/
    └── CoordinatorHealthCheck.java   # 不变
```

---

## 2. 文件职责与实现

### 2.1 新增文件：`mvcc/Revision.java`

**职责**：封装全局递增的 revision

```java
package engine.coordinator.mvcc;

/**
 * 全局单调递增的 revision，复用 Raft 的 applied index。
 */
public record Revision(long revision) implements Comparable<Revision> {
    
    public static final Revision ZERO = new Revision(0);
    
    public Revision next() {
        return new Revision(revision + 1);
    }
    
    public boolean isNewerThan(Revision other) {
        return this.revision > other.revision;
    }
    
    @Override
    public int compareTo(Revision other) {
        return Long.compare(this.revision, other.revision);
    }
    
    @Override
    public String toString() {
        return "Rev(" + revision + ")";
    }
}
```

---

### 2.2 新增文件：`mvcc/VersionedKeyValue.java`

**职责**：存储一个 key 的所有历史版本

```java
package engine.coordinator.mvcc;

import com.google.protobuf.ByteString;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 单个 key 的多版本存储。
 * 使用 TreeMap 按 revision 排序，支持范围查询。
 */
public class VersionedKeyValue {
    
    private final String key;
    private final TreeMap<Long, KvEntry> versions;  // revision → entry
    
    public record KvEntry(
        ByteString value,
        long createRevision,
        long modRevision,
        long version,
        long lease
    ) {
        public boolean isTombstone() {
            return value.isEmpty();
        }
    }
    
    public VersionedKeyValue(String key) {
        this.key = key;
        this.versions = new TreeMap<>();
    }
    
    public String key() { return key; }
    
    /**
     * 添加新版本
     */
    public void put(ByteString value, long revision, long lease) {
        KvEntry last = latest();
        long createRevision = (last != null) ? last.createRevision() : revision;
        long newVersion = (last != null) ? last.version() + 1 : 1;
        
        KvEntry entry = new KvEntry(value, createRevision, revision, newVersion, lease);
        versions.put(revision, entry);
    }
    
    /**
     * 添加 tombstone（软删除）
     */
    public void tombstone(long revision) {
        KvEntry last = latest();
        if (last == null || last.isTombstone()) return;
        
        KvEntry tomb = new KvEntry(
            ByteString.EMPTY,
            last.createRevision(),
            revision,
            last.version() + 1,
            0
        );
        versions.put(revision, tomb);
    }
    
    /**
     * 获取指定 revision 的版本
     */
    public KvEntry getAtRevision(long revision) {
        // floorEntry 返回 <= revision 的最大 entry
        var entry = versions.floorEntry(revision);
        if (entry == null) return null;
        return entry.getValue().isTombstone() ? null : entry.getValue();
    }
    
    /**
     * 获取最新版本
     */
    public KvEntry latest() {
        var entry = versions.lastEntry();
        if (entry == null) return null;
        return entry.getValue().isTombstone() ? null : entry.getValue();
    }
    
    /**
     * 获取 [fromRevision, toRevision] 之间的所有版本
     */
    public NavigableMap<Long, KvEntry> getVersionRange(long fromRevision, long toRevision) {
        return versions.subMap(fromRevision, true, toRevision, true);
    }
    
    /**
     * 获取 >= fromRevision 的所有版本（用于 Watch 历史回放）
     */
    public NavigableMap<Long, KvEntry> getVersionsAfter(long fromRevision) {
        return versions.tailMap(fromRevision, false);
    }
    
    /**
     * 清理指定 revision 之前的旧版本
     */
    public int compact(long beforeRevision) {
        NavigableMap<Long, KvEntry> toRemove = versions.headMap(beforeRevision, false);
        int count = toRemove.size();
        toRemove.clear();
        return count;
    }
    
    /**
     * 版本数量
     */
    public int versionCount() {
        return versions.size();
    }
}
```

---

### 2.3 新增文件：`mvcc/MvccStore.java`

**职责**：MVCC 核心存储，管理所有 key 的多版本数据

```java
package engine.coordinator.mvcc;

import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * MVCC 存储引擎。
 * 
 * 核心数据结构：
 * - store: ConcurrentHashMap<String, VersionedKeyValue>
 * - currentRevision: AtomicLong（复用 Raft applied index）
 * - compactRevision: volatile long（已 compact 的 revision）
 */
@ApplicationScoped
public class MvccStore {
    
    private static final Logger LOG = Logger.getLogger(MvccStore.class);
    
    // ── 事件类型 ─────────────────────────────────────────────────────────────
    public enum EventType { PUT, DELETE }
    
    public record WatchEvent(
        EventType type,
        String key,
        long revision,
        VersionedKeyValue.KvEntry kv
    ) {}
    
    // ── 核心存储 ─────────────────────────────────────────────────────────────
    private final ConcurrentHashMap<String, VersionedKeyValue> store = new ConcurrentHashMap<>();
    private long currentRevision = 0;
    private long compactRevision = 0;
    
    // ── Watcher 列表 ─────────────────────────────────────────────────────────
    private final CopyOnWriteArrayList<Consumer<WatchEvent>> watchers = new CopyOnWriteArrayList<>();
    
    // ── 读写锁 ─────────────────────────────────────────────────────────────
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    
    // ── Put 操作 ─────────────────────────────────────────────────────────────
    
    /**
     * 写入 key-value
     * @return 该操作的 revision
     */
    public long put(String key, ByteString value, long lease) {
        rwLock.writeLock().lock();
        try {
            currentRevision++;
            
            VersionedKeyValue vkv = store.computeIfAbsent(key, VersionedKeyValue::new);
            vkv.put(value, currentRevision, lease);
            
            // 通知 watcher
            notifyWatchers(EventType.PUT, key, currentRevision, vkv.latest());
            
            return currentRevision;
        } finally {
            rwLock.writeLock().unlock();
        }
    }
    
    /**
     * 写入 key-value（无 lease）
     */
    public long put(String key, ByteString value) {
        return put(key, value, 0);
    }
    
    // ── Delete 操作 ──────────────────────────────────────────────────────────
    
    /**
     * 删除单个 key
     * @return 该操作的 revision，如果 key 不存在返回 -1
     */
    public long delete(String key) {
        rwLock.writeLock().lock();
        try {
            VersionedKeyValue vkv = store.get(key);
            if (vkv == null || vkv.latest() == null) {
                return -1;
            }
            
            currentRevision++;
            vkv.tombstone(currentRevision);
            
            // 通知 watcher
            notifyWatchers(EventType.DELETE, key, currentRevision, vkv.latest());
            
            return currentRevision;
        } finally {
            rwLock.writeLock().unlock();
        }
    }
    
    /**
     * 范围删除
     * @return 删除的 key 数量
     */
    public int deleteRange(String startKey, String endKey) {
        rwLock.writeLock().lock();
        try {
            int count = 0;
            for (String key : store.keySet()) {
                if (key.compareTo(startKey) >= 0 && 
                    (endKey.isEmpty() || key.compareTo(endKey) < 0)) {
                    if (delete(key) > 0) {
                        count++;
                    }
                }
            }
            return count;
        } finally {
            rwLock.writeLock().unlock();
        }
    }
    
    // ── Get 操作 ─────────────────────────────────────────────────────────────
    
    /**
     * 获取指定 revision 的 key-value
     */
    public Optional<VersionedKeyValue.KvEntry> get(String key, long revision) {
        rwLock.readLock().lock();
        try {
            if (revision < compactRevision) {
                throw new IllegalStateException("revision " + revision + " has been compacted");
            }
            
            VersionedKeyValue vkv = store.get(key);
            if (vkv == null) return Optional.empty();
            
            VersionedKeyValue.KvEntry entry = (revision > 0) 
                ? vkv.getAtRevision(revision) 
                : vkv.latest();
            
            return Optional.ofNullable(entry);
        } finally {
            rwLock.readLock().unlock();
        }
    }
    
    /**
     * 获取当前版本
     */
    public Optional<VersionedKeyValue.KvEntry> get(String key) {
        return get(key, 0);
    }
    
    // ── Range 操作 ───────────────────────────────────────────────────────────
    
    public record RangeResult(
        List<RangeEntry> entries,
        long revision,
        boolean more
    ) {}
    
    public record RangeEntry(
        String key,
        VersionedKeyValue.KvEntry kv
    ) {}
    
    /**
     * 范围查询
     */
    public RangeResult range(String startKey, String endKey, long revision, long limit) {
        rwLock.readLock().lock();
        try {
            if (revision < compactRevision) {
                throw new IllegalStateException("revision " + revision + " has been compacted");
            }
            
            List<RangeEntry> entries = new ArrayList<>();
            long queryRevision = (revision > 0) ? revision : currentRevision;
            
            for (var entry : store.entrySet()) {
                String key = entry.getKey();
                if (key.compareTo(startKey) < 0) continue;
                if (!endKey.isEmpty() && key.compareTo(endKey) >= 0) continue;
                
                VersionedKeyValue.KvEntry kv = entry.getValue().getAtRevision(queryRevision);
                if (kv != null) {
                    entries.add(new RangeEntry(key, kv));
                    if (limit > 0 && entries.size() >= limit) {
                        return new RangeResult(entries, queryRevision, true);
                    }
                }
            }
            
            return new RangeResult(entries, queryRevision, false);
        } finally {
            rwLock.readLock().unlock();
        }
    }
    
    // ── Watch 历史回放 ──────────────────────────────────────────────────────
    
    /**
     * 获取某个 key 在 [fromRevision, toRevision] 之间的所有事件
     */
    public List<WatchEvent> getHistoryEvents(String key, long fromRevision, long toRevision) {
        rwLock.readLock().lock();
        try {
            List<WatchEvent> events = new ArrayList<>();
            VersionedKeyValue vkv = store.get(key);
            if (vkv == null) return events;
            
            var versions = vkv.getVersionRange(fromRevision, toRevision);
            for (var entry : versions.entrySet()) {
                EventType type = entry.getValue().isTombstone() ? EventType.DELETE : EventType.PUT;
                events.add(new WatchEvent(type, key, entry.getKey(), entry.getValue()));
            }
            
            return events;
        } finally {
            rwLock.readLock().unlock();
        }
    }
    
    /**
     * 获取所有 key 在 [fromRevision, toRevision] 之间的所有事件（用于 Watch 回放）
     */
    public List<WatchEvent> getAllHistoryEvents(long fromRevision, long toRevision) {
        rwLock.readLock().lock();
        try {
            List<WatchEvent> events = new ArrayList<>();
            
            for (var vkvEntry : store.entrySet()) {
                String key = vkvEntry.getKey();
                var versions = vkvEntry.getValue().getVersionRange(fromRevision, toRevision);
                for (var entry : versions.entrySet()) {
                    EventType type = entry.getValue().isTombstone() ? EventType.DELETE : EventType.PUT;
                    events.add(new WatchEvent(type, key, entry.getKey(), entry.getValue()));
                }
            }
            
            // 按 revision 排序
            events.sort(Comparator.comparingLong(WatchEvent::revision));
            return events;
        } finally {
            rwLock.readLock().unlock();
        }
    }
    
    // ── Watcher 管理 ─────────────────────────────────────────────────────────
    
    public void addWatcher(Consumer<WatchEvent> watcher) {
        watchers.add(watcher);
    }
    
    public void removeWatcher(Consumer<WatchEvent> watcher) {
        watchers.remove(watcher);
    }
    
    private void notifyWatchers(EventType type, String key, long revision, 
                                VersionedKeyValue.KvEntry kv) {
        WatchEvent event = new WatchEvent(type, key, revision, kv);
        for (var watcher : watchers) {
            try {
                watcher.accept(event);
            } catch (Exception e) {
                LOG.warnf("Watcher notification failed: %s", e.getMessage());
            }
        }
    }
    
    // ── Compact ──────────────────────────────────────────────────────────────
    
    public long compactRevision() {
        return compactRevision;
    }
    
    public long currentRevision() {
        rwLock.readLock().lock();
        try { return currentRevision; }
        finally { rwLock.readLock().unlock(); }
    }
    
    /**
     * 设置当前 revision（从 Raft applied index 恢复）
     */
    public void setCurrentRevision(long revision) {
        rwLock.writeLock().lock();
        try { this.currentRevision = revision; }
        finally { rwLock.writeLock().unlock(); }
    }
    
    // ── Snapshot / Restore ───────────────────────────────────────────────────
    
    public byte[] snapshot() {
        rwLock.readLock().lock();
        try {
            // 返回当前快照（只包含最新版本）
            var result = range("\0", "", 0, 0);
            // 序列化为 protobuf
            return serializeSnapshot(result);
        } finally {
            rwLock.readLock().unlock();
        }
    }
    
    public void restore(byte[] data) {
        rwLock.writeLock().lock();
        try {
            store.clear();
            // 反序列化并恢复
            deserializeSnapshot(data);
        } finally {
            rwLock.writeLock().unlock();
        }
    }
    
    private byte[] serializeSnapshot(RangeResult result) {
        // TODO: 实现 protobuf 序列化
        return new byte[0];
    }
    
    private void deserializeSnapshot(byte[] data) {
        // TODO: 实现 protobuf 反序列化
    }
}
```

---

### 2.4 新增文件：`mvcc/TxnManager.java`

**职责**：事务管理，支持 If/Then/Else 原子操作

```java
package engine.coordinator.mvcc;

import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * 事务管理器。
 * 支持条件判断和原子操作。
 */
@ApplicationScoped
public class TxnManager {
    
    private static final Logger LOG = Logger.getLogger(TxnManager.class);
    
    @Inject MvccStore mvccStore;
    
    // ── 比较操作 ─────────────────────────────────────────────────────────────
    
    public enum CompareTarget {
        KEY, MOD_REVISION, CREATE_REVISION, VERSION, VALUE
    }
    
    public enum CompareResult {
        EQUAL, GREATER, LESS
    }
    
    public record Compare(
        CompareTarget target,
        CompareResult result,
        String key,
        ByteString value
    ) {}
    
    // ── 操作类型 ─────────────────────────────────────────────────────────────
    
    public enum OpType { PUT, DELETE, RANGE }
    
    public record Op(
        OpType type,
        String key,
        ByteString value,
        String rangeEnd,
        long lease
    ) {}
    
    // ── 事务请求/响应 ──────────────────────────────────────────────────────
    
    public record TxnRequest(
        List<Compare> compares,
        List<Op> successOps,
        List<Op> failureOps
    ) {}
    
    public record TxnResponse(
        boolean succeeded,
        List<Long> revisions
    ) {}
    
    // ── 执行事务 ─────────────────────────────────────────────────────────────
    
    public TxnResponse execute(TxnRequest request) {
        // 评估条件
        boolean conditionMet = evaluateCompares(request.compares());
        
        // 选择操作列表
        List<Op> ops = conditionMet ? request.successOps() : request.failureOps();
        
        // 执行操作
        List<Long> revisions = new ArrayList<>();
        for (Op op : ops) {
            long rev = executeOp(op);
            revisions.add(rev);
        }
        
        LOG.debugf("Txn executed: succeeded=%d, ops=%d", conditionMet, ops.size());
        return new TxnResponse(conditionMet, revisions);
    }
    
    /**
     * 评估所有比较条件
     */
    private boolean evaluateCompares(List<Compare> compares) {
        for (Compare compare : compares) {
            if (!evaluateCompare(compare)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * 评估单个比较条件
     */
    private boolean evaluateCompare(Compare compare) {
        var kvOpt = mvccStore.get(compare.key());
        
        return switch (compare.target()) {
            case KEY -> {
                // KEY 比较：检查 key 是否存在
                boolean exists = kvOpt.isPresent();
                yield compare.result() == CompareResult.EQUAL ? exists : !exists;
            }
            case VALUE -> {
                // VALUE 比较
                if (kvOpt.isEmpty()) yield compare.result() != CompareResult.EQUAL;
                ByteString actual = kvOpt.get().value();
                yield compareValues(actual, compare.value(), compare.result());
            }
            case MOD_REVISION -> {
                // MOD_REVISION 比较
                if (kvOpt.isEmpty()) yield compare.result() != CompareResult.EQUAL;
                long actual = kvOpt.get().modRevision();
                long expected = Long.parseLong(compare.value().toStringUtf8());
                yield compareNumbers(actual, expected, compare.result());
            }
            case CREATE_REVISION -> {
                // CREATE_REVISION 比较
                if (kvOpt.isEmpty()) yield compare.result() != CompareResult.EQUAL;
                long actual = kvOpt.get().createRevision();
                long expected = Long.parseLong(compare.value().toStringUtf8());
                yield compareNumbers(actual, expected, compare.result());
            }
            case VERSION -> {
                // VERSION 比较
                if (kvOpt.isEmpty()) yield compare.result() != CompareResult.EQUAL;
                long actual = kvOpt.get().version();
                long expected = Long.parseLong(compare.value().toStringUtf8());
                yield compareNumbers(actual, expected, compare.result());
            }
        };
    }
    
    private boolean compareValues(ByteString actual, ByteString expected, CompareResult result) {
        int cmp = actual.compareTo(expected);
        return switch (result) {
            case EQUAL -> cmp == 0;
            case GREATER -> cmp > 0;
            case LESS -> cmp < 0;
        };
    }
    
    private boolean compareNumbers(long actual, long expected, CompareResult result) {
        return switch (result) {
            case EQUAL -> actual == expected;
            case GREATER -> actual > expected;
            case LESS -> actual < expected;
        };
    }
    
    /**
     * 执行单个操作
     */
    private long executeOp(Op op) {
        return switch (op.type()) {
            case PUT -> mvccStore.put(op.key(), op.value(), op.lease());
            case DELETE -> {
                if (op.rangeEnd().isEmpty()) {
                    yield mvccStore.delete(op.key());
                } else {
                    yield mvccStore.deleteRange(op.key(), op.rangeEnd());
                }
            }
            case RANGE -> {
                // Range 是只读操作，返回当前 revision
                yield mvccStore.currentRevision();
            }
        };
    }
}
```

---

### 2.5 新增文件：`mvcc/CompactManager.java`

**职责**：管理历史版本的清理

```java
package engine.coordinator.mvcc;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Compact 管理器。
 * 支持手动和自动清理历史版本。
 */
@ApplicationScoped
public class CompactManager {
    
    private static final Logger LOG = Logger.getLogger(CompactManager.class);
    
    // 配置参数
    private static final long MAX_REVISIONS_TO_KEEP = 100_000;
    private static final long AUTO_COMPACT_INTERVAL_MS = 60_000; // 1 分钟
    
    @Inject MvccStore mvccStore;
    
    // ── 手动 Compact ─────────────────────────────────────────────────────────
    
    public record CompactResponse(
        long compactedRevision,
        int removedVersions
    ) {}
    
    /**
     * 手动触发 compact
     */
    public CompactResponse compact(long revision) {
        if (revision < mvccStore.compactRevision()) {
            throw new IllegalStateException(
                "cannot compact revision " + revision + 
                ", already compacted to " + mvccStore.compactRevision()
            );
        }
        
        if (revision > mvccStore.currentRevision()) {
            throw new IllegalStateException(
                "cannot compact revision " + revision + 
                ", current revision is " + mvccStore.currentRevision()
            );
        }
        
        LOG.infof("Compacting to revision %d", revision);
        
        // 执行 compact（MvccStore 内部实现）
        int removed = mvccStore.compact(revision);
        
        LOG.infof("Compacted: removed %d old versions", removed);
        return new CompactResponse(revision, removed);
    }
    
    // ── 自动 Compact ─────────────────────────────────────────────────────────
    
    @Scheduled(every = "1m")
    void autoCompact() {
        long current = mvccStore.currentRevision();
        long compactTo = current - MAX_REVISIONS_TO_KEEP;
        
        if (compactTo > mvccStore.compactRevision()) {
            LOG.debugf("Auto-compacting to revision %d", compactTo);
            compact(compactTo);
        }
    }
    
    // ── 查询 ─────────────────────────────────────────────────────────────────
    
    public long getCompactRevision() {
        return mvccStore.compactRevision();
    }
    
    public long getCurrentRevision() {
        return mvccStore.currentRevision();
    }
}
```

---

### 2.6 修改文件：`coordinator/KvStore.java`

**职责变更**：从直接存储改为委托给 MvccStore

```java
package engine.coordinator;

import engine.coordinator.mvcc.MvccStore;
import engine.coordinator.mvcc.VersionedKeyValue;
import engine.coordinator.v1.CoordinatorOuterClass.*;
import com.google.protobuf.ByteString;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * KV 存储服务层。
 * 委托给 MvccStore 实现 MVCC 语义。
 */
@ApplicationScoped
public class KvStore {
    
    private static final Logger LOG = Logger.getLogger(KvStore.class);
    
    @Inject MvccStore mvccStore;
    
    // ── Watch 事件 ───────────────────────────────────────────────────────────
    
    public record WatchEvent(MvccStore.EventType type, String key, KeyValue kv, long revision) {}
    
    private final List<java.util.function.Consumer<WatchEvent>> listeners = new ArrayList<>();
    
    @PostConstruct
    void init() {
        // 注册到 MvccStore 的 watcher
        mvccStore.addWatcher(this::onMvccEvent);
    }
    
    public void addListener(java.util.function.Consumer<WatchEvent> listener) {
        listeners.add(listener);
    }
    
    private void onMvccEvent(MvccStore.WatchEvent event) {
        // 转换为 KvStore 的 WatchEvent
        KeyValue kv = KeyValue.newBuilder()
            .setKey(ByteString.copyFromUtf8(event.key()))
            .setValue(event.kv() != null ? event.kv().value() : ByteString.EMPTY)
            .setCreateRevision(event.kv() != null ? event.kv().createRevision() : 0)
            .setModRevision(event.kv() != null ? event.kv().modRevision() : 0)
            .setVersion(event.kv() != null ? event.kv().version() : 0)
            .build();
        
        WatchEvent watchEvent = new WatchEvent(event.type(), event.key(), kv, event.revision());
        
        for (var listener : listeners) {
            try {
                listener.accept(watchEvent);
            } catch (Exception e) {
                LOG.warnf("WatchEvent listener error: %s", e.getMessage());
            }
        }
    }
    
    // ── Apply operations ─────────────────────────────────────────────────────
    
    public void applyPut(PutRequest req) {
        mvccStore.put(
            req.getKey().toStringUtf8(),
            req.getValue(),
            req.getLease()
        );
    }
    
    public void applyDelete(DeleteRequest req) {
        String key = req.getKey().toStringUtf8();
        String rangeEnd = req.getRangeEnd().toStringUtf8();
        
        if (rangeEnd.isEmpty()) {
            mvccStore.delete(key);
        } else {
            mvccStore.deleteRange(key, rangeEnd);
        }
    }
    
    // ── Read operations ──────────────────────────────────────────────────────
    
    public GetResponse get(GetRequest req) {
        String key = req.getKey().toStringUtf8();
        String rangeEnd = req.getRangeEnd().toStringUtf8();
        long revision = req.getRevision();
        long limit = req.getLimit();
        
        ResponseHeader.Builder header = ResponseHeader.newBuilder()
            .setRevision(mvccStore.currentRevision());
        
        GetResponse.Builder resp = GetResponse.newBuilder()
            .setHeader(header);
        
        if (rangeEnd.isEmpty()) {
            // 单 key 查询
            var kvOpt = mvccStore.get(key, revision);
            if (kvOpt.isPresent()) {
                resp.addKvs(toProto(key, kvOpt.get()));
                resp.setCount(1);
            }
        } else {
            // 范围查询
            var result = mvccStore.range(key, rangeEnd, revision, limit);
            for (var entry : result.entries()) {
                resp.addKvs(toProto(entry.key(), entry.kv()));
            }
            resp.setCount(result.entries().size());
            resp.setMore(result.more());
        }
        
        return resp.build();
    }
    
    // ── Snapshot / Restore ───────────────────────────────────────────────────
    
    public byte[] snapshot() {
        return mvccStore.snapshot();
    }
    
    public void restore(byte[] data) {
        mvccStore.restore(data);
    }
    
    public long revision() {
        return mvccStore.currentRevision();
    }
    
    // ── Internal ─────────────────────────────────────────────────────────────
    
    private KeyValue toProto(String key, VersionedKeyValue.KvEntry mv) {
        return KeyValue.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setValue(mv.value())
            .setCreateRevision(mv.createRevision())
            .setModRevision(mv.modRevision())
            .setVersion(mv.version())
            .setLease(mv.lease())
            .build();
    }
}
```

---

### 2.7 修改文件：`coordinator/WatchManager.java`

**职责变更**：支持从指定 revision 开始 Watch

```java
package engine.coordinator;

import engine.coordinator.mvcc.MvccStore;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Watch 管理器。
 * 支持从指定 revision 开始监听事件。
 */
@ApplicationScoped
public class WatchManager {
    
    private static final Logger LOG = Logger.getLogger(WatchManager.class);
    private static final long WATCH_TTL_SECONDS = 300;
    
    @Inject KvStore kvStore;
    @Inject MvccStore mvccStore;
    
    private final Map<Long, WatchEntry> watches = new ConcurrentHashMap<>();
    private final AtomicLong nextId = new AtomicLong(1);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    record WatchEntry(
        long id,
        String key,
        String rangeEnd,
        long startRevision,
        Consumer<WatchResponse> callback,
        Instant createdAt
    ) {}
    
    @PostConstruct
    void init() {
        kvStore.addListener(this::onEvent);
        scheduler.scheduleAtFixedRate(this::cleanupExpiredWatches, 60, 60, TimeUnit.SECONDS);
    }
    
    // ── Registration ──────────────────────────────────────────────────────────
    
    public long register(String key, String rangeEnd, long startRevision,
                         Consumer<WatchResponse> callback) {
        long id = nextId.getAndIncrement();
        watches.put(id, new WatchEntry(id, key, rangeEnd, startRevision, callback, Instant.now()));
        
        // 如果指定了 startRevision，先回放历史事件
        if (startRevision > 0) {
            replayHistory(id, key, rangeEnd, startRevision);
        }
        
        LOG.debugf("Watch registered id=%d key=%s rangeEnd=%s startRevision=%d", 
            id, key, rangeEnd, startRevision);
        return id;
    }
    
    public void cancel(long watchId) {
        watches.remove(watchId);
        LOG.debugf("Watch cancelled id=%d", watchId);
    }
    
    // ── 历史回放 ─────────────────────────────────────────────────────────────
    
    private void replayHistory(long watchId, String key, String rangeEnd, long startRevision) {
        WatchEntry entry = watches.get(watchId);
        if (entry == null) return;
        
        long currentRev = mvccStore.currentRevision();
        
        // 获取历史事件
        var events = mvccStore.getAllHistoryEvents(startRevision, currentRev);
        
        for (var event : events) {
            if (!matches(entry, event.key())) continue;
            
            WatchResponse resp = WatchResponse.newBuilder()
                .setWatchId(watchId)
                .setHeader(ResponseHeader.newBuilder().setRevision(event.revision()))
                .addEvents(Event.newBuilder()
                    .setTypeValue(event.type().getNumber())
                    .setKv(toProto(event)))
                .build();
            
            try {
                entry.callback().accept(resp);
            } catch (Exception e) {
                LOG.warnf("Watch replay delivery failed: %s", e.getMessage());
            }
        }
    }
    
    // ── Event handler ────────────────────────────────────────────────────────
    
    private void onEvent(KvStore.WatchEvent event) {
        watches.values().forEach(w -> {
            if (event.revision() < w.startRevision()) return;
            if (!matches(w, event.key())) return;
            
            WatchResponse resp = WatchResponse.newBuilder()
                .setWatchId(w.id())
                .setHeader(ResponseHeader.newBuilder().setRevision(event.revision()))
                .addEvents(Event.newBuilder()
                    .setTypeValue(event.type().getNumber())
                    .setKv(event.kv()))
                .build();
            
            try {
                w.callback().accept(resp);
            } catch (Exception e) {
                LOG.warnf("Watch delivery failed, removing: %s", e.getMessage());
                watches.remove(w.id());
            }
        });
    }
    
    private boolean matches(WatchEntry w, String key) {
        if (w.rangeEnd() == null || w.rangeEnd().isEmpty()) {
            return key.equals(w.key());
        }
        if ("\0".equals(w.rangeEnd())) {
            return key.compareTo(w.key()) >= 0;
        }
        return key.compareTo(w.key()) >= 0 && key.compareTo(w.rangeEnd()) < 0;
    }
    
    // ── Cleanup ──────────────────────────────────────────────────────────────
    
    private void cleanupExpiredWatches() {
        Instant now = Instant.now();
        watches.entrySet().removeIf(entry -> {
            if (now.isAfter(entry.getValue().createdAt().plusSeconds(WATCH_TTL_SECONDS))) {
                LOG.debugf("Watch expired id=%d key=%s", entry.getKey(), entry.getValue().key());
                return true;
            }
            return false;
        });
    }
    
    // ── Internal ─────────────────────────────────────────────────────────────
    
    private engine.log.v1.Log.KeyValue toProto(MvccStore.WatchEvent event) {
        return engine.log.v1.Log.KeyValue.newBuilder()
            .setKey(com.google.protobuf.ByteString.copyFromUtf8(event.key()))
            .setValue(event.kv() != null ? event.kv().value() : com.google.protobuf.ByteString.EMPTY)
            .setCreateRevision(event.kv() != null ? event.kv().createRevision() : 0)
            .setModRevision(event.kv() != null ? event.kv().modRevision() : 0)
            .setVersion(event.kv() != null ? event.kv().version() : 0)
            .build();
    }
}
```

---

### 2.8 修改文件：`coordinator/StateMachineDriver.java`

**职责变更**：添加 Txn 和 Compact 操作码

```java
package engine.coordinator;

// ... 现有导入 ...

/**
 * 状态机驱动器。
 * 新增 Txn 和 Compact 操作。
 */
@ApplicationScoped
public class StateMachineDriver {
    
    // 现有操作码
    public static final byte OP_PUT          = 0x01;
    public static final byte OP_DELETE       = 0x02;
    public static final byte OP_LEASE_GRANT  = 0x03;
    public static final byte OP_LEASE_REVOKE = 0x04;
    
    // 新增操作码
    public static final byte OP_TXN          = 0x05;
    public static final byte OP_COMPACT      = 0x06;
    
    @Inject RaftLogClient raftClient;
    @Inject KvStore       kvStore;
    @Inject LeaseManager  leaseManager;
    @Inject TxnManager    txnManager;      // 新增
    @Inject CompactManager compactManager;  // 新增
    
    // ... 现有代码 ...
    
    // ── 新增：事务操作 ──────────────────────────────────────────────────────
    
    public CompletableFuture<TxnManager.TxnResponse> txn(TxnManager.TxnRequest request) {
        byte[] data = serializeTxnRequest(request);
        
        return raftClient.propose(data)
            .thenCompose(resp -> {
                CompletableFuture<TxnManager.TxnResponse> future = new CompletableFuture<>();
                pendingProposals.put(resp.getIndex(), future);
                
                future.orTimeout(10, TimeUnit.SECONDS)
                      .whenComplete((rev, ex) -> pendingProposals.remove(resp.getIndex()));
                return future;
            });
    }
    
    // ── 新增：Compact 操作 ──────────────────────────────────────────────────
    
    public CompletableFuture<CompactManager.CompactResponse> compact(long revision) {
        byte[] data = serializeCompactRequest(revision);
        
        return raftClient.propose(data)
            .thenCompose(resp -> {
                CompletableFuture<CompactManager.CompactResponse> future = new CompletableFuture<>();
                pendingProposals.put(resp.getIndex(), future);
                
                future.orTimeout(10, TimeUnit.SECONDS)
                      .whenComplete((rev, ex) -> pendingProposals.remove(resp.getIndex()));
                return future;
            });
    }
    
    // ── 修改：applyEntry ────────────────────────────────────────────────────
    
    private void applyEntry(CommittedEntry entry) {
        long index = entry.getIndex();
        byte[] data = entry.getData().toByteArray();
        
        if (data.length == 0) {
            lastApplied.set(index);
            return;
        }
        
        byte opType = data[0];
        byte[] payload = Arrays.copyOfRange(data, 1, data.length);
        
        try {
            switch (opType) {
                case OP_PUT -> {
                    PutRequest req = PutRequest.parseFrom(payload);
                    kvStore.applyPut(req);
                    if (req.getLease() != 0) {
                        leaseManager.attach(req.getLease(), req.getKey().toStringUtf8());
                    }
                }
                case OP_DELETE -> {
                    DeleteRequest req = DeleteRequest.parseFrom(payload);
                    kvStore.applyDelete(req);
                }
                case OP_LEASE_GRANT -> {
                    LeaseGrantRequest req = LeaseGrantRequest.parseFrom(payload);
                    leaseManager.applyGrant(req.getId(), req.getTtl());
                }
                case OP_LEASE_REVOKE -> {
                    LeaseRevokeRequest req = LeaseRevokeRequest.parseFrom(payload);
                    leaseManager.applyRevoke(req.getId());
                }
                case OP_TXN -> {
                    // 事务已在 propose 阶段执行，这里只记录日志
                    LOG.debugf("Txn applied at index=%d", index);
                }
                case OP_COMPACT -> {
                    // Compact 是本地操作，不需要通过 Raft
                    LOG.debugf("Compact applied at index=%d", index);
                }
                default -> LOG.warnf("Unknown op type: 0x%02x at index %d", opType, index);
            }
        } catch (InvalidProtocolBufferException e) {
            LOG.errorf(e, "Failed to parse entry at index %d", index);
        }
        
        lastApplied.set(index);
        
        // Complete pending proposal future
        CompletableFuture<Long> pending = pendingProposals.remove(index);
        if (pending != null) pending.complete(kvStore.revision());
        
        // Periodic snapshot
        if (index - lastSnapshotIndex >= SNAPSHOT_INTERVAL) {
            triggerSnapshot(index);
        }
    }
    
    // ── 序列化辅助方法 ──────────────────────────────────────────────────────
    
    private byte[] serializeTxnRequest(TxnManager.TxnRequest request) {
        // TODO: 实现 protobuf 序列化
        return new byte[0];
    }
    
    private byte[] serializeCompactRequest(long revision) {
        // TODO: 实现 protobuf 序列化
        return new byte[0];
    }
}
```

---

### 2.9 修改文件：`coordinator/CoordinatorGrpcService.java`

**职责变更**：添加 Txn 和 Compact gRPC 接口

```java
package engine.coordinator;

// ... 现有导入 ...

/**
 * gRPC 服务实现。
 * 新增 Txn 和 Compact 接口。
 */
@GrpcService
public class CoordinatorGrpcService extends CoordinatorGrpc.CoordinatorImplBase {
    
    // ... 现有代码 ...
    
    @Inject TxnManager txnManager;        // 新增
    @Inject CompactManager compactManager; // 新增
    
    // ── 新增：事务接口 ──────────────────────────────────────────────────────
    
    @Override
    public void txn(TxnRequest req, StreamObserver<TxnResponse> resp) {
        try {
            // 构建事务请求
            TxnManager.TxnRequest txnReq = buildTxnRequest(req);
            
            // 执行事务
            TxnManager.TxnResponse txnResp = txnManager.execute(txnReq);
            
            // 构建响应
            TxnResponse.Builder builder = TxnResponse.newBuilder()
                .setSucceeded(txnResp.succeeded());
            
            for (Long rev : txnResp.revisions()) {
                builder.addResponses(OperationResponse.newBuilder()
                    .setRevision(rev));
            }
            
            resp.onNext(builder.build());
            resp.onCompleted();
        } catch (Exception e) {
            LOG.errorf(e, "Txn failed");
            resp.onError(io.grpc.Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }
    
    // ── 新增：Compact 接口 ──────────────────────────────────────────────────
    
    @Override
    public void compact(CompactRequest req, StreamObserver<CompactResponse> resp) {
        try {
            CompactManager.CompactResponse compactResp = compactManager.compact(req.getRevision());
            
            resp.onNext(CompactResponse.newBuilder()
                .setRevision(compactResp.compactedRevision())
                .setRemovedCount(compactResp.removedVersions())
                .build());
            resp.onCompleted();
        } catch (Exception e) {
            LOG.errorf(e, "Compact failed");
            resp.onError(io.grpc.Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }
    
    // ── 辅助方法 ─────────────────────────────────────────────────────────────
    
    private TxnManager.TxnRequest buildTxnRequest(TxnRequest req) {
        // 将 proto TxnRequest 转换为内部 TxnRequest
        List<TxnManager.Compare> compares = new ArrayList<>();
        for (var compare : req.getCompareList()) {
            compares.add(new TxnManager.Compare(
                TxnManager.CompareTarget.valueOf(compare.getTarget().name()),
                TxnManager.CompareResult.valueOf(compare.getResult().name()),
                compare.getKey().toStringUtf8(),
                compare.getValue()
            ));
        }
        
        List<TxnManager.Op> successOps = buildOps(req.getSuccessList());
        List<TxnManager.Op> failureOps = buildOps(req.getFailureList());
        
        return new TxnManager.TxnRequest(compares, successOps, failureOps);
    }
    
    private List<TxnManager.Op> buildOps(List<RequestOp> ops) {
        List<TxnManager.Op> result = new ArrayList<>();
        for (var op : ops) {
            if (op.hasPut()) {
                result.add(new TxnManager.Op(
                    TxnManager.OpType.PUT,
                    op.getPut().getKey().toStringUtf8(),
                    op.getPut().getValue(),
                    "",
                    op.getPut().getLease()
                ));
            } else if (op.hasDelete()) {
                result.add(new TxnManager.Op(
                    TxnManager.OpType.DELETE,
                    op.getDelete().getKey().toStringUtf8(),
                    com.google.protobuf.ByteString.EMPTY,
                    op.getDelete().getRangeEnd().toStringUtf8(),
                    0
                ));
            } else if (op.hasRange()) {
                result.add(new TxnManager.Op(
                    TxnManager.OpType.RANGE,
                    op.getRange().getKey().toStringUtf8(),
                    com.google.protobuf.ByteString.EMPTY,
                    op.getRange().getRangeEnd().toStringUtf8(),
                    0
                ));
            }
        }
        return result;
    }
}
```

---

### 2.10 修改文件：`proto/coordinator.proto`

**职责变更**：添加 Txn 和 Compact 接口定义

```protobuf
syntax = "proto3";

package engine.coordinator.v1;

option java_package = "engine.coordinator.v1";
option java_outer_classname = "CoordinatorOuterClass";

service Coordinator {
    // 现有接口
    rpc Put(PutRequest) returns (PutResponse);
    rpc Get(GetRequest) returns (GetResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc Watch(stream WatchRequest) returns (stream WatchResponse);
    rpc LeaseGrant(LeaseGrantRequest) returns (LeaseGrantResponse);
    rpc LeaseRevoke(LeaseRevokeRequest) returns (LeaseRevokeResponse);
    rpc LeaseKeepAlive(stream LeaseKeepAliveRequest) returns (stream LeaseKeepAliveResponse);
    rpc MemberList(MemberListRequest) returns (MemberListResponse);
    
    // 新增接口
    rpc Txn(TxnRequest) returns (TxnResponse);
    rpc Compact(CompactRequest) returns (CompactResponse);
}

// ── 事务相关消息 ──────────────────────────────────────────────────────────────

message TxnRequest {
    repeated Compare compare = 1;  // If 条件
    repeated RequestOp success = 2; // Then 操作
    repeated RequestOp failure = 3; // Else 操作
}

message Compare {
    enum CompareTarget {
        KEY = 0;
        VALUE = 1;
        CREATE_REVISION = 2;
        MOD_REVISION = 3;
        VERSION = 4;
    }
    
    enum CompareResult {
        EQUAL = 0;
        GREATER = 1;
        LESS = 2;
    }
    
    CompareTarget target = 1;
    CompareResult result = 2;
    bytes key = 3;
    bytes value = 4;
}

message RequestOp {
    oneof request {
        PutRequest put = 1;
        DeleteRequest delete = 2;
        RangeRequest range = 3;
    }
}

message TxnResponse {
    bool succeeded = 1;
    repeated OperationResponse responses = 2;
}

message OperationResponse {
    oneof response {
        PutResponse put = 1;
        DeleteResponse delete = 2;
        RangeResponse range = 3;
    }
}

// ── Compact 相关消息 ──────────────────────────────────────────────────────────

message CompactRequest {
    int64 revision = 1;
}

message CompactResponse {
    int64 revision = 1;
    int64 removed_count = 2;
}

// ── 现有消息保持不变 ──────────────────────────────────────────────────────────
// ...
```

---

## 3. 实现顺序

### Phase 1：基础 MVCC 存储（2天）

1. 创建 `mvcc/Revision.java`
2. 创建 `mvcc/VersionedKeyValue.java`
3. 创建 `mvcc/MvccStore.java`
4. 重构 `KvStore.java` 委托给 MvccStore
5. 单元测试

### Phase 2：Watch 历史回放（1天）

1. 修改 `WatchManager.java` 支持 startRevision
2. 实现 `MvccStore.getHistoryEvents()`
3. 实现 `MvccStore.getAllHistoryEvents()`
4. 测试历史回放

### Phase 3：Compact（1天）

1. 创建 `mvcc/CompactManager.java`
2. 实现手动和自动 compact
3. 测试 compact 后的查询行为

### Phase 4：事务（2天）

1. 创建 `mvcc/TxnManager.java`
2. 修改 `StateMachineDriver.java` 添加 OP_TXN
3. 修改 `CoordinatorGrpcService.java` 添加 txn 接口
4. 修改 `coordinator.proto` 添加事务消息
5. 测试事务原子性

### Phase 5：集成测试（1天）

1. 多节点一致性测试
2. Leader 切换测试
3. 历史查询测试
4. Watch 回放测试

---

## 4. 测试用例

### 4.1 MVCC 基本操作测试

```java
@Test
void testPutAndGetWithRevision() {
    MvccStore store = new MvccStore();
    
    // 第一次写入
    long rev1 = store.put("key1", ByteString.copyFromUtf8("value1"));
    assertEquals(1, rev1);
    
    // 第二次写入
    long rev2 = store.put("key1", ByteString.copyFromUtf8("value2"));
    assertEquals(2, rev2);
    
    // 查询当前版本
    var kv = store.get("key1");
    assertTrue(kv.isPresent());
    assertEquals("value2", kv.get().value().toStringUtf8());
    assertEquals(2, kv.get().version());
    
    // 查询历史版本
    var kv1 = store.get("key1", 1);
    assertTrue(kv1.isPresent());
    assertEquals("value1", kv1.get().value().toStringUtf8());
    assertEquals(1, kv1.get().version());
}
```

### 4.2 Watch 历史回放测试

```java
@Test
void testWatchWithHistoryReplay() {
    MvccStore store = new MvccStore();
    
    // 写入一些数据
    store.put("key1", ByteString.copyFromUtf8("v1"));
    store.put("key2", ByteString.copyFromUtf8("v1"));
    store.put("key1", ByteString.copyFromUtf8("v2"));
    
    // 从 revision 2 开始 watch
    List<MvccStore.WatchEvent> events = new ArrayList<>();
    store.addWatcher(events::add);
    
    // 模拟历史回放
    var historyEvents = store.getAllHistoryEvents(2, 3);
    assertEquals(2, historyEvents.size()); // key2@2, key1@3
}
```

### 4.3 事务测试

```java
@Test
void testTxnAtomicity() {
    TxnManager txnManager = new TxnManager();
    
    // 设置初始数据
    txnManager.mvccStore.put("key1", ByteString.copyFromUtf8("value1"));
    
    // 事务：如果 key1 存在，则更新
    TxnManager.TxnRequest request = new TxnManager.TxnRequest(
        List.of(new TxnManager.Compare(
            TxnManager.CompareTarget.KEY,
            TxnManager.CompareResult.EQUAL,
            "key1",
            ByteString.EMPTY
        )),
        List.of(new TxnManager.Op(
            TxnManager.OpType.PUT,
            "key1",
            ByteString.copyFromUtf8("value2"),
            "",
            0
        )),
        List.of()
    );
    
    TxnManager.TxnResponse response = txnManager.execute(request);
    assertTrue(response.succeeded());
    
    // 验证更新成功
    var kv = txnManager.mvccStore.get("key1");
    assertEquals("value2", kv.get().value().toStringUtf8());
}
```

---

## 5. 性能优化

### 5.1 内存优化

- 使用 `ConcurrentHashMap` 替代 `TreeMap` + 锁
- 历史版本使用分代存储（最近版本在内存，旧版本可选持久化）

### 5.2 查询优化

- 为热点 key 添加缓存
- Range 查询使用跳表索引

### 5.3 Watch 优化

- 使用无界队列 + 背压
- 批量推送事件减少网络开销
