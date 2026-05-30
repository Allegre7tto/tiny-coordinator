package engine.mvcc;

import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
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

    public enum EventType { PUT, DELETE }

    public record WatchEvent(
        EventType type,
        String key,
        long revision,
        VersionedKeyValue.KvEntry kv
    ) {}

    private final ConcurrentHashMap<String, VersionedKeyValue> store = new ConcurrentHashMap<>();
    private final AtomicLong currentRevision = new AtomicLong(0);
    private volatile long compactRevision = 0;

    private final CopyOnWriteArrayList<Consumer<WatchEvent>> watchers = new CopyOnWriteArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // ── Put ──────────────────────────────────────────────────────────────────

    public long put(String key, ByteString value, long lease) {
        rwLock.writeLock().lock();
        try {
            long rev = currentRevision.incrementAndGet();
            VersionedKeyValue vkv = store.computeIfAbsent(key, VersionedKeyValue::new);
            vkv.put(value, rev, lease);
            notifyWatchers(EventType.PUT, key, rev, vkv.latest());
            return rev;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public long put(String key, ByteString value) {
        return put(key, value, 0);
    }

    // ── Delete ───────────────────────────────────────────────────────────────

    public long delete(String key) {
        rwLock.writeLock().lock();
        try {
            VersionedKeyValue vkv = store.get(key);
            if (vkv == null || vkv.latest() == null) return -1;
            long rev = currentRevision.incrementAndGet();
            vkv.tombstone(rev);
            notifyWatchers(EventType.DELETE, key, rev, vkv.latest());
            return rev;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public int deleteRange(String startKey, String endKey) {
        rwLock.writeLock().lock();
        try {
            int count = 0;
            for (String key : store.keySet()) {
                if (key.compareTo(startKey) >= 0 &&
                    (endKey.isEmpty() || key.compareTo(endKey) < 0)) {
                    if (delete(key) > 0) count++;
                }
            }
            return count;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ── Get ──────────────────────────────────────────────────────────────────

    public Optional<VersionedKeyValue.KvEntry> get(String key, long revision) {
        rwLock.readLock().lock();
        try {
            if (revision > 0 && revision < compactRevision) {
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

    public Optional<VersionedKeyValue.KvEntry> get(String key) {
        return get(key, 0);
    }

    // ── Range ────────────────────────────────────────────────────────────────

    public record RangeEntry(String key, VersionedKeyValue.KvEntry kv) {}
    public record RangeResult(List<RangeEntry> entries, long revision, boolean more) {}

    public RangeResult range(String startKey, String endKey, long revision, long limit) {
        rwLock.readLock().lock();
        try {
            if (revision > 0 && revision < compactRevision) {
                throw new IllegalStateException("revision " + revision + " has been compacted");
            }
            long queryRevision = (revision > 0) ? revision : currentRevision.get();
            List<RangeEntry> entries = new ArrayList<>();
            for (var e : store.entrySet()) {
                String key = e.getKey();
                if (key.compareTo(startKey) < 0) continue;
                if (!endKey.isEmpty() && key.compareTo(endKey) >= 0) continue;
                VersionedKeyValue.KvEntry kv = e.getValue().getAtRevision(queryRevision);
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

    // ── Watch history ────────────────────────────────────────────────────────

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
            events.sort(Comparator.comparingLong(WatchEvent::revision));
            return events;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // ── Watcher ──────────────────────────────────────────────────────────────

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
            try { watcher.accept(event); }
            catch (Exception e) { LOG.warnf("Watcher error: %s", e.getMessage()); }
        }
    }

    // ── Compact ──────────────────────────────────────────────────────────────

    public long compactRevision() { return compactRevision; }

    public long currentRevision() { return currentRevision.get(); }

    public void setCurrentRevision(long revision) {
        rwLock.writeLock().lock();
        try { currentRevision.set(revision); }
        finally { rwLock.writeLock().unlock(); }
    }

    public int compact(long beforeRevision) {
        rwLock.writeLock().lock();
        try {
            int totalRemoved = 0;
            for (VersionedKeyValue vkv : store.values()) {
                totalRemoved += vkv.compact(beforeRevision);
            }
            compactRevision = beforeRevision;
            return totalRemoved;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ── Snapshot / Restore ───────────────────────────────────────────────────

    public List<RangeEntry> snapshotEntries() {
        rwLock.readLock().lock();
        try {
            List<RangeEntry> entries = new ArrayList<>();
            for (var e : store.entrySet()) {
                VersionedKeyValue.KvEntry kv = e.getValue().latest();
                if (kv != null) {
                    entries.add(new RangeEntry(e.getKey(), kv));
                }
            }
            return entries;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void restoreFromEntries(List<RangeEntry> entries, long revision) {
        rwLock.writeLock().lock();
        try {
            store.clear();
            currentRevision.set(revision);
            compactRevision = 0;
            for (var e : entries) {
                VersionedKeyValue vkv = new VersionedKeyValue(e.key());
                vkv.put(e.kv().value(), e.kv().modRevision(), e.kv().lease());
                store.put(e.key(), vkv);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
