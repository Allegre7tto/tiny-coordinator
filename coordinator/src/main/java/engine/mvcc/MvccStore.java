package engine.mvcc;

import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

@ApplicationScoped
public class MvccStore {

    public static final Comparator<ByteString> CMP = (a, b) -> {
        int minLen = Math.min(a.size(), b.size());
        for (int i = 0; i < minLen; i++) {
            int cmp = (a.byteAt(i) & 0xFF) - (b.byteAt(i) & 0xFF);
            if (cmp != 0) return cmp;
        }
        return a.size() - b.size();
    };

    private static final Logger LOG = Logger.getLogger(MvccStore.class);

    public enum EventType { PUT, DELETE }

    public record WatchEvent(
        EventType type,
        ByteString key,
        long revision,
        VersionedKeyValue.KvEntry kv
    ) {}

    private final TreeMap<ByteString, VersionedKeyValue> store = new TreeMap<>(CMP);
    private final AtomicLong currentRevision = new AtomicLong(0);
    private volatile long compactRevision = 0;

    private final CopyOnWriteArrayList<Consumer<WatchEvent>> watchers = new CopyOnWriteArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    public long putAtRevision(ByteString key, ByteString value, long revision, long lease) {
        rwLock.writeLock().lock();
        try {
            VersionedKeyValue vkv = store.computeIfAbsent(key, VersionedKeyValue::new);
            vkv.put(value, revision, lease);
            notifyWatchers(EventType.PUT, key, revision, vkv.latest());
            return revision;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public long deleteAtRevision(ByteString key, long revision) {
        rwLock.writeLock().lock();
        try {
            VersionedKeyValue vkv = store.get(key);
            if (vkv == null || vkv.latest() == null) return -1;
            vkv.tombstone(revision);
            notifyWatchers(EventType.DELETE, key, revision, vkv.latest());
            return revision;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public int deleteRangeAtRevision(ByteString startKey, ByteString endKey, long revision) {
        rwLock.writeLock().lock();
        try {
            int count = 0;
            var submap = endKey.isEmpty()
                ? store.tailMap(startKey, true)
                : store.subMap(startKey, true, endKey, false);
            List<ByteString> keys = new ArrayList<>(submap.keySet());
            for (ByteString key : keys) {
                VersionedKeyValue vkv = store.get(key);
                if (vkv != null && vkv.latest() != null) {
                    vkv.tombstone(revision);
                    notifyWatchers(EventType.DELETE, key, revision, vkv.latest());
                    count++;
                }
            }
            return count;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Optional<VersionedKeyValue.KvEntry> get(ByteString key, long revision) {
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

    public Optional<VersionedKeyValue.KvEntry> get(ByteString key) {
        return get(key, 0);
    }

    public record RangeEntry(ByteString key, VersionedKeyValue.KvEntry kv) {}
    public record RangeResult(List<RangeEntry> entries, long revision, boolean more) {}

    public RangeResult range(ByteString startKey, ByteString endKey, long revision, long limit) {
        rwLock.readLock().lock();
        try {
            if (revision > 0 && revision < compactRevision) {
                throw new IllegalStateException("revision " + revision + " has been compacted");
            }
            long queryRevision = (revision > 0) ? revision : currentRevision.get();
            List<RangeEntry> entries = new ArrayList<>();

            var submap = endKey.isEmpty()
                ? store.tailMap(startKey, true)
                : store.subMap(startKey, true, endKey, false);

            for (var e : submap.entrySet()) {
                VersionedKeyValue.KvEntry kv = e.getValue().getAtRevision(queryRevision);
                if (kv != null) {
                    entries.add(new RangeEntry(e.getKey(), kv));
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

    public List<WatchEvent> getAllHistoryEvents(long fromRevision, long toRevision) {
        rwLock.readLock().lock();
        try {
            List<WatchEvent> events = new ArrayList<>();
            for (var vkvEntry : store.entrySet()) {
                ByteString key = vkvEntry.getKey();
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

    public void addWatcher(Consumer<WatchEvent> watcher) {
        watchers.add(watcher);
    }

    public void removeWatcher(Consumer<WatchEvent> watcher) {
        watchers.remove(watcher);
    }

    private void notifyWatchers(EventType type, ByteString key, long revision,
                                VersionedKeyValue.KvEntry kv) {
        WatchEvent event = new WatchEvent(type, key, revision, kv);
        for (var watcher : watchers) {
            try { watcher.accept(event); }
            catch (Exception e) { LOG.warnf("Watcher error: %s", e.getMessage()); }
        }
    }

    public long compactRevision() { return compactRevision; }

    public long currentRevision() { return currentRevision.get(); }

    public void setCurrentRevision(long revision) {
        rwLock.writeLock().lock();
        try { currentRevision.set(revision); }
        finally { rwLock.writeLock().unlock(); }
    }

    public void setCompactrev(long rev) {
        rwLock.writeLock().lock();
        try { compactRevision = rev; }
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
