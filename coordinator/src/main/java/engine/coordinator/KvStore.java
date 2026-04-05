package engine.coordinator;

import engine.coordinator.v1.CoordinatorOuterClass.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Java MVCC Key-Value state machine.
 *
 * All mutations happen through {@link #applyPut} / {@link #applyDelete},
 * which are called exclusively by {@link StateMachineDriver} in committed-entry order.
 * Reads via {@link #get} are served directly from memory (no Raft round trip).
 */
@ApplicationScoped
public class KvStore {

    private static final Logger LOG = Logger.getLogger(KvStore.class);

    public record MvccValue(
        String value,
        long   createRevision,
        long   modRevision,
        long   version,
        long   lease
    ) {}

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final TreeMap<String, MvccValue> store = new TreeMap<>();
    private long revision = 0;

    // Listeners notified after each apply (WatchManager hooks in here)
    private final List<Consumer<WatchEvent>> listeners = new ArrayList<>();

    public record WatchEvent(EventType type, String key, KeyValue kv, long revision) {}

    public void addListener(Consumer<WatchEvent> listener) {
        listeners.add(listener);
    }

    // ── Apply operations (called by StateMachineDriver, single-threaded) ──────

    public void applyPut(PutRequest req) {
        rwLock.writeLock().lock();
        try {
            revision++;
            String key = req.getKey().toStringUtf8();

            MvccValue existing = store.get(key);
            long createRev = (existing != null) ? existing.createRevision() : revision;
            long ver       = (existing != null) ? existing.version() + 1 : 1;

            MvccValue mv = new MvccValue(
                    req.getValue().toStringUtf8(),
                    createRev, revision, ver, req.getLease());
            store.put(key, mv);

            notifyListeners(EventType.PUT, key, mv);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void applyDelete(DeleteRequest req) {
        rwLock.writeLock().lock();
        try {
            revision++;
            String key      = req.getKey().toStringUtf8();
            String rangeEnd = req.getRangeEnd().toStringUtf8();

            if (rangeEnd.isEmpty()) {
                MvccValue removed = store.remove(key);
                if (removed != null) notifyListeners(EventType.DELETE, key, removed);
            } else {
                var subMap = "\0".equals(rangeEnd)
                        ? store.tailMap(key, true)
                        : store.subMap(key, true, rangeEnd, false);
                var entries = new ArrayList<>(subMap.entrySet());
                for (var entry : entries) {
                    store.remove(entry.getKey());
                    notifyListeners(EventType.DELETE, entry.getKey(), entry.getValue());
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ── Read (lock-free for concurrent reads) ─────────────────────────────────

    public GetResponse get(GetRequest req) {
        rwLock.readLock().lock();
        try {
            String key      = req.getKey().toStringUtf8();
            String rangeEnd = req.getRangeEnd().toStringUtf8();
            long   limit    = req.getLimit();

            GetResponse.Builder resp = GetResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder().setRevision(revision));

            if (rangeEnd.isEmpty()) {
                MvccValue mv = store.get(key);
                if (mv != null) {
                    resp.addKvs(toProto(key, mv));
                    resp.setCount(1);
                }
            } else {
                var subMap = "\0".equals(rangeEnd)
                        ? store.tailMap(key, true)
                        : store.subMap(key, true, rangeEnd, false);
                long count = 0;
                for (var entry : subMap.entrySet()) {
                    resp.addKvs(toProto(entry.getKey(), entry.getValue()));
                    count++;
                    if (limit > 0 && count >= limit) {
                        resp.setMore(subMap.size() > count);
                        break;
                    }
                }
                resp.setCount(count);
            }
            return resp.build();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public long revision() {
        rwLock.readLock().lock();
        try { return revision; }
        finally { rwLock.readLock().unlock(); }
    }

    // ── Snapshot / Restore ────────────────────────────────────────────────────

    public byte[] snapshot() {
        rwLock.readLock().lock();
        try {
            GetResponse.Builder snap = GetResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder().setRevision(revision));
            for (var entry : store.entrySet())
                snap.addKvs(toProto(entry.getKey(), entry.getValue()));
            return snap.build().toByteArray();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void restore(byte[] data) {
        rwLock.writeLock().lock();
        try {
            GetResponse snap = GetResponse.parseFrom(data);
            store.clear();
            revision = snap.getHeader().getRevision();
            for (KeyValue kv : snap.getKvsList()) {
                store.put(kv.getKey().toStringUtf8(), new MvccValue(
                        kv.getValue().toStringUtf8(),
                        kv.getCreateRevision(),
                        kv.getModRevision(),
                        kv.getVersion(),
                        kv.getLease()));
            }
            LOG.infof("KvStore restored: revision=%d keys=%d", revision, store.size());
        } catch (InvalidProtocolBufferException e) {
            LOG.errorf(e, "Failed to restore KvStore snapshot");
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private void notifyListeners(EventType type, String key, MvccValue mv) {
        KeyValue kv = toProto(key, mv);
        WatchEvent event = new WatchEvent(type, key, kv, revision);
        for (var listener : listeners) {
            try { listener.accept(event); }
            catch (Exception e) { LOG.warnf("WatchEvent listener error: %s", e.getMessage()); }
        }
    }

    private KeyValue toProto(String key, MvccValue mv) {
        return KeyValue.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(mv.value()))
                .setCreateRevision(mv.createRevision())
                .setModRevision(mv.modRevision())
                .setVersion(mv.version())
                .setLease(mv.lease())
                .build();
    }
}
