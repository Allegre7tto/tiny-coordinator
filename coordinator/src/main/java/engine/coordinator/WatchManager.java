package engine.coordinator;

import engine.coordinator.v1.CoordinatorOuterClass.*;
import engine.mvcc.MvccStore;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import com.google.protobuf.ByteString;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Watch 管理器。支持从指定 revision 开始监听事件。
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

    @PreDestroy
    void destroy() {
        scheduler.shutdown();
    }

    // ── Registration ─────────────────────────────────────────────────────────

    public long register(String key, String rangeEnd, long startRevision,
                         Consumer<WatchResponse> callback) {
        long id = nextId.getAndIncrement();
        watches.put(id, new WatchEntry(id, key, rangeEnd, startRevision, callback, Instant.now()));

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

    // ── History replay ───────────────────────────────────────────────────────

    private void replayHistory(long watchId, String key, String rangeEnd, long startRevision) {
        WatchEntry entry = watches.get(watchId);
        if (entry == null) return;

        long currentRev = mvccStore.currentRevision();
        var events = mvccStore.getAllHistoryEvents(startRevision, currentRev);

        for (var event : events) {
            if (!matches(entry, event.key())) continue;

            WatchResponse resp = WatchResponse.newBuilder()
                .setWatchId(watchId)
                .setHeader(ResponseHeader.newBuilder().setRevision(event.revision()))
                .addEvents(Event.newBuilder()
                    .setType(event.type() == MvccStore.EventType.PUT ? engine.coordinator.v1.CoordinatorOuterClass.EventType.PUT : engine.coordinator.v1.CoordinatorOuterClass.EventType.DELETE)
                    .setKv(toProto(event)))
                .build();

            try { entry.callback().accept(resp); }
            catch (Exception e) { LOG.warnf("Watch replay failed: %s", e.getMessage()); }
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
                    .setType(event.type() == MvccStore.EventType.PUT ? engine.coordinator.v1.CoordinatorOuterClass.EventType.PUT : engine.coordinator.v1.CoordinatorOuterClass.EventType.DELETE)
                    .setKv(event.kv()))
                .build();

            try { w.callback().accept(resp); }
            catch (Exception e) {
                LOG.warnf("Watch delivery failed, removing: %s", e.getMessage());
                watches.remove(w.id());
            }
        });
    }

    private boolean matches(WatchEntry w, String key) {
        if (w.rangeEnd() == null || w.rangeEnd().isEmpty()) return key.equals(w.key());
        if ("\0".equals(w.rangeEnd())) return key.compareTo(w.key()) >= 0;
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

    private engine.coordinator.v1.CoordinatorOuterClass.KeyValue toProto(MvccStore.WatchEvent event) {
        return engine.coordinator.v1.CoordinatorOuterClass.KeyValue.newBuilder()
            .setKey(ByteString.copyFromUtf8(event.key()))
            .setValue(event.kv() != null ? event.kv().value() : ByteString.EMPTY)
            .setCreateRevision(event.kv() != null ? event.kv().createRevision() : 0)
            .setModRevision(event.kv() != null ? event.kv().modRevision() : 0)
            .setVersion(event.kv() != null ? event.kv().version() : 0)
            .build();
    }
}
