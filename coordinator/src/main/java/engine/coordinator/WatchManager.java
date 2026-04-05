package engine.coordinator;

import engine.coordinator.v1.CoordinatorOuterClass.*;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Manages active Watch subscriptions and fans out KvStore apply events.
 *
 * Architecture (v3 — no C++ event stream):
 *   StateMachineDriver applies committed entry
 *       → KvStore.applyPut/applyDelete
 *           → KvStore notifies listeners
 *               → WatchManager.onEvent
 *                   → fan-out to matching watchers
 *
 * Watch state is pure Java memory. If Java restarts, clients must re-subscribe.
 * This is the same behavior as etcd (watch connections are not persistent across restarts).
 */
@ApplicationScoped
public class WatchManager {

    private static final Logger LOG = Logger.getLogger(WatchManager.class);

    @Inject KvStore kvStore;

    private final Map<Long, WatchEntry> watches = new ConcurrentHashMap<>();
    private final AtomicLong            nextId  = new AtomicLong(1);

    record WatchEntry(
        long   id,
        String key,
        String rangeEnd,
        long   startRevision,
        Consumer<WatchResponse> callback
    ) {}

    @PostConstruct
    void init() {
        kvStore.addListener(this::onEvent);
    }

    // ── Registration ──────────────────────────────────────────────────────────

    public long register(String key, String rangeEnd, long startRevision,
                         Consumer<WatchResponse> callback) {
        long id = nextId.getAndIncrement();
        watches.put(id, new WatchEntry(id, key, rangeEnd, startRevision, callback));
        LOG.debugf("Watch registered id=%d key=%s rangeEnd=%s", id, key, rangeEnd);
        return id;
    }

    public void cancel(long watchId) {
        watches.remove(watchId);
        LOG.debugf("Watch cancelled id=%d", watchId);
    }

    // ── Event handler (called by KvStore listener) ────────────────────────────

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
                LOG.warnf("Watch id=%d delivery failed, removing: %s", w.id(), e.getMessage());
                watches.remove(w.id());
            }
        });
    }

    private boolean matches(WatchEntry w, String key) {
        if (w.rangeEnd() == null || w.rangeEnd().isEmpty()) return key.equals(w.key());
        if ("\0".equals(w.rangeEnd()))                       return key.compareTo(w.key()) >= 0;
        return key.compareTo(w.key()) >= 0 && key.compareTo(w.rangeEnd()) < 0;
    }
}
