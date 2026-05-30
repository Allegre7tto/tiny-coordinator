package engine.coordinator;

import engine.coordinator.v1.CoordinatorOuterClass.*;
import engine.mvcc.MvccStore;
import engine.mvcc.VersionedKeyValue;

import com.google.protobuf.ByteString;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * KV 存储服务层。委托给 MvccStore 实现 MVCC 语义。
 */
@ApplicationScoped
public class KvStore {

    private static final Logger LOG = Logger.getLogger(KvStore.class);

    @Inject MvccStore mvccStore;

    // ── Watch events ─────────────────────────────────────────────────────────

    public record WatchEvent(MvccStore.EventType type, String key, KeyValue kv, long revision) {}

    private final List<Consumer<WatchEvent>> listeners = new ArrayList<>();

    @PostConstruct
    void init() {
        mvccStore.addWatcher(this::onMvccEvent);
    }

    public void addListener(Consumer<WatchEvent> listener) {
        listeners.add(listener);
    }

    private void onMvccEvent(MvccStore.WatchEvent event) {
        KeyValue kv = toProto(event.key(), event.kv());
        WatchEvent watchEvent = new WatchEvent(event.type(), event.key(), kv, event.revision());
        for (var listener : listeners) {
            try { listener.accept(watchEvent); }
            catch (Exception e) { LOG.warnf("WatchEvent listener error: %s", e.getMessage()); }
        }
    }

    // ── Apply (called by StateMachineDriver, single-threaded) ────────────────

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

    // ── Read ─────────────────────────────────────────────────────────────────

    public GetResponse get(GetRequest req) {
        String key = req.getKey().toStringUtf8();
        String rangeEnd = req.getRangeEnd().toStringUtf8();
        long revision = req.getRevision();
        long limit = req.getLimit();

        ResponseHeader header = ResponseHeader.newBuilder()
            .setRevision(mvccStore.currentRevision())
            .build();

        GetResponse.Builder resp = GetResponse.newBuilder().setHeader(header);

        if (rangeEnd.isEmpty()) {
            var kvOpt = mvccStore.get(key, revision);
            if (kvOpt.isPresent()) {
                resp.addKvs(toProto(key, kvOpt.get()));
                resp.setCount(1);
            }
        } else {
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
        ResponseHeader header = ResponseHeader.newBuilder()
            .setRevision(mvccStore.currentRevision())
            .build();
        GetResponse.Builder snap = GetResponse.newBuilder().setHeader(header);
        for (var entry : mvccStore.snapshotEntries()) {
            snap.addKvs(toProto(entry.key(), entry.kv()));
        }
        return snap.build().toByteArray();
    }

    public void restore(byte[] data) {
        try {
            GetResponse snap = GetResponse.parseFrom(data);
            List<MvccStore.RangeEntry> entries = new ArrayList<>();
            for (KeyValue kv : snap.getKvsList()) {
                entries.add(new MvccStore.RangeEntry(
                    kv.getKey().toStringUtf8(),
                    new VersionedKeyValue.KvEntry(
                        kv.getValue(),
                        kv.getCreateRevision(),
                        kv.getModRevision(),
                        kv.getVersion(),
                        kv.getLease()
                    )
                ));
            }
            mvccStore.restoreFromEntries(entries, snap.getHeader().getRevision());
            LOG.infof("KvStore restored: revision=%d keys=%d", mvccStore.currentRevision(), entries.size());
        } catch (Exception e) {
            LOG.errorf(e, "Failed to restore KvStore snapshot");
        }
    }

    public long revision() { return mvccStore.currentRevision(); }

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
