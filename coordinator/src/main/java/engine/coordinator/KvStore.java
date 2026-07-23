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

@ApplicationScoped
public class KvStore {

    private static final Logger LOG = Logger.getLogger(KvStore.class);

    @Inject MvccStore mvccStore;

    public record WatchEvent(MvccStore.EventType type, ByteString key, KeyValue kv, long revision) {}

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

    public void applyPut(PutRequest req, long revision) {
        mvccStore.putAtRevision(req.getKey(), req.getValue(), revision, req.getLease());
    }

    public void applyDelete(DeleteRequest req, long revision) {
        if (req.getRangeend().isEmpty()) {
            mvccStore.deleteAtRevision(req.getKey(), revision);
        } else {
            mvccStore.deleteRangeAtRevision(req.getKey(), req.getRangeend(), revision);
        }
    }

    public GetResponse get(GetRequest req) {
        long revision = req.getRevision();
        long limit = req.getLimit();

        ResponseHeader header = ResponseHeader.newBuilder()
            .setRevision(mvccStore.currentRevision())
            .build();

        GetResponse.Builder resp = GetResponse.newBuilder().setHeader(header);

        if (req.getRangeend().isEmpty()) {
            var kvOpt = mvccStore.get(req.getKey(), revision);
            if (kvOpt.isPresent()) {
                resp.addKvs(toProto(req.getKey(), kvOpt.get()));
                resp.setCount(1);
            }
        } else {
            var result = mvccStore.range(req.getKey(), req.getRangeend(), revision, limit);
            for (var entry : result.entries()) {
                resp.addKvs(toProto(entry.key(), entry.kv()));
            }
            resp.setCount(result.entries().size());
            resp.setMore(result.more());
        }
        return resp.build();
    }

    public byte[] snapshot(long lastAppliedIndex, LeaseManager leaseManager) {
        StateMachineSnapshot.Builder snap = StateMachineSnapshot.newBuilder()
            .setLastappliedidx(lastAppliedIndex)
            .setMvccrev(mvccStore.currentRevision())
            .setCompactrev(mvccStore.compactRevision());
        for (var entry : mvccStore.snapshotEntries()) {
            snap.addKvs(toProto(entry.key(), entry.kv()));
        }
        for (var ls : leaseManager.allLeases()) {
            LeaseState.Builder lb = LeaseState.newBuilder()
                .setId(ls.id()).setTtlsecs(ls.ttlSeconds())
                .setDeadlinems(ls.deadline().toEpochMilli());
            for (var key : ls.keys()) lb.addKeys(key);
            snap.addLeases(lb.build());
        }
        return snap.build().toByteArray();
    }

    public long restore(byte[] data, LeaseManager leaseManager) {
        try {
            StateMachineSnapshot snap = StateMachineSnapshot.parseFrom(data);
            List<MvccStore.RangeEntry> entries = new ArrayList<>();
            for (KeyValue kv : snap.getKvsList()) {
                entries.add(new MvccStore.RangeEntry(
                    kv.getKey(),
                    new VersionedKeyValue.KvEntry(
                        kv.getValue(),
                        kv.getCreaterev(),
                        kv.getModrev(),
                        kv.getVersion(),
                        kv.getLease()
                    )
                ));
            }
            mvccStore.restoreFromEntries(entries, snap.getMvccrev());
            mvccStore.setCompactrev(snap.getCompactrev());

            for (LeaseState ls : snap.getLeasesList()) {
                leaseManager.restoreLease(
                    ls.getId(), ls.getTtlsecs(),
                    java.time.Instant.ofEpochMilli(ls.getDeadlinems()),
                    new java.util.HashSet<>(ls.getKeysList()));
            }

            LOG.infof("restored: revision=%d compact=%d keys=%d leases=%d lastApplied=%d",
                mvccStore.currentRevision(), mvccStore.compactRevision(),
                entries.size(), snap.getLeasesCount(), snap.getLastappliedidx());

            return snap.getLastappliedidx();
        } catch (Exception e) {
            LOG.errorf(e, "Failed to restore KvStore snapshot");
            return 0;
        }
    }

    public long revision() { return mvccStore.currentRevision(); }

    private KeyValue toProto(ByteString key, VersionedKeyValue.KvEntry mv) {
        return KeyValue.newBuilder()
            .setKey(key)
            .setValue(mv.value())
            .setCreaterev(mv.createRevision())
            .setModrev(mv.modRevision())
            .setVersion(mv.version())
            .setLease(mv.lease())
            .build();
    }
}
