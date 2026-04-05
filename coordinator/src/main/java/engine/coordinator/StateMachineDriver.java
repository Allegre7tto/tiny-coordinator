package engine.coordinator;

import engine.client.RaftLogClient;
import engine.coordinator.v1.CoordinatorOuterClass.*;
import engine.log.v1.Log.CommittedEntry;
import engine.log.v1.Log.SnapshotChunk;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Drives the state machine apply loop:
 *
 *   1. On startup: LoadSnapshot → restore KvStore + LeaseManager
 *   2. SubscribeCommitted(lastApplied+1) → apply each entry in order
 *   3. Periodically SaveSnapshot → C++ truncates Raft log
 *
 * All writes go through {@link #propose}: serialize → RaftLogClient.propose() →
 * wait for the entry to be applied via the committed stream.
 */
@ApplicationScoped
public class StateMachineDriver {

    private static final Logger LOG = Logger.getLogger(StateMachineDriver.class);

    public static final byte OP_PUT          = 0x01;
    public static final byte OP_DELETE       = 0x02;
    public static final byte OP_LEASE_GRANT  = 0x03;
    public static final byte OP_LEASE_REVOKE = 0x04;

    private static final long SNAPSHOT_INTERVAL = 10_000; // entries between snapshots

    @Inject RaftLogClient raftClient;
    @Inject KvStore       kvStore;
    @Inject LeaseManager  leaseManager;

    private final AtomicLong lastApplied = new AtomicLong(0);
    private long lastSnapshotIndex = 0;

    // Pending proposals: log_index → CompletableFuture
    private final Map<Long, CompletableFuture<Long>> pendingProposals = new ConcurrentHashMap<>();

    private Cancellable subscription;

    @PostConstruct
    void start() {
        // Phase 1: Load snapshot from C++
        try {
            raftClient.loadSnapshot()
                    .collect().asList()
                    .await().atMost(java.time.Duration.ofSeconds(30))
                    .forEach(chunk -> {
                        if (chunk.getData().size() > 0) {
                            kvStore.restore(chunk.getData().toByteArray());
                            lastApplied.set(chunk.getLastIndex());
                            lastSnapshotIndex = chunk.getLastIndex();
                            LOG.infof("Loaded snapshot at index=%d", chunk.getLastIndex());
                        }
                    });
        } catch (Exception e) {
            LOG.info("No existing snapshot, starting fresh");
        }

        // Phase 2: Subscribe to committed entries
        long startIdx = lastApplied.get() + 1;
        LOG.infof("Subscribing to committed entries from index=%d", startIdx);

        subscription = raftClient.subscribeCommitted(startIdx)
                .subscribe().with(
                    this::applyEntry,
                    err -> LOG.errorf(err, "Committed stream error; will attempt reconnect")
                );
    }

    @PreDestroy
    void stop() {
        if (subscription != null) subscription.cancel();
    }

    // ── Propose (called by CoordinatorGrpcService) ────────────────────────────

    /**
     * Serialize operation and submit through Raft.
     * Returns a future that completes with the revision when the entry is applied.
     */
    public CompletableFuture<Long> propose(byte opType, com.google.protobuf.Message msg) {
        byte[] payload = msg.toByteArray();
        byte[] data = new byte[1 + payload.length];
        data[0] = opType;
        System.arraycopy(payload, 0, data, 1, payload.length);

        try {
            var resp = raftClient.propose(data);
            CompletableFuture<Long> future = new CompletableFuture<>();
            pendingProposals.put(resp.getIndex(), future);

            // Timeout to prevent indefinite blocking
            future.orTimeout(10, TimeUnit.SECONDS)
                  .whenComplete((rev, ex) -> pendingProposals.remove(resp.getIndex()));
            return future;
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // ── Apply (single-threaded, called from committed stream) ─────────────────

    private void applyEntry(CommittedEntry entry) {
        long index = entry.getIndex();
        byte[] data = entry.getData().toByteArray();

        if (data.length == 0) {
            lastApplied.set(index);
            return;
        }

        byte opType = data[0];
        byte[] payload = new byte[data.length - 1];
        System.arraycopy(data, 1, payload, 0, payload.length);

        try {
            switch (opType) {
                case OP_PUT -> {
                    PutRequest req = PutRequest.parseFrom(payload);
                    kvStore.applyPut(req);
                    if (req.getLease() != 0) leaseManager.attach(req.getLease(), req.getKey().toStringUtf8());
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

    private void triggerSnapshot(long index) {
        CompletableFuture.runAsync(() -> {
            try {
                byte[] snapData = kvStore.snapshot();
                raftClient.saveSnapshot(index, 0, snapData).get(30, TimeUnit.SECONDS);
                lastSnapshotIndex = index;
                LOG.infof("Snapshot saved at index=%d size=%d", index, snapData.length);
            } catch (Exception e) {
                LOG.errorf(e, "Snapshot save failed at index=%d", index);
            }
        });
    }

    public long lastAppliedIndex() { return lastApplied.get(); }
}
