package engine.coordinator;

import engine.client.RaftLib;
import engine.coordinator.v1.CoordinatorOuterClass.*;
import engine.mvcc.CompactManager;
import engine.mvcc.TxnManager;

import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class StateMachineDriver {

    private static final Logger LOG = Logger.getLogger(StateMachineDriver.class);

    public static final byte OP_PUT          = 0x01;
    public static final byte OP_DELETE       = 0x02;
    public static final byte OP_LEASE_GRANT  = 0x03;
    public static final byte OP_LEASE_REVOKE = 0x04;
    public static final byte OP_TXN          = 0x05;
    public static final byte OP_COMPACT      = 0x06;

    private static final long SNAPSHOT_INTERVAL = 10_000;
    private static final int MAX_ENTRY_SIZE = 4 * 1024 * 1024;
    private static final int MAX_SNAP_SIZE  = 256 * 1024 * 1024;

    @Inject KvStore         kvStore;
    @Inject LeaseManager    leaseManager;
    @Inject TxnManager      txnManager;
    @Inject CompactManager  compactManager;

    @ConfigProperty(name = "raft.id")        long   raftId;
    @ConfigProperty(name = "raft.data.dir")  String dataDir;
    @ConfigProperty(name = "raft.peers")     String peers;
    @ConfigProperty(name = "raft.port")      int    raftPort;

    private RaftLib raftLib;
    private final AtomicLong lastApplied = new AtomicLong(0);
    private long lastSnapshotIndex = 0;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Map<Long, CompletableFuture<Long>> pendingProposals = new ConcurrentHashMap<>();

    @PostConstruct
    void start() {
        String[] peerAddrs = peers.split(",\\s*");
        raftLib = new RaftLib(raftId, dataDir, peerAddrs, raftPort);

        loadSnapshot();

        long startIdx = lastApplied.get() + 1;
        LOG.infof("Starting apply loop from index=%d", startIdx);
        Thread recvThread = new Thread(this::recvLoop, "raft-recv");
        recvThread.setDaemon(true);
        recvThread.start();
    }

    @PreDestroy
    void stop() {
        running.set(false);
        raftLib.close();
    }

    // ── Propose ────────────────────────────────────────────────────────────────

    public CompletableFuture<Long> propose(byte opType, com.google.protobuf.Message msg) {
        byte[] payload = msg.toByteArray();
        byte[] data = new byte[1 + payload.length];
        data[0] = opType;
        System.arraycopy(payload, 0, data, 1, payload.length);

        ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
        buf.put(data);

        long encoded = raftLib.propose(buf);
        long statusCode = encoded >> 48;
        long index = encoded & 0x0000_FFFF_FFFF_FFFFL;

        CompletableFuture<Long> future = new CompletableFuture<>();
        if (statusCode != 0) {
            future.completeExceptionally(new RuntimeException("propose failed: status=" + statusCode));
            return future;
        }

        pendingProposals.put(index, future);
        future.orTimeout(10, TimeUnit.SECONDS)
              .whenComplete((rev, ex) -> pendingProposals.remove(index));
        return future;
    }

    // ── Recv loop ──────────────────────────────────────────────────────────────

    private void recvLoop() {
        ByteBuffer buf = ByteBuffer.allocateDirect(MAX_ENTRY_SIZE);
        while (running.get()) {
            try {
                int n = raftLib.recv(buf);
                if (n < 0) {
                    if (running.get()) {
                        LOG.warnf("recv returned %d, restarting loop", n);
                        try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
                    }
                    continue;
                }
                long index = buf.getLong(0);
                long term = buf.getLong(8);
                byte[] data = new byte[n - 16];
                buf.position(16);
                buf.get(data);
                applyEntry(index, term, data);
            } catch (Exception e) {
                LOG.errorf(e, "Error in recv loop");
            }
        }
    }

    // ── Apply ──────────────────────────────────────────────────────────────────

    private void applyEntry(long index, long term, byte[] data) {
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
                case OP_TXN -> LOG.debugf("Txn applied at index=%d", index);
                case OP_COMPACT -> LOG.debugf("Compact applied at index=%d", index);
                default -> LOG.warnf("Unknown op type: 0x%02x at index %d", opType, index);
            }
        } catch (InvalidProtocolBufferException e) {
            LOG.errorf(e, "Failed to parse entry at index %d", index);
        }

        lastApplied.set(index);

        CompletableFuture<Long> pending = pendingProposals.remove(index);
        if (pending != null) pending.complete(kvStore.revision());

        if (index - lastSnapshotIndex >= SNAPSHOT_INTERVAL) {
            saveSnapshot(index);
        }
    }

    // ── Snapshot ───────────────────────────────────────────────────────────────

    private void saveSnapshot(long index) {
        CompletableFuture.runAsync(() -> {
            try {
                byte[] snapData = kvStore.snapshot();
                ByteBuffer buf = ByteBuffer.allocateDirect(snapData.length);
                buf.put(snapData);
                raftLib.snap(index, buf);
                lastSnapshotIndex = index;
                LOG.infof("Snapshot saved at index=%d size=%d", index, snapData.length);
            } catch (Exception e) {
                LOG.errorf(e, "Snapshot save failed at index=%d", index);
            }
        });
    }

    private void loadSnapshot() {
        ByteBuffer buf = ByteBuffer.allocateDirect(MAX_SNAP_SIZE);
        int size = raftLib.load(buf);
        if (size <= 0) {
            LOG.info("No existing snapshot, starting fresh");
            return;
        }
        byte[] data = new byte[size];
        buf.position(0);
        buf.get(data);
        kvStore.restore(data);
        long rev = kvStore.revision();
        lastApplied.set(rev);
        lastSnapshotIndex = rev;
        LOG.infof("Loaded snapshot size=%d revision=%d", size, rev);
    }

    public long lastAppliedIndex() { return lastApplied.get(); }
}
