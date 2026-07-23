package engine.coordinator;

import engine.client.RaftLib;
import engine.coordinator.v1.CoordinatorOuterClass.*;
import engine.jni.v1.Jni.*;
import engine.mvcc.CompactManager;
import engine.mvcc.MvccStore;
import engine.mvcc.TxnManager;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.nio.ByteOrder;
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
    public static final byte OP_LEASE_EXPIRE = 0x07;
    public static final byte OP_LEASE_RENEW  = 0x08;

    private static final long SNAPSHOT_INTERVAL = 10_000;

    @Inject MvccStore       mvccStore;
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
    private final AtomicLong nextPid = new AtomicLong(1);
    private final Map<Long, CompletableFuture<Long>> pending = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<byte[]>> typedPending = new ConcurrentHashMap<>();

    @PostConstruct
    void start() {
        String[] peerAddrs = peers.split(",\\s*");
        raftLib = new RaftLib(raftId, dataDir, peerAddrs, raftPort);
        loadSnapshot();
        LOG.infof("Starting apply loop from index=%d", lastApplied.get() + 1);
        new Thread(this::recvLoop, "raft-recv").start();
    }

    @PreDestroy
    void stop() {
        running.set(false);
        raftLib.close();
    }

    public boolean isLeader()   { return raftLib.isLeader(); }
    public long getTerm()       { return raftLib.getTerm(); }
    public long getLeaderId()   { return raftLib.getLeaderId(); }
    public long getCommitIdx()  { return raftLib.getCommitIndex(); }
    public long getAppliedIdx() { return raftLib.getAppliedIndex(); }
    public long lastApplied()   { return lastApplied.get(); }

    public CompletableFuture<Long> propose(byte op, com.google.protobuf.Message msg) {
        long pid = nextPid.getAndIncrement();
        byte[] raw = envelope(op, pid, msg.toByteArray());
        return doPropose(raw, pid);
    }

    public CompletableFuture<byte[]> proposeTxn(TxnRequest req) {
        long pid = nextPid.getAndIncrement();
        byte[] raw = envelope(OP_TXN, pid, req.toByteArray());
        return doProposeTyped(raw, pid);
    }

    public CompletableFuture<byte[]> proposeCompact(CompactRequest req) {
        long pid = nextPid.getAndIncrement();
        byte[] raw = envelope(OP_COMPACT, pid, req.toByteArray());
        return doProposeTyped(raw, pid);
    }

    public void proposeLeaseExpire(long leaseId) {
        long pid = nextPid.getAndIncrement();
        byte[] pbytes = new byte[8];
        java.nio.ByteBuffer.wrap(pbytes).order(ByteOrder.LITTLE_ENDIAN).putLong(leaseId);
        byte[] raw = envelope(OP_LEASE_EXPIRE, pid, pbytes);
        raftLib.propose(raw);
    }

    private static byte[] envelope(byte op, long pid, byte[] payload) {
        return CommandEnvelope.newBuilder()
            .setOptype(op & 0xFF).setPid(pid)
            .setPayload(ByteString.copyFrom(payload))
            .build().toByteArray();
    }

    private CompletableFuture<Long> doPropose(byte[] raw, long pid) {
        long encoded = raftLib.propose(raw);
        long st = encoded >> 48;
        CompletableFuture<Long> f = new CompletableFuture<>();
        if (st != 0) {
            f.completeExceptionally(new RuntimeException("propose failed: " + st));
            return f;
        }
        pending.put(pid, f);
        f.orTimeout(10, TimeUnit.SECONDS).whenComplete((r, e) -> pending.remove(pid));
        return f;
    }

    private CompletableFuture<byte[]> doProposeTyped(byte[] raw, long pid) {
        long encoded = raftLib.propose(raw);
        long st = encoded >> 48;
        CompletableFuture<byte[]> f = new CompletableFuture<>();
        if (st != 0) {
            f.completeExceptionally(new RuntimeException("propose failed: " + st));
            return f;
        }
        typedPending.put(pid, f);
        f.orTimeout(10, TimeUnit.SECONDS).whenComplete((r, e) -> typedPending.remove(pid));
        return f;
    }

    private void recvLoop() {
        while (running.get()) {
            try {
                CommittedEntry entry = raftLib.recv();
                if (entry == null) {
                    if (running.get()) Thread.sleep(1000);
                    continue;
                }
                apply(entry.getIndex(), entry.getTerm(), entry.getData());
            } catch (Exception e) {
                if (running.get()) LOG.errorf(e, "recv error");
            }
        }
    }

    private void apply(long index, long term, ByteString raw) {
        if (raw.isEmpty()) {
            lastApplied.set(index);
            return;
        }

        CommandEnvelope env;
        try {
            env = CommandEnvelope.parseFrom(raw);
        } catch (InvalidProtocolBufferException e) {
            lastApplied.set(index);
            return;
        }

        byte op = (byte) env.getOptype();
        long pid = env.getPid();
        byte[] payload = env.getPayload().toByteArray();

        long rev = mvccStore.currentRevision() + 1;
        mvccStore.setCurrentRevision(rev);

        try {
            switch (op) {
                case OP_PUT -> {
                    PutRequest req = PutRequest.parseFrom(payload);
                    kvStore.applyPut(req, rev);
                    if (req.getLease() != 0) leaseManager.attach(req.getLease(), req.getKey());
                }
                case OP_DELETE -> kvStore.applyDelete(DeleteRequest.parseFrom(payload), rev);
                case OP_LEASE_GRANT -> {
                    LeaseGrantRequest req = LeaseGrantRequest.parseFrom(payload);
                    leaseManager.applyGrant(req.getId(), req.getTtl());
                }
                case OP_LEASE_REVOKE ->
                    leaseManager.applyRevoke(LeaseRevokeRequest.parseFrom(payload).getId());
                case OP_LEASE_RENEW ->
                    leaseManager.renewOnApply(LeaseKeepAliveRequest.parseFrom(payload).getId());
                case OP_LEASE_EXPIRE -> {
                    long leaseId = java.nio.ByteBuffer.wrap(payload, 0, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
                    for (var key : leaseManager.keysOf(leaseId)) {
                        mvccStore.deleteAtRevision(key, rev);
                    }
                    leaseManager.applyRevoke(leaseId);
                }
                case OP_TXN -> {
                    var f = typedPending.remove(pid);
                    if (f != null) f.complete(txnManager.applyTxn(TxnRequest.parseFrom(payload)).toByteArray());
                }
                case OP_COMPACT -> {
                    var f = typedPending.remove(pid);
                    if (f != null) f.complete(compactManager.applyCompact(CompactRequest.parseFrom(payload)).toByteArray());
                }
                default -> LOG.warnf("Unknown op 0x%02x", op);
            }
        } catch (InvalidProtocolBufferException e) {
            LOG.errorf(e, "parse error at index %d", index);
        }

        lastApplied.set(index);

        var f = pending.remove(pid);
        if (f != null) f.complete(mvccStore.currentRevision());

        if (index - lastSnapshotIndex >= SNAPSHOT_INTERVAL) saveSnapshot(index);
    }

    private void saveSnapshot(long index) {
        try {
            byte[] snap = kvStore.snapshot(lastApplied.get(), leaseManager);
            raftLib.snap(index, snap);
            lastSnapshotIndex = index;
            LOG.infof("snapshot at index=%d size=%d", index, snap.length);
        } catch (Exception e) {
            LOG.errorf(e, "snapshot failed at index=%d", index);
        }
    }

    private void loadSnapshot() {
        SnapshotLoad snap = raftLib.load();
        if (!snap.getFound()) {
            LOG.info("no snapshot, starting fresh");
            return;
        }
        kvStore.restore(snap.getData().toByteArray(), leaseManager);
        lastApplied.set(snap.getLastidx());
        lastSnapshotIndex = snap.getLastidx();
        LOG.infof("loaded snapshot at index=%d size=%d", snap.getLastidx(), snap.getData().size());
    }

    public void setLastApplied(long idx) { lastApplied.set(idx); }
}
