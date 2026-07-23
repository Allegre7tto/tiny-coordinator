package dev.talent.server.state;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dev.talent.proto.coordinator.Command;
import dev.talent.proto.coordinator.Compare;
import dev.talent.proto.coordinator.DeleteRequest;
import dev.talent.proto.coordinator.LeaseExpireCommand;
import dev.talent.proto.coordinator.PutRequest;
import dev.talent.proto.coordinator.RangeRequest;
import dev.talent.proto.coordinator.RequestOp;
import dev.talent.proto.coordinator.TxnRequest;
import dev.talent.runtime.ReplicatedStateMachine;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CoordinatorStateMachine implements ReplicatedStateMachine {
    private static final int MAX_RESULTS = 10_000;

    private final Object lock = new Object();
    private final MvccStore mvcc = new MvccStore();
    private final LeaseRegistry leases = new LeaseRegistry();
    private final WatchHub watches;
    private final LinkedHashMap<Long, StateMachineResult> results =
            new LinkedHashMap<>(128, 0.75f, true);
    private long appliedIndex;
    private long appliedTerm;

    public CoordinatorStateMachine(WatchHub watches) {
        this.watches = watches;
    }

    @Override
    public void apply(long index, long term, byte[] encodedCommand) {
        Command command;
        try {
            command = Command.parseFrom(encodedCommand);
        } catch (InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("malformed coordinator command", invalid);
        }

        List<WatchHub.Event> events = new ArrayList<>();
        synchronized (lock) {
            if (index <= appliedIndex) {
                return;
            }
            if (index != appliedIndex + 1) {
                throw new IllegalStateException(
                        "non-contiguous apply: expected " + (appliedIndex + 1) + ", got " + index);
            }
            StateMachineResult result;
            try {
                result = execute(command, events);
            } catch (IllegalArgumentException invalid) {
                result = new StateMachineResult.Failure(invalid.getMessage(), mvcc.revision());
                events.clear();
            }
            appliedIndex = index;
            appliedTerm = term;
            if (command.getRequestId() != 0) {
                results.put(command.getRequestId(), result);
                while (results.size() > MAX_RESULTS) {
                    results.remove(results.firstEntry().getKey());
                }
            }
        }
        watches.publish(events);
    }

    @Override
    public void advance(long index, long term) {
        synchronized (lock) {
            if (index <= appliedIndex) {
                return;
            }
            if (index != appliedIndex + 1) {
                throw new IllegalStateException(
                        "non-contiguous apply advance: expected "
                                + (appliedIndex + 1)
                                + ", got "
                                + index);
            }
            appliedIndex = index;
            appliedTerm = term;
        }
    }

    public MvccStore.Range range(RangeRequest request) {
        synchronized (lock) {
            return mvcc.range(
                    key(request.getKey()),
                    optionalKey(request.getRangeend()),
                    request.getRevision(),
                    request.getLimit(),
                    request.getKeysonly());
        }
    }

    public StateMachineResult result(long requestId) {
        synchronized (lock) {
            return results.get(requestId);
        }
    }

    public List<Long> expiredLeaseIds(long epochMillis) {
        synchronized (lock) {
            return leases.expiredAt(epochMillis);
        }
    }

    public LeaseRegistry.Lease lease(long id) {
        synchronized (lock) {
            return leases.get(id);
        }
    }

    public byte[] snapshot() {
        synchronized (lock) {
            return CoordinatorSnapshotCodec.encode(appliedIndex, appliedTerm, mvcc, leases);
        }
    }

    @Override
    public void restore(byte[] snapshot) {
        CoordinatorSnapshotCodec.Decoded decoded = CoordinatorSnapshotCodec.decode(snapshot);
        synchronized (lock) {
            mvcc.restore(
                    decoded.revision(), decoded.compactedRevision(), decoded.histories());
            leases.restore(decoded.leases());
            appliedIndex = decoded.appliedIndex();
            appliedTerm = decoded.appliedTerm();
            results.clear();
        }
        watches.reset(decoded.compactedRevision());
    }

    public long appliedIndex() {
        synchronized (lock) {
            return appliedIndex;
        }
    }

    public long appliedTerm() {
        synchronized (lock) {
            return appliedTerm;
        }
    }

    public long revision() {
        synchronized (lock) {
            return mvcc.revision();
        }
    }

    public long compactedRevision() {
        synchronized (lock) {
            return mvcc.compactedRevision();
        }
    }

    public WatchHub watches() {
        return watches;
    }

    private StateMachineResult execute(Command command, List<WatchHub.Event> events) {
        return switch (command.getOperationCase()) {
            case PUT -> put(command.getPut(), events);
            case DELETE -> delete(command.getDelete(), events);
            case TXN -> txn(command.getTxn(), events);
            case COMPACT -> compact(command.getCompact().getRevision());
            case LEASE_GRANT -> {
                var grant = command.getLeaseGrant();
                leases.grant(grant.getId(), grant.getTtlSecs(), grant.getDeadlineEpochMillis());
                yield new StateMachineResult.Lease(
                        grant.getId(), grant.getTtlSecs(), mvcc.revision());
            }
            case LEASE_REVOKE -> revoke(command.getLeaseRevoke().getId(), events);
            case LEASE_KEEP_ALIVE -> {
                var keepAlive = command.getLeaseKeepAlive();
                leases.keepAlive(keepAlive.getId(), keepAlive.getDeadlineEpochMillis());
                LeaseRegistry.Lease lease = leases.snapshot().stream()
                        .filter(candidate -> candidate.id() == keepAlive.getId())
                        .findFirst()
                        .orElseThrow();
                yield new StateMachineResult.Lease(
                        lease.id(), lease.ttlSeconds(), mvcc.revision());
            }
            case LEASE_EXPIRE -> expire(command.getLeaseExpire(), events);
            case OPERATION_NOT_SET -> throw new IllegalArgumentException("command has no operation");
        };
    }

    private StateMachineResult.Put put(PutRequest request, List<WatchHub.Event> events) {
        ByteKey key = key(request.getKey());
        if (!leases.exists(request.getLease())) {
            throw new IllegalArgumentException("unknown lease " + request.getLease());
        }
        long revision = mvcc.beginMutation();
        MvccStore.Put put =
                mvcc.put(key, request.getValue().toByteArray(), request.getLease(), revision);
        leases.attach(key, request.getLease());
        events.add(new WatchHub.Event(WatchHub.Type.PUT, put.current(), put.previous()));
        return new StateMachineResult.Put(put.current(), put.previous(), revision);
    }

    private StateMachineResult.Delete delete(
            DeleteRequest request, List<WatchHub.Event> events) {
        List<ByteKey> keys =
                mvcc.liveKeys(key(request.getKey()), optionalKey(request.getRangeend()));
        if (keys.isEmpty()) {
            return new StateMachineResult.Delete(List.of(), mvcc.revision());
        }
        long revision = mvcc.beginMutation();
        List<KvRecord> previous = new ArrayList<>(keys.size());
        for (ByteKey key : keys) {
            MvccStore.Delete deleted = mvcc.delete(key, revision);
            leases.detach(key);
            previous.add(deleted.previous());
            events.add(new WatchHub.Event(
                    WatchHub.Type.DELETE, deleted.tombstone(), deleted.previous()));
        }
        return new StateMachineResult.Delete(previous, revision);
    }

    private StateMachineResult.Txn txn(TxnRequest request, List<WatchHub.Event> events) {
        boolean succeeded = request.getCompareList().stream().allMatch(this::compare);
        List<RequestOp> branch = succeeded ? request.getSuccessList() : request.getFailureList();
        validateOperations(branch);

        boolean mutates = branch.stream().anyMatch(this::willMutate);
        long revision = mutates ? mvcc.beginMutation() : mvcc.revision();
        List<StateMachineResult> operationResults = new ArrayList<>();
        for (RequestOp operation : branch) {
            switch (operation.getRequestCase()) {
                case PUT -> {
                    PutRequest put = operation.getPut();
                    ByteKey key = key(put.getKey());
                    MvccStore.Put stored =
                            mvcc.put(key, put.getValue().toByteArray(), put.getLease(), revision);
                    leases.attach(key, put.getLease());
                    events.add(new WatchHub.Event(
                            WatchHub.Type.PUT, stored.current(), stored.previous()));
                    operationResults.add(
                            new StateMachineResult.Put(stored.current(), stored.previous(), revision));
                }
                case DELETE -> {
                    DeleteRequest delete = operation.getDelete();
                    List<ByteKey> keys =
                            mvcc.liveKeys(key(delete.getKey()), optionalKey(delete.getRangeend()));
                    List<KvRecord> previous = new ArrayList<>();
                    for (ByteKey key : keys) {
                        MvccStore.Delete deleted = mvcc.delete(key, revision);
                        leases.detach(key);
                        previous.add(deleted.previous());
                        events.add(new WatchHub.Event(
                                WatchHub.Type.DELETE, deleted.tombstone(), deleted.previous()));
                    }
                    operationResults.add(new StateMachineResult.Delete(previous, revision));
                }
                case RANGE -> {
                    MvccStore.Range range = rangeUnlocked(operation.getRange());
                    operationResults.add(new StateMachineResult.Range(range, revision));
                }
                case REQUEST_NOT_SET ->
                        throw new IllegalArgumentException("transaction operation is empty");
            }
        }
        return new StateMachineResult.Txn(succeeded, operationResults, revision);
    }

    private StateMachineResult.Compact compact(long revision) {
        int removed = mvcc.compact(revision);
        watches.compact(revision);
        return new StateMachineResult.Compact(revision, removed, mvcc.revision());
    }

    private StateMachineResult.Lease revoke(long id, List<WatchHub.Event> events) {
        List<ByteKey> keys = leases.revoke(id);
        deleteLeaseKeys(keys, events);
        return new StateMachineResult.Lease(id, 0, mvcc.revision());
    }

    private StateMachineResult.Lease expire(
            LeaseExpireCommand command, List<WatchHub.Event> events) {
        List<ByteKey> keys = new ArrayList<>();
        for (long id : command.getIdsList()) {
            if (leases.hasLease(id)) {
                keys.addAll(leases.revoke(id));
            }
        }
        deleteLeaseKeys(keys.stream().distinct().toList(), events);
        return new StateMachineResult.Lease(0, 0, mvcc.revision());
    }

    private void deleteLeaseKeys(List<ByteKey> keys, List<WatchHub.Event> events) {
        List<ByteKey> live = keys.stream().filter(key -> mvcc.current(key) != null).toList();
        if (live.isEmpty()) {
            return;
        }
        long revision = mvcc.beginMutation();
        for (ByteKey key : live) {
            MvccStore.Delete deleted = mvcc.delete(key, revision);
            events.add(new WatchHub.Event(
                    WatchHub.Type.DELETE, deleted.tombstone(), deleted.previous()));
        }
    }

    private boolean compare(Compare compare) {
        ByteKey key = key(compare.getKey());
        KvRecord actual = mvcc.current(key);
        int ordering = switch (compare.getTarget()) {
            case KEY -> Arrays.compareUnsigned(
                    key.bytes(), compare.getValue().toByteArray());
            case VALUE -> Arrays.compareUnsigned(
                    actual == null ? new byte[0] : actual.value(),
                    compare.getValue().toByteArray());
            case CREATE_REVISION -> Long.compare(
                    actual == null ? 0 : actual.createRevision(),
                    decodeLittleEndianLong(compare.getValue()));
            case MOD_REVISION -> Long.compare(
                    actual == null ? 0 : actual.modificationRevision(),
                    decodeLittleEndianLong(compare.getValue()));
            case VERSION -> Long.compare(
                    actual == null ? 0 : actual.version(),
                    decodeLittleEndianLong(compare.getValue()));
            case UNRECOGNIZED -> throw new IllegalArgumentException("unknown compare target");
        };
        return switch (compare.getResult()) {
            case EQUAL -> ordering == 0;
            case GREATER -> ordering > 0;
            case LESS -> ordering < 0;
            case UNRECOGNIZED -> throw new IllegalArgumentException("unknown compare result");
        };
    }

    private void validateOperations(List<RequestOp> operations) {
        for (RequestOp operation : operations) {
            switch (operation.getRequestCase()) {
                case PUT -> {
                    key(operation.getPut().getKey());
                    if (!leases.exists(operation.getPut().getLease())) {
                        throw new IllegalArgumentException(
                                "unknown lease " + operation.getPut().getLease());
                    }
                }
                case DELETE -> key(operation.getDelete().getKey());
                case RANGE -> key(operation.getRange().getKey());
                case REQUEST_NOT_SET ->
                        throw new IllegalArgumentException("transaction operation is empty");
            }
        }
    }

    private boolean willMutate(RequestOp operation) {
        return switch (operation.getRequestCase()) {
            case PUT -> true;
            case DELETE -> !mvcc.liveKeys(
                    key(operation.getDelete().getKey()),
                    optionalKey(operation.getDelete().getRangeend())).isEmpty();
            case RANGE, REQUEST_NOT_SET -> false;
        };
    }

    private MvccStore.Range rangeUnlocked(RangeRequest request) {
        return mvcc.range(
                key(request.getKey()),
                optionalKey(request.getRangeend()),
                request.getRevision(),
                request.getLimit(),
                request.getKeysonly());
    }

    private static ByteKey key(ByteString bytes) {
        return ByteKey.of(bytes.toByteArray());
    }

    private static ByteKey optionalKey(ByteString bytes) {
        return bytes.isEmpty() ? null : ByteKey.of(bytes.toByteArray());
    }

    private static long decodeLittleEndianLong(ByteString value) {
        if (value.size() != Long.BYTES) {
            throw new IllegalArgumentException("numeric compare value must contain 8 LE bytes");
        }
        return ByteBuffer.wrap(value.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}
