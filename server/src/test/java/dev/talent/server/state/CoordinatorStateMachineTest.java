package dev.talent.server.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import dev.talent.proto.coordinator.Command;
import dev.talent.proto.coordinator.Compare;
import dev.talent.proto.coordinator.CompactRequest;
import dev.talent.proto.coordinator.DeleteRequest;
import dev.talent.proto.coordinator.LeaseGrantCommand;
import dev.talent.proto.coordinator.LeaseRevokeRequest;
import dev.talent.proto.coordinator.PutRequest;
import dev.talent.proto.coordinator.RangeRequest;
import dev.talent.proto.coordinator.RequestOp;
import dev.talent.proto.coordinator.TxnRequest;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CoordinatorStateMachineTest {
    private static final ByteString KEY = bytes("key");

    @Test
    void raftNoopAdvancesAppliedIndexWithoutConsumingMvccRevision() {
        CoordinatorStateMachine state = state();
        state.advance(1, 1);
        apply(state, 2, Command.newBuilder().setPut(put(KEY, "value")).build());

        assertEquals(2, state.appliedIndex());
        assertEquals(1, state.revision());
    }

    @Test
    void compactionRetainsLiveAnchorVersion() {
        CoordinatorStateMachine state = state();
        apply(state, 1, Command.newBuilder().setPut(put(KEY, "v1")).build());
        apply(state, 2, Command.newBuilder()
                .setCompact(CompactRequest.newBuilder().setRevision(1))
                .build());

        MvccStore.Range current = state.range(range(KEY));
        assertEquals(1, current.count());
        assertEquals("v1", new String(current.values().getFirst().value()));
        assertEquals(1, state.revision(), "compaction does not consume an MVCC revision");
    }

    @Test
    void deleteWatchCarriesPreviousValue() {
        CoordinatorStateMachine state = state();
        List<WatchHub.Event> received = new ArrayList<>();
        state.watches().subscribe(
                ByteKey.of(KEY.toByteArray()),
                null,
                0,
                true,
                received::addAll,
                problem -> {
                    throw new AssertionError(problem);
                });

        apply(state, 1, Command.newBuilder().setPut(put(KEY, "value")).build());
        apply(state, 2, Command.newBuilder()
                .setDelete(DeleteRequest.newBuilder().setKey(KEY))
                .build());

        WatchHub.Event deleted = received.stream()
                .filter(event -> event.type() == WatchHub.Type.DELETE)
                .findFirst()
                .orElseThrow();
        assertNotNull(deleted.previous());
        assertEquals("value", new String(deleted.previous().value()));
        assertTrue(deleted.current().tombstone());
    }

    @Test
    void revokeDeletesAttachedKeysAtOneRevision() {
        CoordinatorStateMachine state = state();
        apply(state, 1, Command.newBuilder()
                .setLeaseGrant(LeaseGrantCommand.newBuilder()
                        .setId(9)
                        .setTtlSecs(10)
                        .setDeadlineEpochMillis(5_000))
                .build());
        apply(state, 2, Command.newBuilder()
                .setPut(put(KEY, "leased").toBuilder().setLease(9))
                .build());
        apply(state, 3, Command.newBuilder()
                .setLeaseRevoke(LeaseRevokeRequest.newBuilder().setId(9))
                .build());

        assertEquals(0, state.range(range(KEY)).count());
        assertEquals(2, state.revision());
    }

    @Test
    void snapshotRestoresOriginalMvccMetadataAndLeaseState() {
        CoordinatorStateMachine original = state();
        apply(original, 1, Command.newBuilder().setPut(put(KEY, "v1")).build());
        apply(original, 2, Command.newBuilder().setPut(put(KEY, "v2")).build());

        CoordinatorStateMachine restored = state();
        restored.restore(original.snapshot());

        KvRecord value = restored.range(range(KEY)).values().getFirst();
        assertEquals(1, value.createRevision());
        assertEquals(2, value.modificationRevision());
        assertEquals(2, value.version());
        assertEquals(2, restored.appliedIndex());
    }

    @Test
    void readOnlyTransactionDoesNotConsumeRevision() {
        CoordinatorStateMachine state = state();
        apply(state, 1, Command.newBuilder().setPut(put(KEY, "v")).build());
        TxnRequest txn = TxnRequest.newBuilder()
                .addSuccess(RequestOp.newBuilder().setRange(range(KEY)))
                .build();
        apply(state, 2, Command.newBuilder().setRequestId(22).setTxn(txn).build());

        assertEquals(1, state.revision());
        StateMachineResult.Txn result =
                assertInstanceOf(StateMachineResult.Txn.class, state.result(22));
        assertEquals(1, result.revision());
    }

    @Test
    void transactionUsesLittleEndianInt64AndOneWriteRevision() {
        CoordinatorStateMachine state = state();
        long index = 0;
        for (int version = 1; version <= 128; version++) {
            apply(state, ++index, Command.newBuilder()
                    .setPut(put(KEY, "v" + version))
                    .build());
        }

        Compare versionIs128 = Compare.newBuilder()
                .setTarget(Compare.CompareTarget.VERSION)
                .setResult(Compare.CompareResult.EQUAL)
                .setKey(KEY)
                .setValue(littleEndian(128))
                .build();
        TxnRequest txn = TxnRequest.newBuilder()
                .addCompare(versionIs128)
                .addSuccess(RequestOp.newBuilder().setPut(put(bytes("a"), "1")))
                .addSuccess(RequestOp.newBuilder().setPut(put(bytes("b"), "2")))
                .build();
        apply(state, ++index, Command.newBuilder().setRequestId(77).setTxn(txn).build());

        StateMachineResult.Txn result =
                assertInstanceOf(StateMachineResult.Txn.class, state.result(77));
        assertTrue(result.succeeded());
        assertEquals(129, result.revision());
        assertEquals(
                129,
                state.range(range(bytes("a"))).values().getFirst().modificationRevision());
        assertEquals(
                129,
                state.range(range(bytes("b"))).values().getFirst().modificationRevision());
    }

    private static CoordinatorStateMachine state() {
        return new CoordinatorStateMachine(new WatchHub(Runnable::run, 16));
    }

    private static void apply(CoordinatorStateMachine state, long index, Command command) {
        state.apply(index, 1, command.toByteArray());
    }

    private static PutRequest put(ByteString key, String value) {
        return PutRequest.newBuilder().setKey(key).setValue(bytes(value)).build();
    }

    private static RangeRequest range(ByteString key) {
        return RangeRequest.newBuilder().setKey(key).build();
    }

    private static ByteString bytes(String value) {
        return ByteString.copyFromUtf8(value);
    }

    private static ByteString littleEndian(long value) {
        return ByteString.copyFrom(
                ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array());
    }
}
