package engine.mvcc;

import engine.coordinator.v1.CoordinatorOuterClass.*;
import engine.coordinator.LeaseManager;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TxnManagerTest {

    private MvccStore store;
    private TxnManager txn;

    @BeforeEach
    void setUp() {
        store = new MvccStore();
        txn = new TxnManager();
        txn.mvccStore = store;
        txn.leaseManager = new LeaseManager();
    }

    private long nextRev() {
        long r = store.currentRevision() + 1;
        store.setCurrentRevision(r);
        return r;
    }

    private TxnRequest txnReq(engine.coordinator.v1.CoordinatorOuterClass.Compare cmp,
                               RequestOp success, RequestOp failure) {
        var b = TxnRequest.newBuilder().addCompare(cmp).addSuccess(success);
        if (failure != null) b.addFailure(failure);
        return b.build();
    }

    private TxnRequest justSuccess(RequestOp... ops) {
        var b = TxnRequest.newBuilder();
        for (var op : ops) b.addSuccess(op);
        return b.build();
    }

    private engine.coordinator.v1.CoordinatorOuterClass.Compare keyExists(ByteString key) {
        return engine.coordinator.v1.CoordinatorOuterClass.Compare.newBuilder()
            .setTarget(engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareTarget.KEY)
            .setResult(engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.EQUAL)
            .setKey(key).build();
    }

    private engine.coordinator.v1.CoordinatorOuterClass.Compare valCmp(ByteString key, ByteString val,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult r) {
        return engine.coordinator.v1.CoordinatorOuterClass.Compare.newBuilder()
            .setTarget(engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareTarget.VALUE)
            .setResult(r).setKey(key).setValue(val).build();
    }

    private static ByteString encodeInt64LE(long v) {
        byte[] b = new byte[8];
        java.nio.ByteBuffer.wrap(b).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(v);
        return ByteString.copyFrom(b);
    }

    private engine.coordinator.v1.CoordinatorOuterClass.Compare revCmp(ByteString key, long v,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareTarget t,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult r) {
        return engine.coordinator.v1.CoordinatorOuterClass.Compare.newBuilder()
            .setTarget(t).setResult(r).setKey(key)
            .setValue(encodeInt64LE(v))
            .build();
    }

    private RequestOp putOp(ByteString key, ByteString value) {
        return RequestOp.newBuilder()
            .setPut(PutRequest.newBuilder().setKey(key).setValue(value)).build();
    }

    private RequestOp delOp(ByteString key) {
        return RequestOp.newBuilder()
            .setDelete(DeleteRequest.newBuilder().setKey(key)).build();
    }

    private static ByteString bs(String s) { return ByteString.copyFromUtf8(s); }

    // ── Tests ────────────────────────────────────────────────────────────────

    @Test
    void testTxnPut() {
        TxnResponse r = txn.applyTxn(justSuccess(putOp(bs("k1"), bs("v1"))));
        assertTrue(r.getSucceeded());
        assertTrue(store.get(bs("k1")).isPresent());
    }

    @Test
    void testTxnDelete() {
        store.putAtRevision(bs("k1"), bs("v1"), nextRev(), 0);
        TxnResponse r = txn.applyTxn(justSuccess(delOp(bs("k1"))));
        assertTrue(r.getSucceeded());
        assertFalse(store.get(bs("k1")).isPresent());
    }

    @Test
    void testTxnConditionTrue() {
        store.putAtRevision(bs("k1"), bs("v1"), nextRev(), 0);
        TxnResponse r = txn.applyTxn(
            txnReq(keyExists(bs("k1")), putOp(bs("k2"), bs("success")), putOp(bs("k2"), bs("failure"))));
        assertTrue(r.getSucceeded());
        assertEquals("success", store.get(bs("k2")).get().value().toStringUtf8());
    }

    @Test
    void testTxnConditionFalse() {
        TxnResponse r = txn.applyTxn(
            txnReq(keyExists(bs("k1")), putOp(bs("k2"), bs("success")), putOp(bs("k2"), bs("failure"))));
        assertFalse(r.getSucceeded());
        assertEquals("failure", store.get(bs("k2")).get().value().toStringUtf8());
    }

    @Test
    void testTxnCompareValue() {
        store.putAtRevision(bs("k1"), bs("v1"), nextRev(), 0);
        var cmp = valCmp(bs("k1"), bs("v1"), engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.EQUAL);
        TxnResponse r = txn.applyTxn(txnReq(cmp, putOp(bs("k2"), bs("matched")), null));
        assertTrue(r.getSucceeded());
        assertEquals("matched", store.get(bs("k2")).get().value().toStringUtf8());
    }

    @Test
    void testTxnCompareModRevision() {
        store.putAtRevision(bs("k1"), bs("v1"), nextRev(), 0);
        var cmp = revCmp(bs("k1"), 1,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareTarget.MOD_REVISION,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.EQUAL);
        TxnResponse r = txn.applyTxn(txnReq(cmp, putOp(bs("k2"), bs("ok")), null));
        assertTrue(r.getSucceeded());
    }

    @Test
    void testTxnCompareVersion() {
        store.putAtRevision(bs("k1"), bs("v1"), nextRev(), 0);
        store.putAtRevision(bs("k1"), bs("v2"), nextRev(), 0);
        var cmp = revCmp(bs("k1"), 2,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareTarget.VERSION,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.EQUAL);
        TxnResponse r = txn.applyTxn(txnReq(cmp, putOp(bs("k2"), bs("ok")), null));
        assertTrue(r.getSucceeded());
    }

    @Test
    void testTxnMultipleOps() {
        TxnResponse r = txn.applyTxn(justSuccess(
            putOp(bs("k1"), bs("v1")), putOp(bs("k2"), bs("v2")), putOp(bs("k3"), bs("v3"))));
        assertTrue(r.getSucceeded());
        assertEquals(3, r.getResponsesCount());
        assertEquals("v1", store.get(bs("k1")).get().value().toStringUtf8());
        assertEquals("v2", store.get(bs("k2")).get().value().toStringUtf8());
        assertEquals("v3", store.get(bs("k3")).get().value().toStringUtf8());
    }

    @Test
    void testTxnCompareGreater() {
        store.putAtRevision(bs("k1"), bs("v1"), nextRev(), 0);
        var cmp = revCmp(bs("k1"), 0,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareTarget.VERSION,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.GREATER);
        TxnResponse r = txn.applyTxn(txnReq(cmp, putOp(bs("k2"), bs("ok")), null));
        assertTrue(r.getSucceeded());
    }

    @Test
    void testTxnCompareLess() {
        store.putAtRevision(bs("k1"), bs("v1"), nextRev(), 0);
        var cmp = revCmp(bs("k1"), 2,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareTarget.VERSION,
            engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.LESS);
        TxnResponse r = txn.applyTxn(txnReq(cmp, putOp(bs("k2"), bs("ok")), null));
        assertTrue(r.getSucceeded());
    }
}
