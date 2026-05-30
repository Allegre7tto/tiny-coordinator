package engine.mvcc;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TxnManagerTest {

    private MvccStore mvccStore;
    private TxnManager txnManager;

    @BeforeEach
    void setUp() {
        mvccStore = new MvccStore();
        txnManager = new TxnManager();
        txnManager.mvccStore = mvccStore;
    }

    @Test
    void testTxnPut() {
        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key1",
                ByteString.copyFromUtf8("value1"),
                "",
                0
            )),
            List.of()
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertTrue(response.succeeded());
        assertEquals(1, response.revisions().size());

        var kv = mvccStore.get("key1");
        assertTrue(kv.isPresent());
        assertEquals("value1", kv.get().value().toStringUtf8());
    }

    @Test
    void testTxnDelete() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1"));

        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(),
            List.of(new TxnManager.Op(
                TxnManager.OpType.DELETE,
                "key1",
                ByteString.EMPTY,
                "",
                0
            )),
            List.of()
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertTrue(response.succeeded());
        assertFalse(mvccStore.get("key1").isPresent());
    }

    @Test
    void testTxnConditionTrue() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1"));

        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(new TxnManager.Compare(
                TxnManager.CompareTarget.KEY,
                TxnManager.CompareResult.EQUAL,
                "key1",
                ByteString.EMPTY
            )),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key2",
                ByteString.copyFromUtf8("success"),
                "",
                0
            )),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key2",
                ByteString.copyFromUtf8("failure"),
                "",
                0
            ))
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertTrue(response.succeeded());
        assertEquals("success", mvccStore.get("key2").get().value().toStringUtf8());
    }

    @Test
    void testTxnConditionFalse() {
        // key1 does not exist

        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(new TxnManager.Compare(
                TxnManager.CompareTarget.KEY,
                TxnManager.CompareResult.EQUAL,
                "key1",
                ByteString.EMPTY
            )),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key2",
                ByteString.copyFromUtf8("success"),
                "",
                0
            )),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key2",
                ByteString.copyFromUtf8("failure"),
                "",
                0
            ))
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertFalse(response.succeeded());
        assertEquals("failure", mvccStore.get("key2").get().value().toStringUtf8());
    }

    @Test
    void testTxnCompareValue() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1"));

        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(new TxnManager.Compare(
                TxnManager.CompareTarget.VALUE,
                TxnManager.CompareResult.EQUAL,
                "key1",
                ByteString.copyFromUtf8("v1")
            )),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key2",
                ByteString.copyFromUtf8("matched"),
                "",
                0
            )),
            List.of()
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertTrue(response.succeeded());
        assertEquals("matched", mvccStore.get("key2").get().value().toStringUtf8());
    }

    @Test
    void testTxnCompareModRevision() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1")); // rev 1

        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(new TxnManager.Compare(
                TxnManager.CompareTarget.MOD_REVISION,
                TxnManager.CompareResult.EQUAL,
                "key1",
                ByteString.copyFromUtf8("1")
            )),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key2",
                ByteString.copyFromUtf8("matched"),
                "",
                0
            )),
            List.of()
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertTrue(response.succeeded());
    }

    @Test
    void testTxnCompareVersion() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1")); // version 1
        mvccStore.put("key1", ByteString.copyFromUtf8("v2")); // version 2

        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(new TxnManager.Compare(
                TxnManager.CompareTarget.VERSION,
                TxnManager.CompareResult.EQUAL,
                "key1",
                ByteString.copyFromUtf8("2")
            )),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key2",
                ByteString.copyFromUtf8("matched"),
                "",
                0
            )),
            List.of()
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertTrue(response.succeeded());
    }

    @Test
    void testTxnMultipleOps() {
        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(),
            List.of(
                new TxnManager.Op(TxnManager.OpType.PUT, "key1", ByteString.copyFromUtf8("v1"), "", 0),
                new TxnManager.Op(TxnManager.OpType.PUT, "key2", ByteString.copyFromUtf8("v2"), "", 0),
                new TxnManager.Op(TxnManager.OpType.PUT, "key3", ByteString.copyFromUtf8("v3"), "", 0)
            ),
            List.of()
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertTrue(response.succeeded());
        assertEquals(3, response.revisions().size());
        assertEquals("v1", mvccStore.get("key1").get().value().toStringUtf8());
        assertEquals("v2", mvccStore.get("key2").get().value().toStringUtf8());
        assertEquals("v3", mvccStore.get("key3").get().value().toStringUtf8());
    }

    @Test
    void testTxnCompareGreater() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1")); // version 1

        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(new TxnManager.Compare(
                TxnManager.CompareTarget.VERSION,
                TxnManager.CompareResult.GREATER,
                "key1",
                ByteString.copyFromUtf8("0")
            )),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key2",
                ByteString.copyFromUtf8("matched"),
                "",
                0
            )),
            List.of()
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertTrue(response.succeeded());
    }

    @Test
    void testTxnCompareLess() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1")); // version 1

        TxnManager.TxnRequest request = new TxnManager.TxnRequest(
            List.of(new TxnManager.Compare(
                TxnManager.CompareTarget.VERSION,
                TxnManager.CompareResult.LESS,
                "key1",
                ByteString.copyFromUtf8("2")
            )),
            List.of(new TxnManager.Op(
                TxnManager.OpType.PUT,
                "key2",
                ByteString.copyFromUtf8("matched"),
                "",
                0
            )),
            List.of()
        );

        TxnManager.TxnResponse response = txnManager.execute(request);
        assertTrue(response.succeeded());
    }
}
