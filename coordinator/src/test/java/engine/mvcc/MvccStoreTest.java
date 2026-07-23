package engine.mvcc;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MvccStoreTest {

    private MvccStore store;

    @BeforeEach
    void setUp() { store = new MvccStore(); }

    private long rev() {
        long r = store.currentRevision() + 1;
        store.setCurrentRevision(r);
        return r;
    }

    static ByteString bs(String s) { return ByteString.copyFromUtf8(s); }

    @Test
    void testPutAndGet() {
        long rev1 = store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        assertEquals(1, rev1);

        var kv = store.get(bs("k1"));
        assertTrue(kv.isPresent());
        assertEquals("v1", kv.get().value().toStringUtf8());
        assertEquals(1, kv.get().version());
        assertEquals(1, kv.get().createRevision());
        assertEquals(1, kv.get().modRevision());
    }

    @Test
    void testPutMultipleVersions() {
        store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        store.putAtRevision(bs("k1"), bs("v2"), rev(), 0);
        store.putAtRevision(bs("k1"), bs("v3"), rev(), 0);

        var kv = store.get(bs("k1"));
        assertTrue(kv.isPresent());
        assertEquals("v3", kv.get().value().toStringUtf8());
        assertEquals(3, kv.get().version());
        assertEquals(1, kv.get().createRevision());
        assertEquals(3, kv.get().modRevision());
    }

    @Test
    void testGetNonExistingKey() {
        assertFalse(store.get(bs("no")).isPresent());
    }

    @Test
    void testDelete() {
        store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        long rev2 = store.deleteAtRevision(bs("k1"), rev());
        assertEquals(2, rev2);
        assertFalse(store.get(bs("k1")).isPresent());
    }

    @Test
    void testDeleteNonExisting() {
        assertEquals(-1, store.deleteAtRevision(bs("no"), rev()));
    }

    @Test
    void testHistoricalRead() {
        store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        store.putAtRevision(bs("k1"), bs("v2"), rev(), 0);

        var kv1 = store.get(bs("k1"), 1);
        assertTrue(kv1.isPresent());
        assertEquals("v1", kv1.get().value().toStringUtf8());

        var kv2 = store.get(bs("k1"), 2);
        assertTrue(kv2.isPresent());
        assertEquals("v2", kv2.get().value().toStringUtf8());
    }

    @Test
    void testRangeAll() {
        store.putAtRevision(bs("a"), bs("va"), rev(), 0);
        store.putAtRevision(bs("b"), bs("vb"), rev(), 0);
        store.putAtRevision(bs("c"), bs("vc"), rev(), 0);

        var result = store.range(bs("a"), ByteString.EMPTY, 0, 0);
        assertEquals(3, result.entries().size());
        assertEquals(bs("a"), result.entries().get(0).key());
    }

    @Test
    void testRangePrefix() {
        store.putAtRevision(bs("/prefix/1"), bs("v1"), rev(), 0);
        store.putAtRevision(bs("/prefix/2"), bs("v2"), rev(), 0);
        store.putAtRevision(bs("/other"),   bs("v3"), rev(), 0);

        var result = store.range(bs("/prefix/"), bs("/prefix0"), 0, 0);
        assertEquals(2, result.entries().size());
    }

    @Test
    void testRangeWithLimit() {
        store.putAtRevision(bs("a"), bs("va"), rev(), 0);
        store.putAtRevision(bs("b"), bs("vb"), rev(), 0);
        store.putAtRevision(bs("c"), bs("vc"), rev(), 0);

        var result = store.range(bs("a"), ByteString.EMPTY, 0, 1);
        assertEquals(1, result.entries().size());
        assertTrue(result.more());
    }

    @Test
    void testDeleteRange() {
        store.putAtRevision(bs("a"), bs("va"), rev(), 0);
        store.putAtRevision(bs("b"), bs("vb"), rev(), 0);
        store.putAtRevision(bs("c"), bs("vc"), rev(), 0);

        int deleted = store.deleteRangeAtRevision(bs("a"), bs("c"), rev());
        assertEquals(2, deleted);
        assertFalse(store.get(bs("a")).isPresent());
        assertFalse(store.get(bs("b")).isPresent());
        assertTrue(store.get(bs("c")).isPresent());
    }

    @Test
    void testCompact() {
        store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        store.putAtRevision(bs("k1"), bs("v2"), rev(), 0);

        assertEquals(0, store.compactRevision());
        store.compact(1);
        assertEquals(1, store.compactRevision());
    }

    @Test
    void testSnapshot() {
        store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        store.putAtRevision(bs("k2"), bs("v2"), rev(), 0);

        var entries = store.snapshotEntries();
        assertEquals(2, entries.size());
    }

    @Test
    void testRestore() {
        store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        var snap = store.snapshotEntries();
        store.restoreFromEntries(snap, store.currentRevision());

        var kv = store.get(bs("k1"));
        assertTrue(kv.isPresent());
        assertEquals("v1", kv.get().value().toStringUtf8());
    }

    @Test
    void testBinaryKeys() {
        byte[] raw = new byte[] { (byte) 0x00, (byte) 0xFF, (byte) 0x80 };
        ByteString key = ByteString.copyFrom(raw);
        store.putAtRevision(key, bs("val"), rev(), 0);
        var kv = store.get(key);
        assertTrue(kv.isPresent());
        assertEquals("val", kv.get().value().toStringUtf8());
    }

    @Test
    void testOrdererBinaryKeys() {
        ByteString a = ByteString.copyFrom(new byte[] { (byte) 0x00 });
        ByteString b = ByteString.copyFrom(new byte[] { (byte) 0xFF });
        store.putAtRevision(a, bs("first"), rev(), 0);
        store.putAtRevision(b, bs("second"), rev(), 0);

        var result = store.range(a, ByteString.EMPTY, 0, 0);
        assertEquals(2, result.entries().size());
        assertEquals(a, result.entries().get(0).key());
        assertEquals(b, result.entries().get(1).key());
    }
}
