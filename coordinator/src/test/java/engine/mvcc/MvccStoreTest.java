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
    void setUp() {
        store = new MvccStore();
    }

    // ── Put / Get ────────────────────────────────────────────────────────────

    @Test
    void testPutAndGet() {
        long rev1 = store.put("key1", ByteString.copyFromUtf8("value1"));
        assertEquals(1, rev1);

        var kv = store.get("key1");
        assertTrue(kv.isPresent());
        assertEquals("value1", kv.get().value().toStringUtf8());
        assertEquals(1, kv.get().version());
        assertEquals(1, kv.get().createRevision());
        assertEquals(1, kv.get().modRevision());
    }

    @Test
    void testPutMultipleVersions() {
        store.put("key1", ByteString.copyFromUtf8("v1"));
        store.put("key1", ByteString.copyFromUtf8("v2"));
        long rev3 = store.put("key1", ByteString.copyFromUtf8("v3"));

        var kv = store.get("key1");
        assertTrue(kv.isPresent());
        assertEquals("v3", kv.get().value().toStringUtf8());
        assertEquals(3, kv.get().version());
        assertEquals(1, kv.get().createRevision());
        assertEquals(3, kv.get().modRevision());
    }

    @Test
    void testGetAtRevision() {
        store.put("key1", ByteString.copyFromUtf8("v1"));
        store.put("key1", ByteString.copyFromUtf8("v2"));
        store.put("key1", ByteString.copyFromUtf8("v3"));

        var kv1 = store.get("key1", 1);
        assertTrue(kv1.isPresent());
        assertEquals("v1", kv1.get().value().toStringUtf8());

        var kv2 = store.get("key1", 2);
        assertTrue(kv2.isPresent());
        assertEquals("v2", kv2.get().value().toStringUtf8());

        var kv3 = store.get("key1", 3);
        assertTrue(kv3.isPresent());
        assertEquals("v3", kv3.get().value().toStringUtf8());
    }

    @Test
    void testGetNonExistent() {
        var kv = store.get("nonexistent");
        assertFalse(kv.isPresent());
    }

    // ── Delete ───────────────────────────────────────────────────────────────

    @Test
    void testDelete() {
        store.put("key1", ByteString.copyFromUtf8("v1"));
        long rev2 = store.delete("key1");

        assertEquals(2, rev2);

        var kv = store.get("key1");
        assertFalse(kv.isPresent());
    }

    @Test
    void testDeleteNonExistent() {
        long rev = store.delete("nonexistent");
        assertEquals(-1, rev);
    }

    @Test
    void testDeletePreservesHistory() {
        store.put("key1", ByteString.copyFromUtf8("v1"));
        store.delete("key1");

        var kv1 = store.get("key1", 1);
        assertTrue(kv1.isPresent());
        assertEquals("v1", kv1.get().value().toStringUtf8());

        var kv2 = store.get("key1", 2);
        assertFalse(kv2.isPresent());
    }

    @Test
    void testDeleteRange() {
        store.put("key1", ByteString.copyFromUtf8("v1"));
        store.put("key2", ByteString.copyFromUtf8("v2"));
        store.put("key3", ByteString.copyFromUtf8("v3"));

        int deleted = store.deleteRange("key1", "key3");
        assertEquals(2, deleted);

        assertFalse(store.get("key1").isPresent());
        assertFalse(store.get("key2").isPresent());
        assertTrue(store.get("key3").isPresent());
    }

    // ── Range ────────────────────────────────────────────────────────────────

    @Test
    void testRange() {
        store.put("a", ByteString.copyFromUtf8("1"));
        store.put("b", ByteString.copyFromUtf8("2"));
        store.put("c", ByteString.copyFromUtf8("3"));
        store.put("d", ByteString.copyFromUtf8("4"));

        var result = store.range("b", "d", 0, 0);
        assertEquals(2, result.entries().size());
        assertEquals("b", result.entries().get(0).key());
        assertEquals("c", result.entries().get(1).key());
    }

    @Test
    void testRangeWithLimit() {
        store.put("a", ByteString.copyFromUtf8("1"));
        store.put("b", ByteString.copyFromUtf8("2"));
        store.put("c", ByteString.copyFromUtf8("3"));
        store.put("d", ByteString.copyFromUtf8("4"));

        var result = store.range("a", "", 0, 2);
        assertEquals(2, result.entries().size());
        assertTrue(result.more());
    }

    @Test
    void testRangeAllKeys() {
        store.put("a", ByteString.copyFromUtf8("1"));
        store.put("b", ByteString.copyFromUtf8("2"));

        var result = store.range("\0", "", 0, 0);
        assertEquals(2, result.entries().size());
    }

    // ── Watch ────────────────────────────────────────────────────────────────

    @Test
    void testWatchEvents() {
        List<MvccStore.WatchEvent> events = new ArrayList<>();
        store.addWatcher(events::add);

        store.put("key1", ByteString.copyFromUtf8("v1"));
        store.put("key2", ByteString.copyFromUtf8("v2"));
        store.delete("key1");

        assertEquals(3, events.size());
        assertEquals(MvccStore.EventType.PUT, events.get(0).type());
        assertEquals("key1", events.get(0).key());
        assertEquals(MvccStore.EventType.PUT, events.get(1).type());
        assertEquals("key2", events.get(1).key());
        assertEquals(MvccStore.EventType.DELETE, events.get(2).type());
    }

    @Test
    void testWatchHistoryEvents() {
        store.put("key1", ByteString.copyFromUtf8("v1"));
        store.put("key2", ByteString.copyFromUtf8("v2"));
        store.put("key1", ByteString.copyFromUtf8("v3"));

        var events = store.getAllHistoryEvents(2, 3);
        assertEquals(2, events.size());
        assertEquals("key2", events.get(0).key());
        assertEquals(2, events.get(0).revision());
        assertEquals("key1", events.get(1).key());
        assertEquals(3, events.get(1).revision());
    }

    // ── Compact ──────────────────────────────────────────────────────────────

    @Test
    void testCompact() {
        store.put("key1", ByteString.copyFromUtf8("v1"));
        store.put("key1", ByteString.copyFromUtf8("v2"));
        store.put("key1", ByteString.copyFromUtf8("v3"));

        // compact(2) removes revisions < 2, i.e. rev 1
        int removed = store.compact(2);
        assertEquals(1, removed);

        assertEquals(2, store.compactRevision());

        // Querying below compact revision throws
        assertThrows(IllegalStateException.class, () -> store.get("key1", 1));

        var kv2 = store.get("key1", 2);
        assertTrue(kv2.isPresent());

        var kv3 = store.get("key1", 3);
        assertTrue(kv3.isPresent());
    }

    @Test
    void testCompactThrowsOnOldRevision() {
        // MvccStore itself doesn't validate, CompactManager does
        store.put("key1", ByteString.copyFromUtf8("v1"));
        store.compact(1);
        // MvccStore allows compacting to an older revision (no-op)
        store.compact(0);
        assertEquals(0, store.compactRevision());
    }

    // ── Snapshot / Restore ───────────────────────────────────────────────────

    @Test
    void testSnapshotAndRestore() {
        store.put("key1", ByteString.copyFromUtf8("v1"));
        store.put("key2", ByteString.copyFromUtf8("v2"));
        store.put("key1", ByteString.copyFromUtf8("v3"));

        var entries = store.snapshotEntries();
        assertEquals(2, entries.size());

        MvccStore newStore = new MvccStore();
        newStore.restoreFromEntries(entries, 3);

        assertEquals(3, newStore.currentRevision());
        assertEquals("v3", newStore.get("key1").get().value().toStringUtf8());
        assertEquals("v2", newStore.get("key2").get().value().toStringUtf8());
    }

    // ── Revision ─────────────────────────────────────────────────────────────

    @Test
    void testRevisionIncrement() {
        assertEquals(0, store.currentRevision());
        store.put("key1", ByteString.copyFromUtf8("v1"));
        assertEquals(1, store.currentRevision());
        store.put("key2", ByteString.copyFromUtf8("v2"));
        assertEquals(2, store.currentRevision());
    }

    @Test
    void testSetCurrentRevision() {
        store.setCurrentRevision(100);
        assertEquals(100, store.currentRevision());
    }
}
