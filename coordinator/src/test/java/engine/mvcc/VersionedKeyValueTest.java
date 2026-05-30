package engine.mvcc;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VersionedKeyValueTest {

    private VersionedKeyValue vkv;

    @BeforeEach
    void setUp() {
        vkv = new VersionedKeyValue("testKey");
    }

    @Test
    void testPutAndGet() {
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);

        var entry = vkv.latest();
        assertNotNull(entry);
        assertEquals("v1", entry.value().toStringUtf8());
        assertEquals(1, entry.createRevision());
        assertEquals(1, entry.modRevision());
        assertEquals(1, entry.version());
    }

    @Test
    void testMultipleVersions() {
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);
        vkv.put(ByteString.copyFromUtf8("v2"), 2, 0);
        vkv.put(ByteString.copyFromUtf8("v3"), 3, 0);

        var latest = vkv.latest();
        assertNotNull(latest);
        assertEquals("v3", latest.value().toStringUtf8());
        assertEquals(3, latest.version());
        assertEquals(1, latest.createRevision());
        assertEquals(3, latest.modRevision());
    }

    @Test
    void testGetAtRevision() {
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);
        vkv.put(ByteString.copyFromUtf8("v2"), 2, 0);
        vkv.put(ByteString.copyFromUtf8("v3"), 3, 0);

        var entry1 = vkv.getAtRevision(1);
        assertNotNull(entry1);
        assertEquals("v1", entry1.value().toStringUtf8());

        var entry2 = vkv.getAtRevision(2);
        assertNotNull(entry2);
        assertEquals("v2", entry2.value().toStringUtf8());

        var entry3 = vkv.getAtRevision(3);
        assertNotNull(entry3);
        assertEquals("v3", entry3.value().toStringUtf8());
    }

    @Test
    void testGetAtRevisionNotFound() {
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);

        // floorEntry(5) returns rev 1 (key exists at rev 5 with value from rev 1)
        var entry5 = vkv.getAtRevision(5);
        assertNotNull(entry5);
        assertEquals("v1", entry5.value().toStringUtf8());

        // Query before any version exists
        var entry0 = vkv.getAtRevision(0);
        assertNull(entry0);
    }

    @Test
    void testTombstone() {
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);
        vkv.tombstone(2);

        var latest = vkv.latest();
        assertNull(latest); // tombstone returns null

        var entry1 = vkv.getAtRevision(1);
        assertNotNull(entry1);
        assertEquals("v1", entry1.value().toStringUtf8());

        var entry2 = vkv.getAtRevision(2);
        assertNull(entry2); // tombstone
    }

    @Test
    void testTombstoneOnEmpty() {
        // Should not throw
        vkv.tombstone(1);
        assertNull(vkv.latest());
    }

    @Test
    void testTombstoneAfterTombstone() {
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);
        vkv.tombstone(2);
        vkv.tombstone(3); // should be ignored

        var entry = vkv.getAtRevision(3);
        assertNull(entry);
    }

    @Test
    void testGetVersionRange() {
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);
        vkv.put(ByteString.copyFromUtf8("v2"), 2, 0);
        vkv.put(ByteString.copyFromUtf8("v3"), 3, 0);
        vkv.put(ByteString.copyFromUtf8("v4"), 4, 0);

        var range = vkv.getVersionRange(2, 3);
        assertEquals(2, range.size());
        assertTrue(range.containsKey(2L));
        assertTrue(range.containsKey(3L));
    }

    @Test
    void testGetVersionsAfter() {
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);
        vkv.put(ByteString.copyFromUtf8("v2"), 2, 0);
        vkv.put(ByteString.copyFromUtf8("v3"), 3, 0);

        var after = vkv.getVersionsAfter(2);
        assertEquals(1, after.size());
        assertTrue(after.containsKey(3L));
    }

    @Test
    void testCompact() {
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);
        vkv.put(ByteString.copyFromUtf8("v2"), 2, 0);
        vkv.put(ByteString.copyFromUtf8("v3"), 3, 0);

        int removed = vkv.compact(3);
        assertEquals(2, removed); // removed rev 1 and 2

        var entry1 = vkv.getAtRevision(1);
        assertNull(entry1);

        var entry3 = vkv.getAtRevision(3);
        assertNotNull(entry3);
    }

    @Test
    void testVersionCount() {
        assertEquals(0, vkv.versionCount());
        vkv.put(ByteString.copyFromUtf8("v1"), 1, 0);
        assertEquals(1, vkv.versionCount());
        vkv.put(ByteString.copyFromUtf8("v2"), 2, 0);
        assertEquals(2, vkv.versionCount());
    }

    @Test
    void testIsTombstone() {
        var real = new VersionedKeyValue.KvEntry(
            ByteString.copyFromUtf8("v1"), 1, 1, 1, 0);
        assertFalse(real.isTombstone());

        var tomb = new VersionedKeyValue.KvEntry(
            ByteString.EMPTY, 1, 2, 2, 0);
        assertTrue(tomb.isTombstone());
    }
}
