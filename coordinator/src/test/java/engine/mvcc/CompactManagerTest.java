package engine.mvcc;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompactManagerTest {

    private MvccStore mvccStore;
    private CompactManager compactManager;

    @BeforeEach
    void setUp() {
        mvccStore = new MvccStore();
        compactManager = new CompactManager();
        compactManager.mvccStore = mvccStore;
    }

    @Test
    void testCompact() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1")); // rev 1
        mvccStore.put("key1", ByteString.copyFromUtf8("v2")); // rev 2
        mvccStore.put("key1", ByteString.copyFromUtf8("v3")); // rev 3
        mvccStore.put("key2", ByteString.copyFromUtf8("v1")); // rev 4

        // compact(3) removes revisions < 3: key1 rev 1,2 = 2 entries
        CompactManager.CompactResponse response = compactManager.compact(3);

        assertEquals(3, response.compactedRevision());
        assertEquals(2, response.removedVersions());

        assertEquals(3, compactManager.getCompactRevision());

        // key1 at rev 3 should still be accessible
        var kv = mvccStore.get("key1", 3);
        assertTrue(kv.isPresent());

        // Querying below compact revision throws
        assertThrows(IllegalStateException.class, () -> mvccStore.get("key1", 1));
    }

    @Test
    void testCompactThrowsOnOldRevision() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1"));
        compactManager.compact(1);

        assertThrows(IllegalStateException.class, () -> compactManager.compact(0));
    }

    @Test
    void testCompactThrowsOnFutureRevision() {
        mvccStore.put("key1", ByteString.copyFromUtf8("v1"));

        assertThrows(IllegalStateException.class, () -> compactManager.compact(100));
    }

    @Test
    void testGetCurrentRevision() {
        assertEquals(0, compactManager.getCurrentRevision());
        mvccStore.put("key1", ByteString.copyFromUtf8("v1"));
        assertEquals(1, compactManager.getCurrentRevision());
    }

    @Test
    void testGetCompactRevision() {
        assertEquals(0, compactManager.getCompactRevision());
        mvccStore.put("key1", ByteString.copyFromUtf8("v1"));
        compactManager.compact(1);
        assertEquals(1, compactManager.getCompactRevision());
    }
}
