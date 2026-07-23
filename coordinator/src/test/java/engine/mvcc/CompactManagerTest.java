package engine.mvcc;

import engine.coordinator.v1.CoordinatorOuterClass.*;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompactManagerTest {

    private MvccStore store;
    private CompactManager mgr;

    @BeforeEach
    void setUp() {
        store = new MvccStore();
        mgr = new CompactManager();
        mgr.mvccStore = store;
    }

    private long rev() {
        long r = store.currentRevision() + 1;
        store.setCurrentRevision(r);
        return r;
    }

    private static ByteString bs(String s) { return ByteString.copyFromUtf8(s); }

    @Test
    void testCompact() {
        store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        store.putAtRevision(bs("k1"), bs("v2"), rev(), 0);
        store.putAtRevision(bs("k1"), bs("v3"), rev(), 0);
        store.putAtRevision(bs("k2"), bs("v1"), rev(), 0);

        CompactResponse r = mgr.applyCompact(CompactRequest.newBuilder().setRevision(3).build());
        assertEquals(3, r.getRevision());
        assertEquals(2, r.getRemoved());
        assertEquals(3, store.compactRevision());

        assertTrue(store.get(bs("k1"), 3).isPresent());
    }

    @Test
    void testCompactBeyondCurrent() {
        store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        assertThrows(IllegalStateException.class, () ->
            mgr.applyCompact(CompactRequest.newBuilder().setRevision(100).build()));
    }

    @Test
    void testCompactBelowExisting() {
        store.putAtRevision(bs("k1"), bs("v1"), rev(), 0);
        store.putAtRevision(bs("k1"), bs("v2"), rev(), 0);

        mgr.applyCompact(CompactRequest.newBuilder().setRevision(1).build());
        assertThrows(IllegalStateException.class, () ->
            mgr.applyCompact(CompactRequest.newBuilder().setRevision(1).build()));
    }
}
