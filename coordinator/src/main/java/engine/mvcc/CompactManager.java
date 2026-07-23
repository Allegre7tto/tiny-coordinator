package engine.mvcc;

import engine.coordinator.v1.CoordinatorOuterClass.*;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CompactManager {

    private static final Logger LOG = Logger.getLogger(CompactManager.class);
    private static final long MAX_REVISIONS_TO_KEEP = 100_000;

    @Inject MvccStore mvccStore;

    public CompactResponse applyCompact(CompactRequest req) {
        long rev = req.getRevision();
        if (rev <= mvccStore.compactRevision()) {
            throw new IllegalStateException(
                "cannot compact: already at " + mvccStore.compactRevision());
        }
        if (rev > mvccStore.currentRevision()) {
            throw new IllegalStateException(
                "cannot compact: beyond current revision " + mvccStore.currentRevision());
        }
        int removed = mvccStore.compact(rev);
        LOG.infof("Compacted to revision %d, removed %d versions", rev, removed);
        return CompactResponse.newBuilder()
            .setRevision(rev)
            .setRemoved(removed)
            .build();
    }

    @Scheduled(every = "1m")
    void autoCompact() {
        long current = mvccStore.currentRevision();
        long compactTo = current - MAX_REVISIONS_TO_KEEP;
        if (compactTo > mvccStore.compactRevision()) {
            LOG.debugf("Auto-compacting to revision %d", compactTo);
            mvccStore.compact(compactTo);
        }
    }

    public long getCompactrev() { return mvccStore.compactRevision(); }
    public long getCurrentRevision() { return mvccStore.currentRevision(); }
}
