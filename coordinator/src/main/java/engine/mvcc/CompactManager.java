package engine.mvcc;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Compact 管理器。支持手动和自动清理历史版本。
 */
@ApplicationScoped
public class CompactManager {

    private static final Logger LOG = Logger.getLogger(CompactManager.class);
    private static final long MAX_REVISIONS_TO_KEEP = 100_000;

    @Inject MvccStore mvccStore;

    public record CompactResponse(long compactedRevision, int removedVersions) {}

    /**
     * 手动触发 compact
     */
    public CompactResponse compact(long revision) {
        if (revision < mvccStore.compactRevision()) {
            throw new IllegalStateException(
                "cannot compact revision " + revision +
                ", already compacted to " + mvccStore.compactRevision());
        }
        if (revision > mvccStore.currentRevision()) {
            throw new IllegalStateException(
                "cannot compact revision " + revision +
                ", current revision is " + mvccStore.currentRevision());
        }

        LOG.infof("Compacting to revision %d", revision);
        int removed = mvccStore.compact(revision);
        LOG.infof("Compacted: removed %d old versions", removed);
        return new CompactResponse(revision, removed);
    }

    @Scheduled(every = "1m")
    void autoCompact() {
        long current = mvccStore.currentRevision();
        long compactTo = current - MAX_REVISIONS_TO_KEEP;
        if (compactTo > mvccStore.compactRevision()) {
            LOG.debugf("Auto-compacting to revision %d", compactTo);
            compact(compactTo);
        }
    }

    public long getCompactRevision() { return mvccStore.compactRevision(); }
    public long getCurrentRevision() { return mvccStore.currentRevision(); }
}
