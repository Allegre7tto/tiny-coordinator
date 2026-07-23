package dev.talent.server.state;

public final class CompactedException extends RuntimeException {
    private final long compactedRevision;

    public CompactedException(long compactedRevision) {
        super("requested revision has been compacted at " + compactedRevision);
        this.compactedRevision = compactedRevision;
    }

    public long compactedRevision() {
        return compactedRevision;
    }
}
