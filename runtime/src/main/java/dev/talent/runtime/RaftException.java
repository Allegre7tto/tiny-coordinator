package dev.talent.runtime;

import dev.talent.raft.RaftEffect;

public final class RaftException extends RuntimeException {
    private final RaftEffect.Error error;
    private final Long leaderHint;

    public RaftException(RaftEffect.Error error, Long leaderHint) {
        super(error + (leaderHint == null ? "" : " (leader " + leaderHint + ")"));
        this.error = error;
        this.leaderHint = leaderHint;
    }

    public RaftEffect.Error error() {
        return error;
    }

    public Long leaderHint() {
        return leaderHint;
    }
}
