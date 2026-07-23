package dev.talent.raft;

import java.util.Objects;

public record RaftSnapshot(
        long lastIncludedIndex,
        long lastIncludedTerm,
        ClusterConfiguration configuration,
        byte[] stateMachine) {

    public RaftSnapshot {
        if (lastIncludedIndex < 0 || lastIncludedTerm < 0) {
            throw new IllegalArgumentException("snapshot position must be non-negative");
        }
        configuration = Objects.requireNonNull(configuration, "configuration");
        stateMachine = Objects.requireNonNull(stateMachine, "stateMachine").clone();
    }

    @Override
    public byte[] stateMachine() {
        return stateMachine.clone();
    }
}
