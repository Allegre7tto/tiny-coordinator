package dev.talent.raft;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public record RaftLogEntry(
        long index,
        long term,
        Kind kind,
        byte[] command,
        Set<Long> oldVoters,
        Set<Long> newVoters,
        Set<Long> learners) {

    public enum Kind {
        COMMAND,
        NOOP,
        JOINT_CONFIG,
        STABLE_CONFIG
    }

    public RaftLogEntry {
        if (index < 0 || term < 0) {
            throw new IllegalArgumentException("index and term must be non-negative");
        }
        kind = Objects.requireNonNull(kind, "kind");
        command = command == null ? new byte[0] : command.clone();
        oldVoters = oldVoters == null ? Set.of() : Set.copyOf(oldVoters);
        newVoters = newVoters == null ? Set.of() : Set.copyOf(newVoters);
        learners = learners == null ? Set.of() : Set.copyOf(learners);
    }

    @Override
    public byte[] command() {
        return command.clone();
    }

    public boolean sameContent(RaftLogEntry other) {
        return term == other.term
                && kind == other.kind
                && Arrays.equals(command, other.command)
                && oldVoters.equals(other.oldVoters)
                && newVoters.equals(other.newVoters)
                && learners.equals(other.learners);
    }

    public static RaftLogEntry command(long index, long term, byte[] command) {
        return new RaftLogEntry(index, term, Kind.COMMAND, command, Set.of(), Set.of(), Set.of());
    }

    public static RaftLogEntry noop(long index, long term) {
        return new RaftLogEntry(index, term, Kind.NOOP, null, Set.of(), Set.of(), Set.of());
    }
}
