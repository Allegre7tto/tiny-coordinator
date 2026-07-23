package dev.talent.raft;

import java.util.List;
import java.util.Set;

public sealed interface RaftEvent {
    record Tick(long nowMillis) implements RaftEvent {}

    record Propose(long correlationId, byte[] command) implements RaftEvent {
        public Propose {
            command = command.clone();
        }
        @Override public byte[] command() { return command.clone(); }
    }

    record Read(long correlationId) implements RaftEvent {}

    record ChangeMembership(
            long correlationId, Set<Long> voters, Set<Long> learners) implements RaftEvent {
        public ChangeMembership {
            voters = Set.copyOf(voters);
            learners = Set.copyOf(learners);
        }
    }

    record RequestVote(
            long from, long term, long candidateId, long lastLogIndex, long lastLogTerm)
            implements RaftEvent {}

    record RequestVoteResult(long from, long term, boolean granted) implements RaftEvent {}

    record AppendEntries(
            long from,
            long term,
            long leaderId,
            long previousLogIndex,
            long previousLogTerm,
            List<RaftLogEntry> entries,
            long leaderCommit,
            long readContext) implements RaftEvent {
        public AppendEntries {
            entries = List.copyOf(entries);
        }
    }

    record AppendEntriesResult(
            long from,
            long term,
            boolean success,
            long matchIndex,
            long conflictTerm,
            long conflictIndex,
            long readContext) implements RaftEvent {}

    record InstallSnapshot(long from, long term, long leaderId, RaftSnapshot snapshot)
            implements RaftEvent {}

    record SnapshotInstalled(long from, long term, RaftSnapshot snapshot, boolean success)
            implements RaftEvent {}

    record InstallSnapshotResult(
            long from, long term, boolean installed, long lastIncludedIndex)
            implements RaftEvent {}

    record SnapshotCreated(RaftSnapshot snapshot) implements RaftEvent {}
}
