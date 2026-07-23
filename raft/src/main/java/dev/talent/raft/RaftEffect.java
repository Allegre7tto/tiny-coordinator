package dev.talent.raft;

import java.util.List;

public sealed interface RaftEffect {
    sealed interface Rpc permits VoteRequest, VoteResponse, AppendRequest, AppendResponse,
            SnapshotRequest, SnapshotResponse {}

    record Send(long target, Rpc rpc) implements RaftEffect {}

    record VoteRequest(long term, long candidateId, long lastLogIndex, long lastLogTerm)
            implements Rpc {}

    record VoteResponse(long term, boolean granted) implements Rpc {}

    record AppendRequest(
            long term,
            long leaderId,
            long previousLogIndex,
            long previousLogTerm,
            List<RaftLogEntry> entries,
            long leaderCommit,
            long readContext) implements Rpc {
        public AppendRequest {
            entries = List.copyOf(entries);
        }
    }

    record AppendResponse(
            long term,
            boolean success,
            long matchIndex,
            long conflictTerm,
            long conflictIndex,
            long readContext) implements Rpc {}

    record SnapshotRequest(long term, long leaderId, RaftSnapshot snapshot) implements Rpc {}

    record SnapshotResponse(long term, boolean installed, long lastIncludedIndex) implements Rpc {}

    record Persist(PersistentState state) implements RaftEffect {}

    record SaveSnapshot(RaftSnapshot snapshot) implements RaftEffect {}

    record Apply(long index, long term, byte[] command) implements RaftEffect {
        public Apply {
            command = command.clone();
        }
        @Override public byte[] command() { return command.clone(); }
    }

    record AdvanceApplied(long index, long term) implements RaftEffect {}

    record RestoreSnapshot(long leaderId, long term, RaftSnapshot snapshot) implements RaftEffect {}

    record ProposalCommitted(long correlationId, long index) implements RaftEffect {}

    record ReadReady(long correlationId, long readIndex) implements RaftEffect {}

    record Rejected(long correlationId, Error error, Long leaderHint) implements RaftEffect {}

    record RoleChanged(RaftCore.Role role, long term, Long leaderId) implements RaftEffect {}

    enum Error {
        NOT_LEADER,
        MEMBERSHIP_CHANGE_IN_PROGRESS,
        MEMBER_NOT_CAUGHT_UP,
        INVALID_CONFIGURATION
    }

    record PersistentState(
            long term,
            Long votedFor,
            long commitIndex,
            long snapshotIndex,
            long snapshotTerm,
            ClusterConfiguration configuration,
            List<RaftLogEntry> log) {
        public PersistentState {
            log = List.copyOf(log);
        }
    }
}
