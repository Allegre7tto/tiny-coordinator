package dev.talent.runtime;

import com.google.protobuf.ByteString;
import dev.talent.raft.ClusterConfiguration;
import dev.talent.raft.RaftEffect;
import dev.talent.raft.RaftEvent;
import dev.talent.raft.RaftLogEntry;
import dev.talent.raft.RaftSnapshot;
import dev.talent.proto.raft.AppendEntriesRequest;
import dev.talent.proto.raft.AppendEntriesResponse;
import dev.talent.proto.raft.EntryKind;
import dev.talent.proto.raft.InstallSnapshotRequest;
import dev.talent.proto.raft.InstallSnapshotResponse;
import dev.talent.proto.raft.LogEntry;
import dev.talent.proto.raft.RequestVoteRequest;
import dev.talent.proto.raft.RequestVoteResponse;
import dev.talent.proto.raft.VoterSet;

import java.util.Set;

public final class RaftProtocolMapper {
    private RaftProtocolMapper() {}

    public static RequestVoteRequest toProto(RaftEffect.VoteRequest request) {
        return RequestVoteRequest.newBuilder()
                .setTerm(request.term())
                .setCandidateId(request.candidateId())
                .setLastLogIndex(request.lastLogIndex())
                .setLastLogTerm(request.lastLogTerm())
                .build();
    }

    public static AppendEntriesRequest toProto(RaftEffect.AppendRequest request) {
        return AppendEntriesRequest.newBuilder()
                .setTerm(request.term())
                .setLeaderId(request.leaderId())
                .setPreviousLogIndex(request.previousLogIndex())
                .setPreviousLogTerm(request.previousLogTerm())
                .addAllEntries(request.entries().stream().map(RaftProtocolMapper::toProto).toList())
                .setLeaderCommit(request.leaderCommit())
                .setReadContext(request.readContext())
                .build();
    }

    public static InstallSnapshotRequest toProto(RaftEffect.SnapshotRequest request) {
        RaftSnapshot snapshot = request.snapshot();
        return InstallSnapshotRequest.newBuilder()
                .setTerm(request.term())
                .setLeaderId(request.leaderId())
                .setLastIncludedIndex(snapshot.lastIncludedIndex())
                .setLastIncludedTerm(snapshot.lastIncludedTerm())
                .setOldVoters(toProto(snapshot.configuration().oldVoters()))
                .setNewVoters(toProto(snapshot.configuration().newVoters()))
                .addAllLearners(snapshot.configuration().learners())
                .setStateMachine(ByteString.copyFrom(snapshot.stateMachine()))
                .build();
    }

    public static RaftEvent.RequestVote fromProto(long sender, RequestVoteRequest request) {
        return new RaftEvent.RequestVote(
                sender,
                request.getTerm(),
                request.getCandidateId(),
                request.getLastLogIndex(),
                request.getLastLogTerm());
    }

    public static RaftEvent.AppendEntries fromProto(long sender, AppendEntriesRequest request) {
        return new RaftEvent.AppendEntries(
                sender,
                request.getTerm(),
                request.getLeaderId(),
                request.getPreviousLogIndex(),
                request.getPreviousLogTerm(),
                request.getEntriesList().stream().map(RaftProtocolMapper::fromProto).toList(),
                request.getLeaderCommit(),
                request.getReadContext());
    }

    public static RaftEvent.InstallSnapshot fromProto(long sender, InstallSnapshotRequest request) {
        Set<Long> oldVoters = Set.copyOf(request.getOldVoters().getNodeIdsList());
        Set<Long> newVoters = Set.copyOf(request.getNewVoters().getNodeIdsList());
        ClusterConfiguration configuration =
                new ClusterConfiguration(oldVoters, newVoters, Set.copyOf(request.getLearnersList()));
        RaftSnapshot snapshot = new RaftSnapshot(
                request.getLastIncludedIndex(),
                request.getLastIncludedTerm(),
                configuration,
                request.getStateMachine().toByteArray());
        return new RaftEvent.InstallSnapshot(
                sender, request.getTerm(), request.getLeaderId(), snapshot);
    }

    public static RequestVoteResponse toProto(RaftEffect.VoteResponse response) {
        return RequestVoteResponse.newBuilder()
                .setTerm(response.term())
                .setGranted(response.granted())
                .build();
    }

    public static AppendEntriesResponse toProto(RaftEffect.AppendResponse response) {
        return AppendEntriesResponse.newBuilder()
                .setTerm(response.term())
                .setSuccess(response.success())
                .setMatchIndex(response.matchIndex())
                .setConflictTerm(response.conflictTerm())
                .setConflictIndex(response.conflictIndex())
                .setReadContext(response.readContext())
                .build();
    }

    public static InstallSnapshotResponse toProto(RaftEffect.SnapshotResponse response) {
        return InstallSnapshotResponse.newBuilder()
                .setTerm(response.term())
                .setInstalled(response.installed())
                .setLastIncludedIndex(response.lastIncludedIndex())
                .build();
    }

    public static RaftEffect.VoteResponse fromProto(RequestVoteResponse response) {
        return new RaftEffect.VoteResponse(response.getTerm(), response.getGranted());
    }

    public static RaftEffect.AppendResponse fromProto(AppendEntriesResponse response) {
        return new RaftEffect.AppendResponse(
                response.getTerm(),
                response.getSuccess(),
                response.getMatchIndex(),
                response.getConflictTerm(),
                response.getConflictIndex(),
                response.getReadContext());
    }

    public static RaftEffect.SnapshotResponse fromProto(InstallSnapshotResponse response) {
        return new RaftEffect.SnapshotResponse(
                response.getTerm(), response.getInstalled(), response.getLastIncludedIndex());
    }

    private static LogEntry toProto(RaftLogEntry entry) {
        return LogEntry.newBuilder()
                .setIndex(entry.index())
                .setTerm(entry.term())
                .setKind(EntryKind.forNumber(entry.kind().ordinal() + 1))
                .setData(ByteString.copyFrom(entry.command()))
                .setOldVoters(toProto(entry.oldVoters()))
                .setNewVoters(toProto(entry.newVoters()))
                .addAllLearners(entry.learners())
                .build();
    }

    private static RaftLogEntry fromProto(LogEntry entry) {
        if (entry.getKind() == EntryKind.ENTRY_KIND_UNSPECIFIED
                || entry.getKind() == EntryKind.UNRECOGNIZED) {
            throw new IllegalArgumentException("invalid Raft entry kind " + entry.getKind());
        }
        return new RaftLogEntry(
                entry.getIndex(),
                entry.getTerm(),
                RaftLogEntry.Kind.values()[entry.getKindValue() - 1],
                entry.getData().toByteArray(),
                Set.copyOf(entry.getOldVoters().getNodeIdsList()),
                Set.copyOf(entry.getNewVoters().getNodeIdsList()),
                Set.copyOf(entry.getLearnersList()));
    }

    private static VoterSet toProto(Set<Long> voters) {
        return VoterSet.newBuilder().addAllNodeIds(voters.stream().sorted().toList()).build();
    }
}
