package dev.talent.server;

import dev.talent.raft.RaftEffect;
import dev.talent.proto.raft.AppendEntriesRequest;
import dev.talent.proto.raft.AppendEntriesResponse;
import dev.talent.proto.raft.InstallSnapshotRequest;
import dev.talent.proto.raft.InstallSnapshotResponse;
import dev.talent.proto.raft.RaftGrpc;
import dev.talent.proto.raft.RequestVoteRequest;
import dev.talent.proto.raft.RequestVoteResponse;
import dev.talent.runtime.RaftProtocolMapper;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@GrpcService
@Singleton
public class PeerGrpcService extends RaftGrpc.RaftImplBase {
    @Inject
    CoordinatorNode node;

    @Override
    public void requestVote(
            RequestVoteRequest request, StreamObserver<RequestVoteResponse> response) {
        long sender = request.getCandidateId();
        complete(
                node.runtime().receive(sender, RaftProtocolMapper.fromProto(sender, request)),
                RaftEffect.VoteResponse.class,
                RaftProtocolMapper::toProto,
                response);
    }

    @Override
    public void appendEntries(
            AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> response) {
        long sender = request.getLeaderId();
        complete(
                node.runtime().receive(sender, RaftProtocolMapper.fromProto(sender, request)),
                RaftEffect.AppendResponse.class,
                RaftProtocolMapper::toProto,
                response);
    }

    @Override
    public void installSnapshot(
            InstallSnapshotRequest request, StreamObserver<InstallSnapshotResponse> response) {
        long sender = request.getLeaderId();
        complete(
                node.runtime().receive(sender, RaftProtocolMapper.fromProto(sender, request)),
                RaftEffect.SnapshotResponse.class,
                RaftProtocolMapper::toProto,
                response);
    }

    private static <R extends RaftEffect.Rpc, P> void complete(
            CompletionStage<RaftEffect.Rpc> stage,
            Class<R> responseType,
            Function<R, P> mapper,
            StreamObserver<P> observer) {
        stage.whenComplete((rpc, problem) -> {
            if (problem != null) {
                observer.onError(problem);
            } else if (!responseType.isInstance(rpc)) {
                observer.onError(new IllegalStateException("unexpected Raft response " + rpc));
            } else {
                observer.onNext(mapper.apply(responseType.cast(rpc)));
                observer.onCompleted();
            }
        });
    }
}
