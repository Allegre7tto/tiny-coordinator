package dev.talent.runtime;

import dev.talent.raft.RaftEffect;
import dev.talent.proto.raft.RaftGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public final class GrpcRaftTransport implements RaftTransport, AutoCloseable {
    private final Map<Long, ManagedChannel> channels;
    private final Map<Long, RaftGrpc.RaftStub> stubs;

    public GrpcRaftTransport(Map<Long, String> peerTargets) {
        channels = new ConcurrentHashMap<>();
        stubs = new ConcurrentHashMap<>();
        peerTargets.forEach(this::addPeer);
    }

    public void addPeer(long id, String target) {
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        ManagedChannel previous = channels.put(id, channel);
        stubs.put(id, RaftGrpc.newStub(channel));
        if (previous != null) {
            previous.shutdown();
        }
    }

    public void removePeer(long id) {
        stubs.remove(id);
        ManagedChannel channel = channels.remove(id);
        if (channel != null) {
            channel.shutdown();
        }
    }

    @Override
    public CompletionStage<RaftEffect.Rpc> request(long target, RaftEffect.Rpc request) {
        RaftGrpc.RaftStub stub = stubs.get(target);
        if (stub == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("unknown Raft peer " + target));
        }
        CompletableFuture<RaftEffect.Rpc> result = new CompletableFuture<>();
        switch (request) {
            case RaftEffect.VoteRequest vote -> stub.requestVote(
                    RaftProtocolMapper.toProto(vote),
                    observer(result, RaftProtocolMapper::fromProto));
            case RaftEffect.AppendRequest append -> stub.appendEntries(
                    RaftProtocolMapper.toProto(append),
                    observer(result, RaftProtocolMapper::fromProto));
            case RaftEffect.SnapshotRequest snapshot -> stub.installSnapshot(
                    RaftProtocolMapper.toProto(snapshot),
                    observer(result, RaftProtocolMapper::fromProto));
            default -> result.completeExceptionally(
                    new IllegalArgumentException("transport cannot send a response as a request"));
        }
        return result;
    }

    private static <T> StreamObserver<T> observer(
            CompletableFuture<RaftEffect.Rpc> result,
            java.util.function.Function<T, RaftEffect.Rpc> mapper) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                result.complete(mapper.apply(value));
            }

            @Override
            public void onError(Throwable problem) {
                result.completeExceptionally(problem);
            }

            @Override
            public void onCompleted() {
                if (!result.isDone()) {
                    result.completeExceptionally(
                            new IllegalStateException("peer completed without a response"));
                }
            }
        };
    }

    @Override
    public void close() throws InterruptedException {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
        }
        for (ManagedChannel channel : channels.values()) {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        }
    }
}
