package engine.client;

import engine.log.v1.RaftLogGrpc;
import engine.log.v1.Log.*;

import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;

import com.google.protobuf.ByteString;

import java.util.concurrent.CompletableFuture;

/**
 * Reactive gRPC client for the C++ RaftLog service.
 * Java uses this to:
 *   - Propose log entries (writes go through Raft consensus)
 *   - Subscribe to committed entries (for state machine replay)
 *   - Save/Load snapshots
 *   - Check cluster status
 */
@ApplicationScoped
public class RaftLogClient {

    @GrpcClient("cpp-raft")
    RaftLogGrpc.RaftLogBlockingStub blockingStub;

    @GrpcClient("cpp-raft")
    RaftLogGrpc.RaftLogStub asyncStub;

    // ── Propose ───────────────────────────────────────────────────────────────

    public CompletableFuture<ProposeResp> propose(byte[] data) {
        CompletableFuture<ProposeResp> future = new CompletableFuture<>();
        asyncStub.propose(ProposeReq.newBuilder()
                .setData(ByteString.copyFrom(data))
                .build(),
                new StreamObserver<>() {
                    @Override public void onNext(ProposeResp resp) { future.complete(resp); }
                    @Override public void onError(Throwable t)     { future.completeExceptionally(t); }
                    @Override public void onCompleted()            { if (!future.isDone()) future.completeExceptionally(new RuntimeException("Stream completed without response")); }
                });
        return future;
    }

    // ── Subscribe to committed entries (reactive stream) ──────────────────────

    public Multi<CommittedEntry> subscribeCommitted(long startIndex) {
        SubscribeReq req = SubscribeReq.newBuilder()
                .setStartIndex(startIndex)
                .build();

        return Multi.createFrom().<CommittedEntry>emitter(emitter -> {
            asyncStub.subscribeCommitted(req, new StreamObserver<>() {
                @Override public void onNext(CommittedEntry entry) { emitter.emit(entry); }
                @Override public void onError(Throwable t)        { emitter.fail(t); }
                @Override public void onCompleted()               { emitter.complete(); }
            });
        }).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    public CompletableFuture<Boolean> saveSnapshot(long lastIndex, long lastTerm, byte[] data) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        StreamObserver<SnapshotChunk> writer = asyncStub.saveSnapshot(
                new StreamObserver<>() {
                    @Override public void onNext(SaveSnapshotResp resp) { future.complete(resp.getOk()); }
                    @Override public void onError(Throwable t)         { future.completeExceptionally(t); }
                    @Override public void onCompleted()                { if (!future.isDone()) future.complete(true); }
                });

        writer.onNext(SnapshotChunk.newBuilder()
                .setLastIndex(lastIndex)
                .setLastTerm(lastTerm)
                .setData(ByteString.copyFrom(data))
                .setDone(true)
                .build());
        writer.onCompleted();
        return future;
    }

    public Multi<SnapshotChunk> loadSnapshot() {
        return Multi.createFrom().<SnapshotChunk>emitter(emitter -> {
            asyncStub.loadSnapshot(LoadSnapshotReq.getDefaultInstance(), new StreamObserver<>() {
                @Override public void onNext(SnapshotChunk chunk) { emitter.emit(chunk); }
                @Override public void onError(Throwable t)       { emitter.fail(t); }
                @Override public void onCompleted()              { emitter.complete(); }
            });
        }).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    // ── Status ────────────────────────────────────────────────────────────────

    public StatusResp status() {
        return blockingStub.status(StatusReq.getDefaultInstance());
    }
}
