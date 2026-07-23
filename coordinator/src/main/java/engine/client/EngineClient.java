package engine.client;

import engine.coordinator.v1.CoordinatorGrpc;
import engine.coordinator.v1.CoordinatorOuterClass.*;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Java Client SDK for the Engine coordinator.
 *
 * Connects to the Java Quarkus coordinator (coordinator.proto).
 * Handles automatic retry on UNAVAILABLE (not-leader) responses.
 *
 * Usage:
 * <pre>{@code
 *   try (EngineClient client = new EngineClient(List.of("localhost:9000"))) {
 *       client.put("key", "value");
 *       List<KeyValue> kvs = client.get("key");
 *   }
 * }</pre>
 */
public class EngineClient implements AutoCloseable {

    private static final int    MAX_RETRIES   = 5;
    private static final long   RETRY_DELAY_MS = 200;

    private final List<String>  addrs;
    private int                 current = 0;
    private ManagedChannel      channel;
    private CoordinatorGrpc.CoordinatorBlockingStub stub;

    public EngineClient(List<String> addrs) {
        if (addrs == null || addrs.isEmpty()) throw new IllegalArgumentException("no addresses");
        this.addrs = addrs;
        connect(addrs.get(0));
    }

    private void connect(String addr) {
        if (channel != null) channel.shutdownNow();
        channel = ManagedChannelBuilder.forTarget(addr).usePlaintext().build();
        stub    = CoordinatorGrpc.newBlockingStub(channel);
    }

    // ── KV ────────────────────────────────────────────────────────────────────

    public void put(String key, String value) {
        put(key, value, 0);
    }

    public void put(String key, String value, long leaseId) {
        retry(() -> stub.put(PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .setLease(leaseId)
                .build()));
    }

    public List<KeyValue> get(String key) {
        GetResponse r = retry(() -> stub.get(GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build()));
        return r.getKvsList();
    }

    public List<KeyValue> range(String key, String rangeEnd, int limit) {
        GetResponse r = retry(() -> stub.get(GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setRangeend(ByteString.copyFromUtf8(rangeEnd))
                .setLimit(limit)
                .build()));
        return r.getKvsList();
    }

    public void delete(String key) {
        retry(() -> stub.delete(DeleteRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build()));
    }

    // ── Lease ─────────────────────────────────────────────────────────────────

    public long leaseGrant(long ttlSeconds) {
        LeaseGrantResponse r = retry(() -> stub.leaseGrant(LeaseGrantRequest.newBuilder()
                .setTtl(ttlSeconds).build()));
        return r.getId();
    }

    public void leaseRevoke(long id) {
        retry(() -> stub.leaseRevoke(LeaseRevokeRequest.newBuilder().setId(id).build()));
    }

    // ── Watch (blocking, calls callback on each event) ────────────────────────

    public void watch(String key, String rangeEnd, Consumer<WatchResponse> cb) {
        // Streaming call — not retried automatically; caller reconnects if needed
        var requestObserver = io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
                channel.newCall(CoordinatorGrpc.getWatchMethod(), io.grpc.CallOptions.DEFAULT),
                new io.grpc.stub.StreamObserver<WatchResponse>() {
                    @Override public void onNext(WatchResponse r)  { cb.accept(r); }
                    @Override public void onError(Throwable t)     {}
                    @Override public void onCompleted()            {}
                });

        requestObserver.onNext(WatchRequest.newBuilder()
                .setCreatereq(WatchCreateRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(key))
                        .setRangeend(ByteString.copyFromUtf8(rangeEnd)))
                .build());
    }

    // ── Retry helper ──────────────────────────────────────────────────────────

    @FunctionalInterface
    private interface RpcCall<T> { T call(); }

    @SuppressWarnings("unchecked")
    private <T> T retry(RpcCall<T> fn) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            try {
                return fn.call();
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE) {
                    current = (current + 1) % addrs.size();
                    connect(addrs.get(current));
                    try { Thread.sleep(RETRY_DELAY_MS); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                } else {
                    throw e;
                }
            }
        }
        throw new RuntimeException("max retries exceeded");
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.shutdown();
            try { channel.awaitTermination(5, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
