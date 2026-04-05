package engine.coordinator;

import engine.coordinator.v1.CoordinatorGrpc;
import engine.coordinator.v1.CoordinatorOuterClass.*;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implements coordinator.proto for external clients.
 *
 * Architecture (v3):
 *   - Writes → StateMachineDriver.propose() → C++ Raft → committed → apply
 *   - Reads  → KvStore.get() (local memory, no Raft round trip)
 *   - Watch  → WatchManager (observes KvStore apply events)
 *   - Lease  → StateMachineDriver.propose(LEASE_GRANT/REVOKE) for persistence
 */
@GrpcService
public class CoordinatorGrpcService extends CoordinatorGrpc.CoordinatorImplBase {

    private static final Logger LOG = Logger.getLogger(CoordinatorGrpcService.class);

    @Inject StateMachineDriver driver;
    @Inject KvStore            kvStore;
    @Inject WatchManager       watchManager;
    @Inject LeaseManager       leaseManager;

    // ── KV ────────────────────────────────────────────────────────────────────

    @Override
    public void put(PutRequest req, StreamObserver<PutResponse> resp) {
        try {
            long revision = driver.propose(StateMachineDriver.OP_PUT, req)
                    .get(10, TimeUnit.SECONDS);
            resp.onNext(PutResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder().setRevision(revision))
                    .build());
            resp.onCompleted();
        } catch (Exception e) {
            LOG.errorf(e, "Put failed: key=%s", req.getKey().toStringUtf8());
            resp.onError(io.grpc.Status.UNAVAILABLE.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void get(GetRequest req, StreamObserver<GetResponse> resp) {
        try {
            resp.onNext(kvStore.get(req));
            resp.onCompleted();
        } catch (Exception e) {
            LOG.errorf(e, "Get failed");
            resp.onError(io.grpc.Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest req, StreamObserver<DeleteResponse> resp) {
        try {
            long revision = driver.propose(StateMachineDriver.OP_DELETE, req)
                    .get(10, TimeUnit.SECONDS);
            resp.onNext(DeleteResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder().setRevision(revision))
                    .build());
            resp.onCompleted();
        } catch (Exception e) {
            LOG.errorf(e, "Delete failed");
            resp.onError(io.grpc.Status.UNAVAILABLE.withCause(e).asRuntimeException());
        }
    }

    // ── Watch (bidirectional streaming) ───────────────────────────────────────

    @Override
    public StreamObserver<WatchRequest> watch(StreamObserver<WatchResponse> downstream) {
        List<Long> activeWatches = new ArrayList<>();

        return new StreamObserver<>() {
            @Override
            public void onNext(WatchRequest req) {
                if (req.hasCreateRequest()) {
                    WatchCreateRequest cr = req.getCreateRequest();
                    long id = watchManager.register(
                            cr.getKey().toStringUtf8(),
                            cr.getRangeEnd().toStringUtf8(),
                            cr.getStartRevision(),
                            downstream::onNext);
                    activeWatches.add(id);
                    downstream.onNext(WatchResponse.newBuilder()
                            .setWatchId(id).setCreated(true).build());

                } else if (req.hasCancelRequest()) {
                    long id = req.getCancelRequest().getWatchId();
                    watchManager.cancel(id);
                    activeWatches.remove(id);
                }
            }

            @Override
            public void onError(Throwable t) {
                activeWatches.forEach(watchManager::cancel);
                LOG.debugf("Watch stream error: %s", t.getMessage());
            }

            @Override
            public void onCompleted() {
                activeWatches.forEach(watchManager::cancel);
                downstream.onCompleted();
            }
        };
    }

    // ── Lease ─────────────────────────────────────────────────────────────────

    @Override
    public void leaseGrant(LeaseGrantRequest req, StreamObserver<LeaseGrantResponse> resp) {
        try {
            // Persist through Raft
            driver.propose(StateMachineDriver.OP_LEASE_GRANT, req)
                    .get(10, TimeUnit.SECONDS);
            resp.onNext(LeaseGrantResponse.newBuilder()
                    .setId(req.getId()).setTtl(req.getTtl()).build());
            resp.onCompleted();
        } catch (Exception e) {
            resp.onNext(LeaseGrantResponse.newBuilder().setError(e.getMessage()).build());
            resp.onCompleted();
        }
    }

    @Override
    public void leaseRevoke(LeaseRevokeRequest req, StreamObserver<LeaseRevokeResponse> resp) {
        try {
            // Delete attached keys first
            for (String key : leaseManager.keysOf(req.getId())) {
                driver.propose(StateMachineDriver.OP_DELETE,
                        DeleteRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .build());
            }
            // Then revoke the lease
            driver.propose(StateMachineDriver.OP_LEASE_REVOKE, req)
                    .get(10, TimeUnit.SECONDS);
            resp.onNext(LeaseRevokeResponse.getDefaultInstance());
            resp.onCompleted();
        } catch (Exception e) {
            resp.onError(io.grpc.Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public StreamObserver<LeaseKeepAliveRequest> leaseKeepAlive(
            StreamObserver<LeaseKeepAliveResponse> resp) {
        return new StreamObserver<>() {
            @Override
            public void onNext(LeaseKeepAliveRequest req) {
                try {
                    long ttl = leaseManager.keepAlive(req.getId());
                    resp.onNext(LeaseKeepAliveResponse.newBuilder()
                            .setId(req.getId()).setTtl(ttl).build());
                } catch (Exception e) {
                    resp.onError(io.grpc.Status.NOT_FOUND
                            .withDescription(e.getMessage()).asRuntimeException());
                }
            }
            @Override public void onError(Throwable t) { LOG.debugf("KeepAlive error: %s", t.getMessage()); }
            @Override public void onCompleted()        { resp.onCompleted(); }
        };
    }

    // ── Cluster ───────────────────────────────────────────────────────────────

    @Override
    public void memberList(MemberListRequest req, StreamObserver<MemberListResponse> resp) {
        resp.onNext(MemberListResponse.getDefaultInstance());
        resp.onCompleted();
    }
}
