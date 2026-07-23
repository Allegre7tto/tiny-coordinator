package engine.coordinator;

import engine.coordinator.v1.CoordinatorGrpc;
import engine.coordinator.v1.CoordinatorOuterClass.*;
import engine.mvcc.CompactManager;
import engine.mvcc.TxnManager;

import com.google.protobuf.ByteString;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@GrpcService
public class CoordinatorGrpcService extends CoordinatorGrpc.CoordinatorImplBase {

    private static final Logger LOG = Logger.getLogger(CoordinatorGrpcService.class);

    @Inject StateMachineDriver driver;
    @Inject KvStore            kvStore;
    @Inject WatchManager       watchManager;
    @Inject LeaseManager       leaseManager;
    @Inject TxnManager         txnManager;
    @Inject CompactManager     compactManager;

    @Override
    public void put(PutRequest req, StreamObserver<PutResponse> resp) {
        try {
            requireLeader();
            long revision = driver.propose(StateMachineDriver.OP_PUT, req)
                    .get(10, TimeUnit.SECONDS);
            resp.onNext(PutResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setRevision(revision)
                        .setRaftterm(driver.getTerm()))
                    .build());
            resp.onCompleted();
        } catch (StatusException e) {
            resp.onError(e);
        } catch (Exception e) {
            LOG.errorf(e, "Put failed");
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
            requireLeader();
            long revision = driver.propose(StateMachineDriver.OP_DELETE, req)
                    .get(10, TimeUnit.SECONDS);
            resp.onNext(DeleteResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setRevision(revision)
                        .setRaftterm(driver.getTerm()))
                    .build());
            resp.onCompleted();
        } catch (StatusException e) {
            resp.onError(e);
        } catch (Exception e) {
            LOG.errorf(e, "Delete failed");
            resp.onError(io.grpc.Status.UNAVAILABLE.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void txn(TxnRequest req, StreamObserver<TxnResponse> resp) {
        try {
            requireLeader();
            byte[] result = driver.proposeTxn(req).get(10, TimeUnit.SECONDS);
            resp.onNext(TxnResponse.parseFrom(result));
            resp.onCompleted();
        } catch (StatusException e) {
            resp.onError(e);
        } catch (Exception e) {
            LOG.errorf(e, "Txn failed");
            resp.onError(io.grpc.Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void compact(CompactRequest req, StreamObserver<CompactResponse> resp) {
        try {
            requireLeader();
            byte[] result = driver.proposeCompact(req).get(10, TimeUnit.SECONDS);
            resp.onNext(CompactResponse.parseFrom(result));
            resp.onCompleted();
        } catch (StatusException e) {
            resp.onError(e);
        } catch (Exception e) {
            LOG.errorf(e, "Compact failed");
            resp.onError(io.grpc.Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public StreamObserver<WatchRequest> watch(StreamObserver<WatchResponse> downstream) {
        List<Long> activeWatches = new ArrayList<>();

        return new StreamObserver<>() {
            @Override
            public void onNext(WatchRequest req) {
                if (req.hasCreatereq()) {
                    WatchCreateRequest cr = req.getCreatereq();
                    long id = watchManager.register(
                            cr.getKey(),
                            cr.getRangeend(),
                            cr.getStartrev(),
                            downstream::onNext);
                    activeWatches.add(id);
                    downstream.onNext(WatchResponse.newBuilder()
                            .setWatchid(id).setCreated(true).build());
                } else if (req.hasCancelreq()) {
                    long id = req.getCancelreq().getWatchid();
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

    @Override
    public void leaseGrant(LeaseGrantRequest req, StreamObserver<LeaseGrantResponse> resp) {
        try {
            requireLeader();
            long leaseId = req.getId();
            if (leaseId == 0) {
                leaseId = leaseManager.generateId();
                req = req.toBuilder().setId(leaseId).build();
            }
            driver.propose(StateMachineDriver.OP_LEASE_GRANT, req)
                    .get(10, TimeUnit.SECONDS);
            resp.onNext(LeaseGrantResponse.newBuilder()
                    .setId(leaseId).setTtl(req.getTtl())
                    .setHeader(header()).build());
            resp.onCompleted();
        } catch (StatusException e) {
            resp.onError(e);
        } catch (Exception e) {
            resp.onNext(LeaseGrantResponse.newBuilder().setError(e.getMessage()).build());
            resp.onCompleted();
        }
    }

    @Override
    public void leaseRevoke(LeaseRevokeRequest req, StreamObserver<LeaseRevokeResponse> resp) {
        try {
            requireLeader();
            driver.propose(StateMachineDriver.OP_LEASE_REVOKE, req)
                    .get(10, TimeUnit.SECONDS);
            resp.onNext(LeaseRevokeResponse.newBuilder()
                    .setHeader(header()).build());
            resp.onCompleted();
        } catch (StatusException e) {
            resp.onError(e);
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
                    requireLeader();
                    driver.propose(StateMachineDriver.OP_LEASE_RENEW, req)
                            .get(10, TimeUnit.SECONDS);
                    long ttl = leaseManager.remaining(req.getId());
                    resp.onNext(LeaseKeepAliveResponse.newBuilder()
                            .setId(req.getId()).setTtl(ttl).build());
                } catch (StatusException e) {
                    resp.onError(e);
                } catch (Exception e) {
                    resp.onError(io.grpc.Status.NOT_FOUND
                            .withDescription(e.getMessage()).asRuntimeException());
                }
            }
            @Override public void onError(Throwable t) { LOG.debugf("KeepAlive error: %s", t.getMessage()); }
            @Override public void onCompleted()        { resp.onCompleted(); }
        };
    }

    @Override
    public void memberList(MemberListRequest req, StreamObserver<MemberListResponse> resp) {
        resp.onNext(MemberListResponse.getDefaultInstance());
        resp.onCompleted();
    }

    private void requireLeader() throws StatusException {
        if (!driver.isLeader()) {
            throw io.grpc.Status.UNAVAILABLE
                .withDescription("NOT_LEADER")
                .asException();
        }
    }

    private ResponseHeader header() {
        return ResponseHeader.newBuilder()
            .setRaftterm(driver.getTerm())
            .build();
    }

}
