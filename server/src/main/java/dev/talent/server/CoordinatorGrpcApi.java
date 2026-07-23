package dev.talent.server;

import com.google.protobuf.ByteString;
import dev.talent.raft.RaftCore;
import dev.talent.proto.coordinator.Command;
import dev.talent.proto.coordinator.CompactRequest;
import dev.talent.proto.coordinator.CompactResponse;
import dev.talent.proto.coordinator.CoordinatorGrpc;
import dev.talent.proto.coordinator.DeleteRequest;
import dev.talent.proto.coordinator.DeleteResponse;
import dev.talent.proto.coordinator.Event;
import dev.talent.proto.coordinator.EventType;
import dev.talent.proto.coordinator.KeyValue;
import dev.talent.proto.coordinator.LeaseGrantCommand;
import dev.talent.proto.coordinator.LeaseGrantRequest;
import dev.talent.proto.coordinator.LeaseGrantResponse;
import dev.talent.proto.coordinator.LeaseKeepAliveCommand;
import dev.talent.proto.coordinator.LeaseKeepAliveRequest;
import dev.talent.proto.coordinator.LeaseKeepAliveResponse;
import dev.talent.proto.coordinator.LeaseRevokeRequest;
import dev.talent.proto.coordinator.LeaseRevokeResponse;
import dev.talent.proto.coordinator.Member;
import dev.talent.proto.coordinator.MemberAddRequest;
import dev.talent.proto.coordinator.MemberChangeResponse;
import dev.talent.proto.coordinator.MemberListRequest;
import dev.talent.proto.coordinator.MemberListResponse;
import dev.talent.proto.coordinator.MemberRemoveRequest;
import dev.talent.proto.coordinator.OperationResponse;
import dev.talent.proto.coordinator.PutRequest;
import dev.talent.proto.coordinator.PutResponse;
import dev.talent.proto.coordinator.RangeRequest;
import dev.talent.proto.coordinator.RangeResponse;
import dev.talent.proto.coordinator.ResponseHeader;
import dev.talent.proto.coordinator.TxnRequest;
import dev.talent.proto.coordinator.TxnResponse;
import dev.talent.proto.coordinator.WatchRequest;
import dev.talent.proto.coordinator.WatchResponse;
import dev.talent.runtime.RaftException;
import dev.talent.server.state.CompactedException;
import dev.talent.server.state.KvRecord;
import dev.talent.server.state.MvccStore;
import dev.talent.server.state.StateMachineResult;
import dev.talent.server.state.WatchHub;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@GrpcService
@Singleton
public class CoordinatorGrpcApi extends CoordinatorGrpc.CoordinatorImplBase {
    @Inject
    CoordinatorNode node;

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> observer) {
        Command command = Command.newBuilder().setPut(request).build();
        respond(node.replicate(command), result -> {
            StateMachineResult.Put put = require(result, StateMachineResult.Put.class);
            PutResponse.Builder response = PutResponse.newBuilder().setHeader(header(put.revision()));
            if (request.getPrevkv() && put.previous() != null) {
                response.setPrevkv(toProto(put.previous(), false));
            }
            return response.build();
        }, observer);
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> observer) {
        respond(node.readBarrier().thenApply(ignored -> node.stateMachine().range(request)),
                this::rangeResponse,
                observer);
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> observer) {
        respond(node.replicate(Command.newBuilder().setDelete(request).build()), result -> {
            StateMachineResult.Delete deleted = require(result, StateMachineResult.Delete.class);
            DeleteResponse.Builder response = DeleteResponse.newBuilder()
                    .setHeader(header(deleted.revision()))
                    .setDeleted(deleted.previous().size());
            if (request.getPrevkv()) {
                deleted.previous().forEach(value -> response.addPrevkvs(toProto(value, false)));
            }
            return response.build();
        }, observer);
    }

    @Override
    public void txn(TxnRequest request, StreamObserver<TxnResponse> observer) {
        respond(node.replicate(Command.newBuilder().setTxn(request).build()), result -> {
            StateMachineResult.Txn txn = require(result, StateMachineResult.Txn.class);
            TxnResponse.Builder response = TxnResponse.newBuilder()
                    .setHeader(header(txn.revision()))
                    .setSucceeded(txn.succeeded());
            txn.operations().stream().map(this::operationResponse).forEach(response::addResponses);
            return response.build();
        }, observer);
    }

    @Override
    public void compact(CompactRequest request, StreamObserver<CompactResponse> observer) {
        respond(node.replicate(Command.newBuilder().setCompact(request).build()), result -> {
            StateMachineResult.Compact compact =
                    require(result, StateMachineResult.Compact.class);
            return CompactResponse.newBuilder()
                    .setHeader(header(compact.revision()))
                    .setRevision(compact.compactedRevision())
                    .setRemoved(compact.removed())
                    .build();
        }, observer);
    }

    @Override
    public StreamObserver<WatchRequest> watch(StreamObserver<WatchResponse> observer) {
        AtomicReference<WatchHub.Handle> handle = new AtomicReference<>();
        return new StreamObserver<>() {
            @Override
            public void onNext(WatchRequest request) {
                switch (request.getRequnionCase()) {
                    case CREATEREQ -> {
                        if (handle.get() != null) {
                            observer.onError(Status.FAILED_PRECONDITION
                                    .withDescription("watch already created")
                                    .asRuntimeException());
                            return;
                        }
                        var create = request.getCreatereq();
                        try {
                            WatchHub.Handle created = node.stateMachine().watches().subscribe(
                                    dev.talent.server.state.ByteKey.of(
                                            create.getKey().toByteArray()),
                                    create.getRangeend().isEmpty()
                                            ? null
                                            : dev.talent.server.state.ByteKey.of(
                                                    create.getRangeend().toByteArray()),
                                    create.getStartrev(),
                                    create.getPrevkv(),
                                    events -> {
                                        WatchHub.Handle current = handle.get();
                                        if (current != null) {
                                            emitWatch(observer, events, current.id());
                                        }
                                    },
                                    problem -> observer.onError(toStatus(problem)));
                            handle.set(created);
                            synchronized (observer) {
                                observer.onNext(WatchResponse.newBuilder()
                                        .setHeader(header(node.stateMachine().revision()))
                                        .setWatchid(created.id())
                                        .setCreated(true)
                                        .build());
                            }
                        } catch (Throwable problem) {
                            observer.onError(toStatus(problem));
                        }
                    }
                    case CANCELREQ -> {
                        WatchHub.Handle current = handle.getAndSet(null);
                        if (current != null && current.id() == request.getCancelreq().getWatchid()) {
                            current.close();
                            synchronized (observer) {
                                observer.onNext(WatchResponse.newBuilder()
                                        .setWatchid(current.id())
                                        .setCanceled(true)
                                        .build());
                                observer.onCompleted();
                            }
                        }
                    }
                    case REQUNION_NOT_SET -> observer.onError(Status.INVALID_ARGUMENT
                            .withDescription("watch request is empty")
                            .asRuntimeException());
                }
            }

            @Override
            public void onError(Throwable problem) {
                close(handle);
            }

            @Override
            public void onCompleted() {
                close(handle);
                observer.onCompleted();
            }
        };
    }

    @Override
    public void leaseGrant(
            LeaseGrantRequest request, StreamObserver<LeaseGrantResponse> observer) {
        long id = request.getId() == 0
                ? ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)
                : request.getId();
        long deadline;
        try {
            deadline = Math.addExact(
                    System.currentTimeMillis(), Math.multiplyExact(request.getTtl(), 1_000));
        } catch (ArithmeticException overflow) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("lease TTL is too large")
                    .asRuntimeException());
            return;
        }
        LeaseGrantCommand grant = LeaseGrantCommand.newBuilder()
                .setId(id)
                .setTtlSecs(request.getTtl())
                .setDeadlineEpochMillis(deadline)
                .build();
        respond(node.replicate(Command.newBuilder().setLeaseGrant(grant).build()), result -> {
            StateMachineResult.Lease lease = require(result, StateMachineResult.Lease.class);
            return LeaseGrantResponse.newBuilder()
                    .setHeader(header(lease.revision()))
                    .setId(lease.id())
                    .setTtl(lease.ttlSeconds())
                    .build();
        }, observer);
    }

    @Override
    public void leaseRevoke(
            LeaseRevokeRequest request, StreamObserver<LeaseRevokeResponse> observer) {
        respond(node.replicate(Command.newBuilder().setLeaseRevoke(request).build()), result ->
                LeaseRevokeResponse.newBuilder()
                        .setHeader(header(result.revision()))
                        .build(), observer);
    }

    @Override
    public StreamObserver<LeaseKeepAliveRequest> leaseKeepAlive(
            StreamObserver<LeaseKeepAliveResponse> observer) {
        return new StreamObserver<>() {
            @Override
            public void onNext(LeaseKeepAliveRequest request) {
                try {
                    long ttl = node.stateMachine().lease(request.getId()).ttlSeconds();
                    long deadline = Math.addExact(
                            System.currentTimeMillis(), Math.multiplyExact(ttl, 1_000));
                    LeaseKeepAliveCommand keepAlive = LeaseKeepAliveCommand.newBuilder()
                            .setId(request.getId())
                            .setDeadlineEpochMillis(deadline)
                            .build();
                    respond(node.replicate(
                                    Command.newBuilder().setLeaseKeepAlive(keepAlive).build()),
                            result -> LeaseKeepAliveResponse.newBuilder()
                                    .setHeader(header(result.revision()))
                                    .setId(request.getId())
                                    .setTtl(ttl)
                                    .build(),
                            observer);
                } catch (Throwable problem) {
                    observer.onError(toStatus(problem));
                }
            }

            @Override public void onError(Throwable ignored) {}
            @Override public void onCompleted() { observer.onCompleted(); }
        };
    }

    @Override
    public void memberList(
            MemberListRequest request, StreamObserver<MemberListResponse> observer) {
        respond(node.readBarrier(), ignored -> {
            MemberListResponse.Builder response =
                    MemberListResponse.newBuilder().setHeader(header(node.stateMachine().revision()));
            node.members().entrySet().stream()
                    .sorted(java.util.Map.Entry.comparingByKey())
                    .map(entry -> member(entry.getKey(), entry.getValue()))
                    .forEach(response::addMembers);
            return response.build();
        }, observer);
    }

    @Override
    public void memberAdd(
            MemberAddRequest request, StreamObserver<MemberChangeResponse> observer) {
        if (!request.hasMember() || request.getMember().getPeerurlsCount() != 1) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("member must contain exactly one peer URL")
                    .asRuntimeException());
            return;
        }
        long id = request.getMember().getId();
        String target = request.getMember().getPeerurls(0);
        node.addPeer(id, target);
        Set<Long> voters = new HashSet<>(node.runtime().status().configuration().voters());
        voters.add(id);
        respond(node.changeMembership(voters, Set.of()), ignored -> memberChange(), observer);
    }

    @Override
    public void memberRemove(
            MemberRemoveRequest request, StreamObserver<MemberChangeResponse> observer) {
        Set<Long> voters = new HashSet<>(node.runtime().status().configuration().voters());
        if (!voters.remove(request.getId()) || voters.isEmpty()) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("unknown member or removal would empty the cluster")
                    .asRuntimeException());
            return;
        }
        respond(node.changeMembership(voters, Set.of()), ignored -> {
            node.removePeer(request.getId());
            return memberChange();
        }, observer);
    }

    private MemberChangeResponse memberChange() {
        MemberChangeResponse.Builder response =
                MemberChangeResponse.newBuilder().setHeader(header(node.stateMachine().revision()));
        node.members().entrySet().stream()
                .sorted(java.util.Map.Entry.comparingByKey())
                .map(entry -> member(entry.getKey(), entry.getValue()))
                .forEach(response::addMembers);
        return response.build();
    }

    private OperationResponse operationResponse(StateMachineResult result) {
        OperationResponse.Builder response =
                OperationResponse.newBuilder().setRevision(result.revision());
        switch (result) {
            case StateMachineResult.Put put -> {
                PutResponse.Builder value = PutResponse.newBuilder().setHeader(header(put.revision()));
                if (put.previous() != null) {
                    value.setPrevkv(toProto(put.previous(), false));
                }
                response.setPut(value);
            }
            case StateMachineResult.Delete delete -> {
                DeleteResponse.Builder value = DeleteResponse.newBuilder()
                        .setHeader(header(delete.revision()))
                        .setDeleted(delete.previous().size());
                delete.previous().forEach(previous ->
                        value.addPrevkvs(toProto(previous, false)));
                response.setDelete(value);
            }
            case StateMachineResult.Range range -> response.setRange(rangeResponse(range.range()));
            default -> throw new IllegalStateException(
                    "unsupported transaction result " + result.getClass().getSimpleName());
        }
        return response.build();
    }

    private RangeResponse rangeResponse(MvccStore.Range range) {
        return RangeResponse.newBuilder()
                .setHeader(header(node.stateMachine().revision()))
                .addAllKvs(range.values().stream().map(value -> toProto(value, false)).toList())
                .setCount(range.count())
                .setMore(range.more())
                .build();
    }

    private void emitWatch(
            StreamObserver<WatchResponse> observer, List<WatchHub.Event> events, long watchId) {
        WatchResponse.Builder response = WatchResponse.newBuilder()
                .setWatchid(watchId)
                .setHeader(header(node.stateMachine().revision()));
        events.stream().map(this::toProto).forEach(response::addEvents);
        synchronized (observer) {
            observer.onNext(response.build());
        }
    }

    private Event toProto(WatchHub.Event event) {
        Event.Builder value = Event.newBuilder()
                .setType(event.type() == WatchHub.Type.PUT ? EventType.PUT : EventType.DELETE)
                .setKv(toProto(event.current(), false));
        if (event.previous() != null) {
            value.setPrevkv(toProto(event.previous(), false));
        }
        return value.build();
    }

    private ResponseHeader header(long revision) {
        RaftCore.Status status = node.runtime().status();
        return ResponseHeader.newBuilder()
                .setMemberid(node.nodeId())
                .setRevision(revision)
                .setRaftterm(status.term())
                .build();
    }

    private static KeyValue toProto(KvRecord value, boolean keysOnly) {
        return KeyValue.newBuilder()
                .setKey(ByteString.copyFrom(value.key().bytes()))
                .setValue(keysOnly ? ByteString.EMPTY : ByteString.copyFrom(value.value()))
                .setCreaterev(value.createRevision())
                .setModrev(value.modificationRevision())
                .setVersion(value.version())
                .setLease(value.leaseId())
                .build();
    }

    private static Member member(long id, String target) {
        return Member.newBuilder().setId(id).setName("node-" + id).addPeerurls(target).build();
    }

    private static <T> T require(StateMachineResult result, Class<T> type) {
        if (result instanceof StateMachineResult.Failure failure) {
            throw new IllegalArgumentException(failure.message());
        }
        if (!type.isInstance(result)) {
            throw new IllegalStateException(
                    "expected " + type.getSimpleName() + ", got " + result.getClass().getSimpleName());
        }
        return type.cast(result);
    }

    private static <T, R> void respond(
            CompletionStage<T> stage, Function<T, R> mapper, StreamObserver<R> observer) {
        stage.whenComplete((value, problem) -> {
            if (problem != null) {
                observer.onError(toStatus(problem));
                return;
            }
            try {
                observer.onNext(mapper.apply(value));
                observer.onCompleted();
            } catch (Throwable mappingProblem) {
                observer.onError(toStatus(mappingProblem));
            }
        });
    }

    private static RuntimeException toStatus(Throwable original) {
        Throwable problem = original;
        while ((problem instanceof CompletionException
                        || problem instanceof java.util.concurrent.ExecutionException)
                && problem.getCause() != null) {
            problem = problem.getCause();
        }
        if (problem instanceof RaftException raft) {
            return switch (raft.error()) {
                case NOT_LEADER -> Status.FAILED_PRECONDITION
                        .withDescription(raft.getMessage())
                        .asRuntimeException();
                case MEMBERSHIP_CHANGE_IN_PROGRESS, MEMBER_NOT_CAUGHT_UP ->
                        Status.ABORTED.withDescription(raft.getMessage()).asRuntimeException();
                case INVALID_CONFIGURATION ->
                        Status.INVALID_ARGUMENT.withDescription(raft.getMessage()).asRuntimeException();
            };
        }
        if (problem instanceof CompactedException compacted) {
            return Status.OUT_OF_RANGE
                    .withDescription(compacted.getMessage())
                    .asRuntimeException();
        }
        if (problem instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT
                    .withDescription(problem.getMessage())
                    .asRuntimeException();
        }
        return Status.UNAVAILABLE
                .withDescription(problem.getMessage() == null
                        ? problem.getClass().getSimpleName()
                        : problem.getMessage())
                .withCause(problem)
                .asRuntimeException();
    }

    private static void close(AtomicReference<WatchHub.Handle> handle) {
        WatchHub.Handle current = handle.getAndSet(null);
        if (current != null) {
            current.close();
        }
    }
}
