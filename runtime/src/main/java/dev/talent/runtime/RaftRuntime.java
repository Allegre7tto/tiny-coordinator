package dev.talent.runtime;

import dev.talent.raft.RaftCore;
import dev.talent.raft.RaftEffect;
import dev.talent.raft.RaftEvent;
import dev.talent.raft.RaftSnapshot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Serializes every interaction with {@link RaftCore} without owning a thread.
 * The supplied executor may be managed by Quarkus; the drain guard guarantees
 * that at most one task mutates the core at a time.
 */
public final class RaftRuntime implements AutoCloseable {
    private record Envelope(
            RaftEvent event, long sender, CompletableFuture<RaftEffect.Rpc> inboundReply) {}

    private final RaftCore core;
    private final RaftTransport transport;
    private final RaftStorage storage;
    private final ReplicatedStateMachine stateMachine;
    private final Executor executor;
    private final ConcurrentLinkedQueue<Envelope> mailbox = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong correlations = new AtomicLong();
    private final ConcurrentHashMap<Long, CompletableFuture<Long>> pending =
            new ConcurrentHashMap<>();
    private volatile RaftCore.Status publishedStatus;
    private volatile Throwable failure;

    public RaftRuntime(
            RaftCore core,
            RaftTransport transport,
            RaftStorage storage,
            ReplicatedStateMachine stateMachine,
            Executor executor) {
        this.core = Objects.requireNonNull(core, "core");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.storage = Objects.requireNonNull(storage, "storage");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.publishedStatus = core.status();
    }

    public void start() {
        submitBootstrap();
    }

    public CompletionStage<Long> propose(byte[] command) {
        long correlation = correlations.incrementAndGet();
        CompletableFuture<Long> result = register(correlation);
        enqueue(new Envelope(new RaftEvent.Propose(correlation, command), 0, null));
        return result;
    }

    public CompletionStage<Long> readBarrier() {
        long correlation = correlations.incrementAndGet();
        CompletableFuture<Long> result = register(correlation);
        enqueue(new Envelope(new RaftEvent.Read(correlation), 0, null));
        return result;
    }

    public CompletionStage<Long> changeMembership(Set<Long> voters, Set<Long> learners) {
        long correlation = correlations.incrementAndGet();
        CompletableFuture<Long> result = register(correlation);
        enqueue(new Envelope(
                new RaftEvent.ChangeMembership(correlation, voters, learners), 0, null));
        return result;
    }

    public void tick(long monotonicMillis) {
        enqueue(new Envelope(new RaftEvent.Tick(monotonicMillis), 0, null));
    }

    public CompletionStage<RaftEffect.Rpc> receive(long sender, RaftEvent event) {
        CompletableFuture<RaftEffect.Rpc> reply = new CompletableFuture<>();
        enqueue(new Envelope(event, sender, reply));
        return reply;
    }

    public void snapshot(long index, long term, byte[] bytes) {
        RaftSnapshot snapshot =
                new RaftSnapshot(index, term, publishedStatus.configuration(), bytes);
        enqueue(new Envelope(new RaftEvent.SnapshotCreated(snapshot), 0, null));
    }

    public RaftCore.Status status() {
        return publishedStatus;
    }

    public boolean healthy() {
        return running.get() && failure == null;
    }

    public Throwable failure() {
        return failure;
    }

    private CompletableFuture<Long> register(long correlation) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("runtime is stopped"));
        }
        CompletableFuture<Long> result = new CompletableFuture<>();
        pending.put(correlation, result);
        return result;
    }

    private void submitBootstrap() {
        if (draining.compareAndSet(false, true)) {
            executor.execute(() -> {
                try {
                    List<RaftEffect> effects = core.bootstrap();
                    publishedStatus = core.status();
                    processEffects(effects, 0, null);
                } catch (Throwable problem) {
                    fail(problem);
                } finally {
                    draining.set(false);
                    scheduleDrain();
                }
            });
        }
    }

    private void enqueue(Envelope envelope) {
        if (!running.get()) {
            if (envelope.inboundReply() != null) {
                envelope.inboundReply().completeExceptionally(
                        new IllegalStateException("runtime is stopped"));
            }
            return;
        }
        mailbox.add(envelope);
        scheduleDrain();
    }

    private void scheduleDrain() {
        if (!mailbox.isEmpty() && draining.compareAndSet(false, true)) {
            executor.execute(this::drain);
        }
    }

    private void drain() {
        try {
            Envelope envelope;
            while (running.get() && (envelope = mailbox.poll()) != null) {
                List<RaftEffect> effects = core.handle(envelope.event());
                publishedStatus = core.status();
                processEffects(effects, envelope.sender(), envelope.inboundReply());
            }
        } catch (Throwable problem) {
            fail(problem);
        } finally {
            draining.set(false);
            scheduleDrain();
        }
    }

    private void processEffects(
            List<RaftEffect> effects,
            long inboundSender,
            CompletableFuture<RaftEffect.Rpc> inboundReply) throws IOException {
        for (RaftEffect effect : effects) {
            switch (effect) {
                case RaftEffect.Persist persist -> storage.persist(persist.state());
                case RaftEffect.SaveSnapshot save -> storage.persistSnapshot(save.snapshot());
                case RaftEffect.Apply apply ->
                        stateMachine.apply(apply.index(), apply.term(), apply.command());
                case RaftEffect.AdvanceApplied advance ->
                        stateMachine.advance(advance.index(), advance.term());
                case RaftEffect.RestoreSnapshot restore -> {
                    boolean success = true;
                    try {
                        storage.persistSnapshot(restore.snapshot());
                        stateMachine.restore(restore.snapshot().stateMachine());
                    } catch (RuntimeException | IOException problem) {
                        success = false;
                    }
                    enqueue(new Envelope(new RaftEvent.SnapshotInstalled(
                            restore.leaderId(),
                            restore.term(),
                            restore.snapshot(),
                            success), 0, null));
                }
                case RaftEffect.Send send -> {
                    if (inboundReply != null
                            && send.target() == inboundSender
                            && isResponse(send.rpc())) {
                        inboundReply.complete(send.rpc());
                    } else {
                        sendRequest(send.target(), send.rpc());
                    }
                }
                case RaftEffect.ProposalCommitted committed -> complete(
                        committed.correlationId(), committed.index());
                case RaftEffect.ReadReady ready ->
                        complete(ready.correlationId(), ready.readIndex());
                case RaftEffect.Rejected rejected -> reject(rejected);
                case RaftEffect.RoleChanged ignored -> { }
            }
        }
    }

    private void sendRequest(long target, RaftEffect.Rpc request) {
        if (isResponse(request)) {
            return;
        }
        transport.request(target, request).whenComplete((response, problem) -> {
            if (problem != null || response == null || !running.get()) {
                return;
            }
            RaftEvent event = responseEvent(target, response);
            if (event != null) {
                enqueue(new Envelope(event, target, null));
            }
        });
    }

    private RaftEvent responseEvent(long peer, RaftEffect.Rpc response) {
        return switch (response) {
            case RaftEffect.VoteResponse vote ->
                    new RaftEvent.RequestVoteResult(peer, vote.term(), vote.granted());
            case RaftEffect.AppendResponse append -> new RaftEvent.AppendEntriesResult(
                    peer,
                    append.term(),
                    append.success(),
                    append.matchIndex(),
                    append.conflictTerm(),
                    append.conflictIndex(),
                    append.readContext());
            case RaftEffect.SnapshotResponse snapshot -> new RaftEvent.InstallSnapshotResult(
                    peer, snapshot.term(), snapshot.installed(), snapshot.lastIncludedIndex());
            default -> null;
        };
    }

    private boolean isResponse(RaftEffect.Rpc rpc) {
        return rpc instanceof RaftEffect.VoteResponse
                || rpc instanceof RaftEffect.AppendResponse
                || rpc instanceof RaftEffect.SnapshotResponse;
    }

    private void complete(long correlation, long index) {
        CompletableFuture<Long> future = pending.remove(correlation);
        if (future != null) {
            future.complete(index);
        }
    }

    private void reject(RaftEffect.Rejected rejected) {
        CompletableFuture<Long> future = pending.remove(rejected.correlationId());
        if (future != null) {
            future.completeExceptionally(
                    new RaftException(rejected.error(), rejected.leaderHint()));
        }
    }

    private void fail(Throwable problem) {
        failure = problem;
        running.set(false);
        UncheckedIOException storageFailure = problem instanceof IOException io
                ? new UncheckedIOException(io)
                : null;
        Throwable exposed = storageFailure == null ? problem : storageFailure;
        pending.forEach((ignored, future) -> future.completeExceptionally(exposed));
        pending.clear();
        Envelope envelope;
        while ((envelope = mailbox.poll()) != null) {
            if (envelope.inboundReply() != null) {
                envelope.inboundReply().completeExceptionally(exposed);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (running.compareAndSet(true, false)) {
            IllegalStateException stopped = new IllegalStateException("runtime stopped");
            pending.forEach((ignored, future) -> future.completeExceptionally(stopped));
            pending.clear();
            storage.close();
        }
    }
}
