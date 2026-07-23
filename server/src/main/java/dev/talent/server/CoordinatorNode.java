package dev.talent.server;

import dev.talent.raft.ClusterConfiguration;
import dev.talent.raft.RaftCore;
import dev.talent.proto.coordinator.Command;
import dev.talent.proto.coordinator.LeaseExpireCommand;
import dev.talent.runtime.FileRaftStorage;
import dev.talent.runtime.GrpcRaftTransport;
import dev.talent.runtime.RaftRuntime;
import dev.talent.runtime.RaftStorage;
import dev.talent.server.state.CoordinatorStateMachine;
import dev.talent.server.state.StateMachineResult;
import dev.talent.server.state.WatchHub;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.context.ManagedExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class CoordinatorNode {
    @Inject
    ManagedExecutor executor;

    @ConfigProperty(name = "raft.node-id")
    long nodeId;

    @ConfigProperty(name = "raft.members")
    String configuredMembers;

    @ConfigProperty(name = "raft.data-dir", defaultValue = "data")
    String dataDirectory;

    @ConfigProperty(name = "raft.election-timeout-ms", defaultValue = "3000")
    long electionTimeoutMillis;

    @ConfigProperty(name = "raft.heartbeat-interval-ms", defaultValue = "1000")
    long heartbeatIntervalMillis;

    private final AtomicLong requestIds = new AtomicLong();
    private final AtomicLong lastSnapshottedIndex = new AtomicLong();
    private final Map<Long, String> members = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile CoordinatorStateMachine stateMachine;
    private volatile GrpcRaftTransport transport;
    private volatile RaftRuntime runtime;
    private volatile boolean started;

    @PostConstruct
    void start() throws IOException {
        members.putAll(parseMembers(configuredMembers));
        if (!members.containsKey(nodeId)) {
            throw new IllegalArgumentException("raft.members does not contain local node " + nodeId);
        }

        stateMachine = new CoordinatorStateMachine(new WatchHub(executor, 256));
        FileRaftStorage storage = new FileRaftStorage(Path.of(dataDirectory));
        Optional<RaftStorage.Recovered> recovered = storage.recover();
        if (recovered.isPresent() && recovered.get().snapshot().isPresent()) {
            stateMachine.restore(recovered.get().snapshot().orElseThrow().stateMachine());
            lastSnapshottedIndex.set(recovered.get().snapshot().orElseThrow().lastIncludedIndex());
        }

        Set<Long> voters = Set.copyOf(members.keySet());
        RaftCore core = recovered
                .map(value -> new RaftCore(
                        nodeId,
                        electionTimeoutMillis,
                        heartbeatIntervalMillis,
                        value.state(),
                        value.snapshot().orElse(null)))
                .orElseGet(() -> new RaftCore(
                        nodeId,
                        ClusterConfiguration.stable(voters),
                        electionTimeoutMillis,
                        heartbeatIntervalMillis));

        Map<Long, String> remoteMembers = new LinkedHashMap<>(members);
        remoteMembers.remove(nodeId);
        transport = new GrpcRaftTransport(remoteMembers);
        runtime = new RaftRuntime(core, transport, storage, stateMachine, executor);
        runtime.start();
        started = true;
    }

    @Scheduled(
            every = "${raft.tick-interval:1s}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        RaftRuntime current = runtime;
        if (current != null) {
            current.tick(System.nanoTime() / 1_000_000);
        }
    }

    @Scheduled(
            every = "${lease.scan-interval:1s}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void expireLeases() {
        RaftRuntime current = runtime;
        CoordinatorStateMachine state = stateMachine;
        if (current == null || state == null || !current.status().role().equals(RaftCore.Role.LEADER)) {
            return;
        }
        var expired = state.expiredLeaseIds(System.currentTimeMillis());
        if (expired.isEmpty()) {
            return;
        }
        Command command = Command.newBuilder()
                .setRequestId(nextRequestId())
                .setLeaseExpire(LeaseExpireCommand.newBuilder().addAllIds(expired))
                .build();
        current.propose(command.toByteArray()).exceptionally(ignored -> null);
    }

    @Scheduled(
            every = "${raft.snapshot-interval:30s}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void createSnapshot() {
        RaftRuntime current = runtime;
        CoordinatorStateMachine state = stateMachine;
        if (current == null || state == null) {
            return;
        }
        long applied = state.appliedIndex();
        if (applied - lastSnapshottedIndex.get() < 1_000) {
            return;
        }
        current.snapshot(applied, state.appliedTerm(), state.snapshot());
        lastSnapshottedIndex.set(applied);
    }

    public CompletionStage<StateMachineResult> replicate(Command command) {
        long requestId = command.getRequestId() == 0 ? nextRequestId() : command.getRequestId();
        Command replicated = command.toBuilder().setRequestId(requestId).build();
        return runtime.propose(replicated.toByteArray()).thenApply(ignored -> {
            StateMachineResult result = stateMachine.result(requestId);
            if (result == null) {
                throw new IllegalStateException("committed command has no state-machine result");
            }
            return result;
        });
    }

    public CompletionStage<Long> readBarrier() {
        return runtime.readBarrier();
    }

    public CompletionStage<Long> changeMembership(Set<Long> voters, Set<Long> learners) {
        return runtime.changeMembership(voters, learners);
    }

    public void addPeer(long id, String target) {
        if (id == nodeId) {
            throw new IllegalArgumentException("cannot add local node as remote peer");
        }
        transport.addPeer(id, target);
        members.put(id, target);
    }

    public void removePeer(long id) {
        if (id != nodeId) {
            transport.removePeer(id);
        }
        members.remove(id);
    }

    public Map<Long, String> members() {
        return Map.copyOf(members);
    }

    public CoordinatorStateMachine stateMachine() {
        return stateMachine;
    }

    public RaftRuntime runtime() {
        return runtime;
    }

    public long nodeId() {
        return nodeId;
    }

    public boolean live() {
        return started && runtime != null && runtime.healthy();
    }

    public boolean ready() {
        return live() && stateMachine.appliedIndex() >= runtime.status().commitIndex();
    }

    private long nextRequestId() {
        long sequence = requestIds.incrementAndGet() & 0xffff_ffffL;
        return (nodeId << 32) | sequence;
    }

    @PreDestroy
    void stop() throws Exception {
        started = false;
        if (runtime != null) {
            runtime.close();
        }
        if (transport != null) {
            transport.close();
        }
    }

    static Map<Long, String> parseMembers(String text) {
        Map<Long, String> parsed = new LinkedHashMap<>();
        for (String item : text.split(",")) {
            String[] pair = item.trim().split("=", 2);
            if (pair.length != 2 || pair[1].isBlank()) {
                throw new IllegalArgumentException("invalid raft member: " + item);
            }
            long id = Long.parseLong(pair[0].trim());
            if (id <= 0 || parsed.put(id, pair[1].trim()) != null) {
                throw new IllegalArgumentException("invalid or duplicate member id " + id);
            }
        }
        return parsed;
    }
}
