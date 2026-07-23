package dev.talent.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.talent.raft.ClusterConfiguration;
import dev.talent.raft.RaftCore;
import dev.talent.runtime.RaftRuntime;
import dev.talent.runtime.ReplicatedStateMachine;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class FaultInjectionTest {
    @Test
    void minorityLeaderCannotCommitAndCatchesFollowersUpAfterHeal() {
        Cluster cluster = cluster();
        FaultyNetwork network = cluster.network;
        Map<Long, RecordingMachine> machines = cluster.machines;
        Map<Long, RaftRuntime> nodes = cluster.nodes;

        network.partition(Set.of(1L), Set.of(2L, 3L));
        CompletableFuture<Long> proposal = nodes.get(1L)
                .propose("safe".getBytes(StandardCharsets.UTF_8))
                .toCompletableFuture();
        assertFalse(proposal.isDone());
        assertTrue(machines.values().stream().allMatch(machine -> machine.commands.isEmpty()));

        network.heal();
        nodes.get(1L).tick(1_300);

        assertTrue(proposal.isDone());
        assertEquals(
                List.of("safe"),
                machines.get(1L).commands.stream()
                        .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                        .toList());
        nodes.get(1L).tick(1_500);
        assertTrue(machines.values().stream().allMatch(machine ->
                machine.commands.stream().anyMatch(bytes ->
                        new String(bytes, StandardCharsets.UTF_8).equals("safe"))));
    }

    @Test
    void delayedReorderedAndDuplicatedAppendsApplyEachCommandOnce() {
        Cluster cluster = cluster();
        cluster.network.delay(1, 2);
        cluster.network.delay(1, 3);

        CompletableFuture<Long> first = cluster.nodes.get(1L)
                .propose("first".getBytes(StandardCharsets.UTF_8))
                .toCompletableFuture();
        CompletableFuture<Long> second = cluster.nodes.get(1L)
                .propose("second".getBytes(StandardCharsets.UTF_8))
                .toCompletableFuture();
        assertTrue(cluster.network.delayedCount() >= 4);

        cluster.network.duplicateNext(1, 2);
        cluster.network.stopDelaying(1, 2);
        cluster.network.stopDelaying(1, 3);
        cluster.network.deliverDelayed(true);
        cluster.nodes.get(1L).tick(1_300);

        assertTrue(first.isDone());
        assertTrue(second.isDone());
        cluster.nodes.get(1L).tick(1_500);
        for (RecordingMachine machine : cluster.machines.values()) {
            List<String> applied = machine.commands.stream()
                    .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                    .toList();
            assertEquals(List.of("first", "second"), applied);
        }
    }

    private static Cluster cluster() {
        FaultyNetwork network = new FaultyNetwork();
        Map<Long, RecordingMachine> machines = new LinkedHashMap<>();
        Map<Long, RaftRuntime> nodes = new LinkedHashMap<>();
        for (long id = 1; id <= 3; id++) {
            RecordingMachine machine = new RecordingMachine();
            RaftCore core =
                    new RaftCore(id, ClusterConfiguration.stable(Set.of(1L, 2L, 3L)), 1_000, 100);
            RaftRuntime runtime = new RaftRuntime(
                    core,
                    network.transportFor(id),
                    new InMemoryRaftStorage(),
                    machine,
                    Runnable::run);
            machines.put(id, machine);
            nodes.put(id, runtime);
            network.register(id, runtime);
            runtime.start();
        }
        nodes.get(1L).tick(1_100);
        assertEquals(RaftCore.Role.LEADER, nodes.get(1L).status().role());
        return new Cluster(network, machines, nodes);
    }

    private record Cluster(
            FaultyNetwork network,
            Map<Long, RecordingMachine> machines,
            Map<Long, RaftRuntime> nodes) {}

    private static final class RecordingMachine implements ReplicatedStateMachine {
        private final List<byte[]> commands = new ArrayList<>();

        @Override
        public void apply(long index, long term, byte[] command) {
            commands.add(command.clone());
        }

        @Override
        public void restore(byte[] snapshot) {
            commands.clear();
        }
    }
}
