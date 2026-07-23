package dev.talent.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RaftCoreTest {
    @Test
    void singleNodeElectsAndCommitsWithoutPeerResponse() {
        RaftCore node = node(1, Set.of(1L));

        node.handle(new RaftEvent.Tick(2_000));
        assertEquals(RaftCore.Role.LEADER, node.status().role());
        assertEquals(1, node.status().commitIndex(), "leader no-op must commit locally");

        List<RaftEffect> effects =
                node.handle(new RaftEvent.Propose(7, "value".getBytes(StandardCharsets.UTF_8)));

        RaftEffect.Apply apply = effects.stream()
                .filter(RaftEffect.Apply.class::isInstance)
                .map(RaftEffect.Apply.class::cast)
                .findFirst()
                .orElseThrow();
        assertEquals(2, apply.index());
        assertEquals("value", new String(apply.command(), StandardCharsets.UTF_8));
        assertTrue(effects.stream().anyMatch(effect ->
                effect.equals(new RaftEffect.ProposalCommitted(7, 2))));
    }

    @Test
    void readIndexWaitsForCurrentLeaderQuorum() {
        RaftCore node = electedThreeNodeLeader();

        List<RaftEffect> started = node.handle(new RaftEvent.Read(42));
        RaftEffect.AppendRequest heartbeat = started.stream()
                .filter(RaftEffect.Send.class::isInstance)
                .map(RaftEffect.Send.class::cast)
                .map(RaftEffect.Send::rpc)
                .filter(RaftEffect.AppendRequest.class::isInstance)
                .map(RaftEffect.AppendRequest.class::cast)
                .findFirst()
                .orElseThrow();
        assertTrue(heartbeat.readContext() > 0);
        assertTrue(started.stream().noneMatch(RaftEffect.ReadReady.class::isInstance));

        List<RaftEffect> completed = node.handle(new RaftEvent.AppendEntriesResult(
                2, node.status().term(), true, node.status().lastLogIndex(), 0, 0,
                heartbeat.readContext()));
        assertTrue(completed.stream().anyMatch(effect ->
                effect.equals(new RaftEffect.ReadReady(42, node.status().commitIndex()))));
    }

    @Test
    void jointConfigurationRequiresBothMajorities() {
        ClusterConfiguration joint =
                ClusterConfiguration.joint(Set.of(1L, 2L, 3L), Set.of(3L, 4L, 5L), Set.of());

        assertTrue(joint.hasElectionQuorum(Set.of(1L, 2L, 3L, 4L)));
        assertTrue(!joint.hasElectionQuorum(Set.of(1L, 2L, 4L)));
        assertTrue(!joint.hasElectionQuorum(Set.of(3L, 4L)));
    }

    @Test
    void addingVoterStagesLearnerThenUsesJointConsensus() {
        RaftCore node = node(1, Set.of(1L));
        node.handle(new RaftEvent.Tick(2_000));

        node.handle(new RaftEvent.ChangeMembership(99, Set.of(1L, 2L), Set.of()));
        assertEquals(Set.of(2L), node.status().configuration().learners());

        node.handle(new RaftEvent.AppendEntriesResult(
                2, node.status().term(), true, 2, 0, 0, 0));
        assertEquals(4, node.status().lastLogIndex(),
                "old single-voter quorum commits joint and appends stable immediately");
        assertTrue(node.status().configuration().joint());

        List<RaftEffect> stable = node.handle(new RaftEvent.AppendEntriesResult(
                2, node.status().term(), true, 4, 0, 0, 0));
        assertEquals(Set.of(1L, 2L), node.status().configuration().voters());
        assertTrue(!node.status().configuration().joint());
        assertTrue(stable.stream().anyMatch(effect ->
                effect.equals(new RaftEffect.ProposalCommitted(99, 4))));
    }

    @Test
    void followerRejectsStaleAppendWithTypedResponse() {
        RaftCore node = node(1, Set.of(1L, 2L, 3L));
        node.handle(new RaftEvent.Tick(2_000));
        long currentTerm = node.status().term();

        List<RaftEffect> effects = node.handle(new RaftEvent.AppendEntries(
                2, currentTerm - 1, 2, 0, 0, List.of(), 0, 0));

        RaftEffect.Send response = assertInstanceOf(RaftEffect.Send.class, effects.getFirst());
        RaftEffect.AppendResponse append =
                assertInstanceOf(RaftEffect.AppendResponse.class, response.rpc());
        assertTrue(!append.success());
        assertEquals(currentTerm, append.term());
    }

    private static RaftCore electedThreeNodeLeader() {
        RaftCore node = node(1, Set.of(1L, 2L, 3L));
        node.handle(new RaftEvent.Tick(2_000));
        node.handle(new RaftEvent.RequestVoteResult(2, 1, true));
        assertEquals(RaftCore.Role.LEADER, node.status().role());
        return node;
    }

    private static RaftCore node(long id, Set<Long> voters) {
        return new RaftCore(id, ClusterConfiguration.stable(voters), 1_000, 100);
    }
}
