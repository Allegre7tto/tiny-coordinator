package dev.talent.raft;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public record ClusterConfiguration(
        Set<Long> oldVoters,
        Set<Long> newVoters,
        Set<Long> learners) {

    public ClusterConfiguration {
        oldVoters = immutableNonEmpty(oldVoters, "oldVoters");
        newVoters = immutableNonEmpty(newVoters, "newVoters");
        learners = Set.copyOf(Objects.requireNonNull(learners, "learners"));
        LinkedHashSet<Long> voterUnion = new LinkedHashSet<>(oldVoters);
        voterUnion.addAll(newVoters);
        if (!disjoint(voterUnion, learners)) {
            throw new IllegalArgumentException("a node cannot be both voter and learner");
        }
    }

    public static ClusterConfiguration stable(Set<Long> voters) {
        Set<Long> copy = immutableNonEmpty(voters, "voters");
        return new ClusterConfiguration(copy, copy, Set.of());
    }

    public static ClusterConfiguration stable(Set<Long> voters, Set<Long> learners) {
        Set<Long> copy = immutableNonEmpty(voters, "voters");
        return new ClusterConfiguration(copy, copy, learners);
    }

    public static ClusterConfiguration joint(
            Set<Long> oldVoters, Set<Long> newVoters, Set<Long> learners) {
        return new ClusterConfiguration(oldVoters, newVoters, learners);
    }

    public boolean joint() {
        return !oldVoters.equals(newVoters);
    }

    public Set<Long> voters() {
        LinkedHashSet<Long> union = new LinkedHashSet<>(oldVoters);
        union.addAll(newVoters);
        return Set.copyOf(union);
    }

    public Set<Long> replicationTargets() {
        LinkedHashSet<Long> all = new LinkedHashSet<>(voters());
        all.addAll(learners);
        return Set.copyOf(all);
    }

    public boolean hasElectionQuorum(Set<Long> acknowledgements) {
        return majority(oldVoters, acknowledgements)
                && (!joint() || majority(newVoters, acknowledgements));
    }

    public boolean hasCommitQuorum(long index, java.util.Map<Long, Long> matchIndexes) {
        return majorityAt(oldVoters, index, matchIndexes)
                && (!joint() || majorityAt(newVoters, index, matchIndexes));
    }

    private static boolean majority(Set<Long> voters, Set<Long> acknowledgements) {
        long votes = voters.stream().filter(acknowledgements::contains).count();
        return votes >= voters.size() / 2 + 1;
    }

    private static boolean majorityAt(
            Set<Long> voters, long index, java.util.Map<Long, Long> matchIndexes) {
        long votes = voters.stream().filter(id -> matchIndexes.getOrDefault(id, 0L) >= index).count();
        return votes >= voters.size() / 2 + 1;
    }

    private static Set<Long> immutableNonEmpty(Set<Long> values, String name) {
        Objects.requireNonNull(values, name);
        if (values.isEmpty()) {
            throw new IllegalArgumentException(name + " cannot be empty");
        }
        return Set.copyOf(values);
    }

    private static boolean disjoint(Set<Long> left, Set<Long> right) {
        return left.stream().noneMatch(right::contains);
    }
}
