package dev.talent.testkit;

import dev.talent.raft.RaftEffect;
import dev.talent.raft.RaftSnapshot;
import dev.talent.runtime.RaftStorage;
import java.util.Optional;

public final class InMemoryRaftStorage implements RaftStorage {
    private RaftEffect.PersistentState state;
    private RaftSnapshot snapshot;

    @Override
    public Optional<Recovered> recover() {
        return state == null
                ? Optional.empty()
                : Optional.of(new Recovered(state, Optional.ofNullable(snapshot)));
    }

    @Override
    public void persist(RaftEffect.PersistentState state) {
        this.state = state;
    }

    @Override
    public void persistSnapshot(RaftSnapshot snapshot) {
        this.snapshot = snapshot;
    }
}
