package dev.talent.runtime;

import dev.talent.raft.RaftEffect;
import dev.talent.raft.RaftSnapshot;
import java.io.IOException;
import java.util.Optional;

public interface RaftStorage extends AutoCloseable {
    record Recovered(RaftEffect.PersistentState state, Optional<RaftSnapshot> snapshot) {
        public Recovered {
            snapshot = snapshot == null ? Optional.empty() : snapshot;
        }
    }

    Optional<Recovered> recover() throws IOException;

    void persist(RaftEffect.PersistentState state) throws IOException;

    void persistSnapshot(RaftSnapshot snapshot) throws IOException;

    @Override
    default void close() throws IOException {}
}
