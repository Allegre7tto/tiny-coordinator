package dev.talent.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.talent.raft.ClusterConfiguration;
import dev.talent.raft.RaftEffect;
import dev.talent.raft.RaftLogEntry;
import dev.talent.raft.RaftSnapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileRaftStorageTest {
    @TempDir
    Path directory;

    @Test
    void recoversLastFlushedCheckpointAndSnapshot() throws Exception {
        ClusterConfiguration configuration = ClusterConfiguration.stable(Set.of(1L, 2L, 3L));
        RaftSnapshot snapshot = new RaftSnapshot(1, 1, configuration, new byte[] {4, 5});
        RaftEffect.PersistentState state = new RaftEffect.PersistentState(
                2,
                1L,
                2,
                1,
                1,
                configuration,
                List.of(
                        RaftLogEntry.noop(1, 1),
                        RaftLogEntry.command(2, 2, new byte[] {9})));

        try (FileRaftStorage storage = new FileRaftStorage(directory)) {
            storage.persistSnapshot(snapshot);
            storage.persist(state);
        }

        try (FileRaftStorage reopened = new FileRaftStorage(directory)) {
            RaftStorage.Recovered recovered = reopened.recover().orElseThrow();
            assertEquals(2, recovered.state().term());
            assertEquals(2, recovered.state().commitIndex());
            assertEquals(2, recovered.state().log().size());
            assertArrayEquals(
                    new byte[] {4, 5},
                    recovered.snapshot().orElseThrow().stateMachine());
        }
    }

    @Test
    void checksumFailureIsNotSilentlyIgnored() throws Exception {
        ClusterConfiguration configuration = ClusterConfiguration.stable(Set.of(1L));
        try (FileRaftStorage storage = new FileRaftStorage(directory)) {
            storage.persist(new RaftEffect.PersistentState(
                    1, 1L, 0, 0, 0, configuration, List.of(RaftLogEntry.noop(0, 0))));
        }
        Files.write(
                directory.resolve("raft.wal"),
                new byte[] {99},
                StandardOpenOption.APPEND);

        try (FileRaftStorage reopened = new FileRaftStorage(directory)) {
            assertThrows(java.io.IOException.class, reopened::recover);
        }
    }

    @Test
    void orphanSnapshotIsRejectedInsteadOfRestoredOverOlderWal() throws Exception {
        ClusterConfiguration configuration = ClusterConfiguration.stable(Set.of(1L));
        try (FileRaftStorage storage = new FileRaftStorage(directory)) {
            storage.persist(new RaftEffect.PersistentState(
                    1, 1L, 0, 0, 0, configuration, List.of(RaftLogEntry.noop(0, 0))));
            storage.persistSnapshot(new RaftSnapshot(1, 1, configuration, new byte[] {7}));
        }

        try (FileRaftStorage reopened = new FileRaftStorage(directory)) {
            assertThrows(java.io.IOException.class, reopened::recover);
        }
    }
}
