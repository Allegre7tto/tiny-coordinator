package dev.talent.runtime;

import dev.talent.raft.ClusterConfiguration;
import dev.talent.raft.RaftEffect;
import dev.talent.raft.RaftLogEntry;
import dev.talent.raft.RaftSnapshot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.zip.CRC32C;

/**
 * Checksummed append-only Raft state journal plus atomically replaced snapshot.
 *
 * <p>Each journal record is a complete checkpoint. This intentionally favors
 * inspectability during learning while retaining append/flush/crash semantics;
 * obsolete checkpoints can later be compacted after a snapshot.</p>
 */
public final class FileRaftStorage implements RaftStorage {
    private static final int WAL_MAGIC = 0x5443574c; // TCWL
    private static final int SNAPSHOT_MAGIC = 0x54435353; // TCSS
    private static final int VERSION = 1;
    private static final int MAX_RECORD_BYTES = 256 * 1024 * 1024;

    private final Path directory;
    private final Path walPath;
    private final Path snapshotPath;
    private final FileChannel wal;

    public FileRaftStorage(Path directory) throws IOException {
        this.directory = directory.toAbsolutePath().normalize();
        Files.createDirectories(this.directory);
        walPath = this.directory.resolve("raft.wal");
        snapshotPath = this.directory.resolve("raft.snapshot");
        wal = FileChannel.open(
                walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        wal.position(wal.size());
    }

    @Override
    public synchronized Optional<Recovered> recover() throws IOException {
        RaftEffect.PersistentState state = readLastWalRecord();
        if (state == null) {
            return Optional.empty();
        }
        Optional<RaftSnapshot> snapshot = readSnapshot();
        long persistedSnapshotIndex =
                snapshot.map(RaftSnapshot::lastIncludedIndex).orElse(0L);
        if (persistedSnapshotIndex != state.snapshotIndex()) {
            throw new IOException("WAL references a missing or mismatched snapshot");
        }
        return Optional.of(new Recovered(state, snapshot));
    }

    @Override
    public synchronized void persist(RaftEffect.PersistentState state) throws IOException {
        byte[] payload = encodeState(state);
        writeRecord(wal, WAL_MAGIC, payload);
        wal.force(true);
    }

    @Override
    public synchronized void persistSnapshot(RaftSnapshot snapshot) throws IOException {
        byte[] payload = encodeSnapshot(snapshot);
        Path temporary = directory.resolve("raft.snapshot.tmp");
        try (FileChannel file = FileChannel.open(
                temporary,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            writeRecord(file, SNAPSHOT_MAGIC, payload);
            file.force(true);
        }
        try {
            Files.move(
                    temporary,
                    snapshotPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException unsupported) {
            Files.move(temporary, snapshotPath, StandardCopyOption.REPLACE_EXISTING);
        }
        try (FileChannel directoryHandle = FileChannel.open(directory, StandardOpenOption.READ)) {
            directoryHandle.force(true);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        wal.close();
    }

    private RaftEffect.PersistentState readLastWalRecord() throws IOException {
        wal.force(false);
        wal.position(0);
        RaftEffect.PersistentState last = null;
        ByteBuffer header = ByteBuffer.allocate(12);
        while (wal.position() < wal.size()) {
            header.clear();
            readFully(wal, header);
            header.flip();
            int magic = header.getInt();
            int version = header.getInt();
            int length = header.getInt();
            validateHeader(magic, WAL_MAGIC, version, length);
            ByteBuffer body = ByteBuffer.allocate(length + Integer.BYTES);
            readFully(wal, body);
            body.flip();
            byte[] payload = new byte[length];
            body.get(payload);
            verifyChecksum(payload, body.getInt());
            last = decodeState(payload);
        }
        wal.position(wal.size());
        return last;
    }

    private Optional<RaftSnapshot> readSnapshot() throws IOException {
        if (!Files.exists(snapshotPath)) {
            return Optional.empty();
        }
        try (FileChannel file = FileChannel.open(snapshotPath, StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(12);
            readFully(file, header);
            header.flip();
            int magic = header.getInt();
            int version = header.getInt();
            int length = header.getInt();
            validateHeader(magic, SNAPSHOT_MAGIC, version, length);
            ByteBuffer body = ByteBuffer.allocate(length + Integer.BYTES);
            readFully(file, body);
            if (file.position() != file.size()) {
                throw new IOException("snapshot contains trailing bytes");
            }
            body.flip();
            byte[] payload = new byte[length];
            body.get(payload);
            verifyChecksum(payload, body.getInt());
            return Optional.of(decodeSnapshot(payload));
        }
    }

    private static void writeRecord(FileChannel file, int magic, byte[] payload)
            throws IOException {
        CRC32C checksum = new CRC32C();
        checksum.update(payload, 0, payload.length);
        ByteBuffer record = ByteBuffer.allocate(12 + payload.length + Integer.BYTES);
        record.putInt(magic);
        record.putInt(VERSION);
        record.putInt(payload.length);
        record.put(payload);
        record.putInt((int) checksum.getValue());
        record.flip();
        while (record.hasRemaining()) {
            file.write(record);
        }
    }

    private static byte[] encodeState(RaftEffect.PersistentState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(state.term());
            out.writeBoolean(state.votedFor() != null);
            if (state.votedFor() != null) {
                out.writeLong(state.votedFor());
            }
            out.writeLong(state.commitIndex());
            out.writeLong(state.snapshotIndex());
            out.writeLong(state.snapshotTerm());
            writeConfiguration(out, state.configuration());
            out.writeInt(state.log().size());
            for (RaftLogEntry entry : state.log()) {
                writeEntry(out, entry);
            }
        }
        return bytes.toByteArray();
    }

    private static RaftEffect.PersistentState decodeState(byte[] bytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long term = in.readLong();
            Long votedFor = in.readBoolean() ? in.readLong() : null;
            long commitIndex = in.readLong();
            long snapshotIndex = in.readLong();
            long snapshotTerm = in.readLong();
            ClusterConfiguration configuration = readConfiguration(in);
            int count = checkedCount(in.readInt(), "log entries");
            List<RaftLogEntry> log = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                log.add(readEntry(in));
            }
            ensureConsumed(in);
            return new RaftEffect.PersistentState(
                    term,
                    votedFor,
                    commitIndex,
                    snapshotIndex,
                    snapshotTerm,
                    configuration,
                    log);
        }
    }

    private static byte[] encodeSnapshot(RaftSnapshot snapshot) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(snapshot.lastIncludedIndex());
            out.writeLong(snapshot.lastIncludedTerm());
            writeConfiguration(out, snapshot.configuration());
            writeBytes(out, snapshot.stateMachine());
        }
        return bytes.toByteArray();
    }

    private static RaftSnapshot decodeSnapshot(byte[] bytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long index = in.readLong();
            long term = in.readLong();
            ClusterConfiguration configuration = readConfiguration(in);
            byte[] stateMachine = readBytes(in);
            ensureConsumed(in);
            return new RaftSnapshot(index, term, configuration, stateMachine);
        }
    }

    private static void writeEntry(DataOutputStream out, RaftLogEntry entry) throws IOException {
        out.writeLong(entry.index());
        out.writeLong(entry.term());
        out.writeByte(entry.kind().ordinal());
        writeBytes(out, entry.command());
        writeSet(out, entry.oldVoters());
        writeSet(out, entry.newVoters());
        writeSet(out, entry.learners());
    }

    private static RaftLogEntry readEntry(DataInputStream in) throws IOException {
        long index = in.readLong();
        long term = in.readLong();
        int ordinal = in.readUnsignedByte();
        if (ordinal >= RaftLogEntry.Kind.values().length) {
            throw new IOException("unknown log entry kind " + ordinal);
        }
        return new RaftLogEntry(
                index,
                term,
                RaftLogEntry.Kind.values()[ordinal],
                readBytes(in),
                readSet(in),
                readSet(in),
                readSet(in));
    }

    private static void writeConfiguration(
            DataOutputStream out, ClusterConfiguration configuration) throws IOException {
        writeSet(out, configuration.oldVoters());
        writeSet(out, configuration.newVoters());
        writeSet(out, configuration.learners());
    }

    private static ClusterConfiguration readConfiguration(DataInputStream in) throws IOException {
        return new ClusterConfiguration(readSet(in), readSet(in), readSet(in));
    }

    private static void writeSet(DataOutputStream out, Set<Long> values) throws IOException {
        out.writeInt(values.size());
        for (long value : values.stream().sorted().toList()) {
            out.writeLong(value);
        }
    }

    private static Set<Long> readSet(DataInputStream in) throws IOException {
        int count = checkedCount(in.readInt(), "set");
        Set<Long> values = new LinkedHashSet<>();
        for (int i = 0; i < count; i++) {
            values.add(in.readLong());
        }
        if (values.size() != count) {
            throw new IOException("set contains duplicate values");
        }
        return Set.copyOf(values);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_RECORD_BYTES) {
            throw new IOException("invalid byte array length " + length);
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("truncated byte array");
        }
        return bytes;
    }

    private static int checkedCount(int count, String field) throws IOException {
        if (count < 0 || count > 10_000_000) {
            throw new IOException("invalid " + field + " count " + count);
        }
        return count;
    }

    private static void validateHeader(int magic, int expected, int version, int length)
            throws IOException {
        if (magic != expected) {
            throw new IOException("invalid storage magic");
        }
        if (version != VERSION) {
            throw new IOException("unsupported storage version " + version);
        }
        if (length < 0 || length > MAX_RECORD_BYTES) {
            throw new IOException("invalid record length " + length);
        }
    }

    private static void verifyChecksum(byte[] payload, int expected) throws IOException {
        CRC32C checksum = new CRC32C();
        checksum.update(payload, 0, payload.length);
        if ((int) checksum.getValue() != expected) {
            throw new IOException("storage checksum mismatch");
        }
    }

    private static void readFully(FileChannel file, ByteBuffer target) throws IOException {
        while (target.hasRemaining()) {
            if (file.read(target) < 0) {
                throw new EOFException("truncated storage record");
            }
        }
    }

    private static void ensureConsumed(DataInputStream in) throws IOException {
        if (in.available() != 0) {
            throw new IOException("storage record contains trailing bytes");
        }
    }
}
