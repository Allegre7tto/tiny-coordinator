package dev.talent.server.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

final class CoordinatorSnapshotCodec {
    private static final int MAGIC = 0x54434353; // TCCS
    private static final int VERSION = 1;
    private static final int MAX_BYTES = 256 * 1024 * 1024;

    record Decoded(
            long appliedIndex,
            long appliedTerm,
            long revision,
            long compactedRevision,
            NavigableMap<ByteKey, List<KvRecord>> histories,
            List<LeaseRegistry.Lease> leases) {}

    private CoordinatorSnapshotCodec() {}

    static byte[] encode(
            long appliedIndex,
            long appliedTerm,
            MvccStore store,
            LeaseRegistry leases) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(VERSION);
                out.writeLong(appliedIndex);
                out.writeLong(appliedTerm);
                out.writeLong(store.revision());
                out.writeLong(store.compactedRevision());
                var histories = store.copyHistories();
                out.writeInt(histories.size());
                for (var item : histories.entrySet()) {
                    writeBytes(out, item.getKey().bytes());
                    out.writeInt(item.getValue().size());
                    for (KvRecord value : item.getValue()) {
                        writeBytes(out, value.value());
                        out.writeLong(value.createRevision());
                        out.writeLong(value.modificationRevision());
                        out.writeLong(value.version());
                        out.writeLong(value.leaseId());
                        out.writeBoolean(value.tombstone());
                    }
                }
                List<LeaseRegistry.Lease> leaseList = leases.snapshot();
                out.writeInt(leaseList.size());
                for (LeaseRegistry.Lease lease : leaseList) {
                    out.writeLong(lease.id());
                    out.writeLong(lease.ttlSeconds());
                    out.writeLong(lease.deadlineEpochMillis());
                    out.writeInt(lease.keys().size());
                    for (ByteKey key : lease.keys().stream().sorted().toList()) {
                        writeBytes(out, key.bytes());
                    }
                }
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
    }

    static Decoded decode(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readInt() != MAGIC) {
                throw new IllegalArgumentException("invalid coordinator snapshot magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("unsupported coordinator snapshot " + version);
            }
            long appliedIndex = in.readLong();
            long appliedTerm = in.readLong();
            long revision = in.readLong();
            long compactedRevision = in.readLong();
            int keyCount = count(in.readInt());
            NavigableMap<ByteKey, List<KvRecord>> histories = new TreeMap<>();
            for (int i = 0; i < keyCount; i++) {
                ByteKey key = ByteKey.of(readBytes(in));
                int versionCount = count(in.readInt());
                List<KvRecord> versions = new ArrayList<>(versionCount);
                for (int j = 0; j < versionCount; j++) {
                    versions.add(new KvRecord(
                            key,
                            readBytes(in),
                            in.readLong(),
                            in.readLong(),
                            in.readLong(),
                            in.readLong(),
                            in.readBoolean()));
                }
                histories.put(key, List.copyOf(versions));
            }
            int leaseCount = count(in.readInt());
            List<LeaseRegistry.Lease> leases = new ArrayList<>(leaseCount);
            for (int i = 0; i < leaseCount; i++) {
                long id = in.readLong();
                long ttl = in.readLong();
                long deadline = in.readLong();
                int attachedCount = count(in.readInt());
                java.util.Set<ByteKey> keys = new java.util.LinkedHashSet<>();
                for (int j = 0; j < attachedCount; j++) {
                    keys.add(ByteKey.of(readBytes(in)));
                }
                leases.add(new LeaseRegistry.Lease(id, ttl, deadline, keys));
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("coordinator snapshot has trailing bytes");
            }
            return new Decoded(
                    appliedIndex,
                    appliedTerm,
                    revision,
                    compactedRevision,
                    histories,
                    List.copyOf(leases));
        } catch (IOException corrupt) {
            throw new IllegalArgumentException("corrupt coordinator snapshot", corrupt);
        }
    }

    private static int count(int value) {
        if (value < 0 || value > 10_000_000) {
            throw new IllegalArgumentException("invalid snapshot count " + value);
        }
        return value;
    }

    private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_BYTES) {
            throw new IllegalArgumentException("invalid snapshot byte length " + length);
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new IllegalArgumentException("truncated coordinator snapshot");
        }
        return bytes;
    }
}
