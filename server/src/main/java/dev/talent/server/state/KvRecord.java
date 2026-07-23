package dev.talent.server.state;

import java.util.Objects;

public record KvRecord(
        ByteKey key,
        byte[] value,
        long createRevision,
        long modificationRevision,
        long version,
        long leaseId,
        boolean tombstone) {

    public KvRecord {
        key = Objects.requireNonNull(key, "key");
        value = value == null ? new byte[0] : value.clone();
        if (createRevision <= 0 || modificationRevision <= 0 || version <= 0) {
            throw new IllegalArgumentException("MVCC metadata must be positive");
        }
    }

    @Override
    public byte[] value() {
        return value.clone();
    }

    public KvRecord withoutValue() {
        return new KvRecord(
                key, new byte[0], createRevision, modificationRevision, version, leaseId, tombstone);
    }
}
