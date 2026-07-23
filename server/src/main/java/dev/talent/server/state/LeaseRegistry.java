package dev.talent.server.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class LeaseRegistry {
    public record Lease(long id, long ttlSeconds, long deadlineEpochMillis, Set<ByteKey> keys) {
        public Lease {
            keys = Set.copyOf(keys);
        }
    }

    private static final class MutableLease {
        private final long id;
        private final long ttlSeconds;
        private long deadlineEpochMillis;
        private final Set<ByteKey> keys = new LinkedHashSet<>();

        private MutableLease(long id, long ttlSeconds, long deadlineEpochMillis) {
            this.id = id;
            this.ttlSeconds = ttlSeconds;
            this.deadlineEpochMillis = deadlineEpochMillis;
        }
    }

    private final Map<Long, MutableLease> leases = new HashMap<>();
    private final Map<ByteKey, Long> keyToLease = new HashMap<>();

    public void grant(long id, long ttlSeconds, long deadlineEpochMillis) {
        if (id <= 0 || ttlSeconds <= 0 || deadlineEpochMillis <= 0) {
            throw new IllegalArgumentException("invalid lease");
        }
        if (leases.putIfAbsent(id, new MutableLease(id, ttlSeconds, deadlineEpochMillis)) != null) {
            throw new IllegalArgumentException("lease already exists: " + id);
        }
    }

    public boolean exists(long id) {
        return id == 0 || leases.containsKey(id);
    }

    public boolean hasLease(long id) {
        return leases.containsKey(id);
    }

    public void keepAlive(long id, long deadlineEpochMillis) {
        MutableLease lease = require(id);
        if (deadlineEpochMillis <= lease.deadlineEpochMillis) {
            throw new IllegalArgumentException("lease deadline must move forward");
        }
        lease.deadlineEpochMillis = deadlineEpochMillis;
    }

    public void attach(ByteKey key, long leaseId) {
        detach(key);
        if (leaseId == 0) {
            return;
        }
        MutableLease lease = require(leaseId);
        lease.keys.add(key);
        keyToLease.put(key, leaseId);
    }

    public void detach(ByteKey key) {
        Long oldLease = keyToLease.remove(key);
        if (oldLease != null) {
            MutableLease lease = leases.get(oldLease);
            if (lease != null) {
                lease.keys.remove(key);
            }
        }
    }

    public List<ByteKey> revoke(long id) {
        MutableLease lease = leases.remove(id);
        if (lease == null) {
            throw new IllegalArgumentException("unknown lease " + id);
        }
        lease.keys.forEach(keyToLease::remove);
        return List.copyOf(lease.keys);
    }

    public List<Long> expiredAt(long epochMillis) {
        return leases.values().stream()
                .filter(lease -> lease.deadlineEpochMillis <= epochMillis)
                .map(lease -> lease.id)
                .sorted()
                .toList();
    }

    public Lease get(long id) {
        MutableLease lease = require(id);
        return new Lease(lease.id, lease.ttlSeconds, lease.deadlineEpochMillis, lease.keys);
    }

    public List<Lease> snapshot() {
        return leases.values().stream()
                .map(lease -> new Lease(
                        lease.id, lease.ttlSeconds, lease.deadlineEpochMillis, lease.keys))
                .sorted(java.util.Comparator.comparingLong(Lease::id))
                .toList();
    }

    public void restore(List<Lease> restored) {
        leases.clear();
        keyToLease.clear();
        for (Lease lease : restored) {
            MutableLease mutable =
                    new MutableLease(lease.id(), lease.ttlSeconds(), lease.deadlineEpochMillis());
            mutable.keys.addAll(lease.keys());
            if (leases.put(lease.id(), mutable) != null) {
                throw new IllegalArgumentException("duplicate lease " + lease.id());
            }
            for (ByteKey key : lease.keys()) {
                if (keyToLease.put(key, lease.id()) != null) {
                    throw new IllegalArgumentException("key attached to multiple leases");
                }
            }
        }
    }

    private MutableLease require(long id) {
        MutableLease lease = leases.get(id);
        if (lease == null) {
            throw new IllegalArgumentException("unknown lease " + id);
        }
        return lease;
    }
}
