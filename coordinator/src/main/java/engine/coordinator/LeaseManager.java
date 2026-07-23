package engine.coordinator;

import engine.coordinator.v1.CoordinatorOuterClass.*;

import com.google.protobuf.ByteString;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class LeaseManager {

    private static final Logger LOG = Logger.getLogger(LeaseManager.class);

    @Inject StateMachineDriver driver;

    private record Lease(
        long              id,
        long              ttlSeconds,
        Instant           deadline,
        Set<ByteString>   keys
    ) {
        Lease withDeadline(Instant d) { return new Lease(id, ttlSeconds, d, keys); }
    }

    private final Map<Long, Lease> leases = new ConcurrentHashMap<>();
    private final AtomicLong nextLeaseId = new AtomicLong(1);

    public long generateId() { return nextLeaseId.getAndIncrement(); }

    public void applyGrant(long id, long ttlSeconds) {
        Instant deadline = Instant.now().plusSeconds(ttlSeconds);
        leases.put(id, new Lease(id, ttlSeconds, deadline, ConcurrentHashMap.newKeySet()));
        LOG.debugf("Lease applied: grant id=%d ttl=%ds", id, ttlSeconds);
    }

    public void applyRevoke(long id) {
        Lease lease = leases.remove(id);
        if (lease != null) {
            LOG.debugf("Lease applied: revoke id=%d (%d keys)", id, lease.keys().size());
        }
    }

    public void renewOnApply(long id) {
        Lease lease = leases.get(id);
        if (lease == null) throw new IllegalArgumentException("lease not found: " + id);
        Instant newDeadline = Instant.now().plusSeconds(lease.ttlSeconds());
        leases.put(id, lease.withDeadline(newDeadline));
    }

    public void attach(long id, ByteString key) {
        Lease lease = leases.get(id);
        if (lease != null) lease.keys().add(key);
    }

    public void detach(long id, ByteString key) {
        Lease lease = leases.get(id);
        if (lease != null) lease.keys().remove(key);
    }

    public Set<ByteString> keysOf(long id) {
        Lease lease = leases.get(id);
        return (lease != null) ? Set.copyOf(lease.keys()) : Set.of();
    }

    public long remaining(long id) {
        Lease lease = leases.get(id);
        if (lease == null) return -1;
        return Math.max(0, Instant.now().until(lease.deadline(), ChronoUnit.SECONDS));
    }

    public boolean exists(long id) { return leases.containsKey(id); }

    @Scheduled(every = "1s")
    void checkExpiry() {
        if (!driver.isLeader()) return;

        Instant now = Instant.now();
        List<Long> expiredIds = new ArrayList<>();
        leases.forEach((id, lease) -> {
            if (now.isAfter(lease.deadline())) expiredIds.add(id);
        });
        for (long id : expiredIds) {
            LOG.infof("Lease expired id=%d, proposing expire", id);
            try {
                driver.proposeLeaseExpire(id);
            } catch (Exception e) {
                LOG.warnf("Failed to propose lease expire id=%d: %s", id, e.getMessage());
            }
        }
    }

    public List<LeaseSnapshot> allLeases() {
        List<LeaseSnapshot> result = new ArrayList<>();
        leases.forEach((id, l) -> result.add(new LeaseSnapshot(id, l.ttlSeconds(), l.deadline(), Set.copyOf(l.keys()))));
        return result;
    }

    public record LeaseSnapshot(long id, long ttlSeconds, Instant deadline, Set<ByteString> keys) {}

    public void restoreLease(long id, long ttlSeconds, Instant deadline, Set<ByteString> keys) {
        Lease lease = new Lease(id, ttlSeconds, deadline, ConcurrentHashMap.newKeySet());
        lease.keys().addAll(keys);
        leases.put(id, lease);
    }
}
