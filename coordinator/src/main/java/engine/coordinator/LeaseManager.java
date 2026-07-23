package engine.coordinator;

import engine.coordinator.v1.CoordinatorOuterClass.*;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class LeaseManager {

    private static final Logger LOG = Logger.getLogger(LeaseManager.class);

    @Inject StateMachineDriver driver;

    private record Lease(
        long       id,
        long       ttlSeconds,
        Instant    deadline,
        Set<String> keys
    ) {
        Lease withDeadline(Instant d) { return new Lease(id, ttlSeconds, d, keys); }
    }

    private final Map<Long, Lease> leases = new ConcurrentHashMap<>();

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

    public long keepAlive(long id) {
        Lease lease = leases.get(id);
        if (lease == null) throw new IllegalArgumentException("lease not found: " + id);
        Instant newDeadline = Instant.now().plusSeconds(lease.ttlSeconds());
        leases.put(id, lease.withDeadline(newDeadline));
        return lease.ttlSeconds();
    }

    public void attach(long id, String key) {
        Lease lease = leases.get(id);
        if (lease != null) lease.keys().add(key);
    }

    public void detach(long id, String key) {
        Lease lease = leases.get(id);
        if (lease != null) lease.keys().remove(key);
    }

    public Set<String> keysOf(long id) {
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
        Instant now = Instant.now();
        leases.forEach((id, lease) -> {
            if (now.isAfter(lease.deadline())) {
                LOG.infof("Lease expired id=%d, proposing revoke for %d keys", id, lease.keys().size());
                try {
                    driver.propose(StateMachineDriver.OP_LEASE_REVOKE,
                            LeaseRevokeRequest.newBuilder().setId(id).build());

                    for (String key : lease.keys()) {
                        driver.propose(StateMachineDriver.OP_DELETE,
                                DeleteRequest.newBuilder()
                                        .setKey(com.google.protobuf.ByteString.copyFromUtf8(key))
                                        .build());
                    }
                } catch (Exception e) {
                    LOG.warnf("Failed to propose lease revoke for id=%d: %s", id, e.getMessage());
                }
            }
        });
    }
}
