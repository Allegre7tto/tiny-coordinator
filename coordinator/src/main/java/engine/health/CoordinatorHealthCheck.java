package engine.health;

import engine.client.RaftLogClient;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.health.Readiness;

/**
 * Kubernetes health probes.
 *
 *  /q/health/live   — is the Java process alive?
 *  /q/health/ready  — can the C++ Raft engine be reached and is a leader elected?
 */
@ApplicationScoped
@Liveness
@Readiness
public class CoordinatorHealthCheck implements HealthCheck {

    @Inject RaftLogClient raftClient;

    @Override
    public HealthCheckResponse call() {
        try {
            var status = raftClient.status();
            return HealthCheckResponse.named("cpp-raft-engine")
                    .status(true)
                    .withData("is_leader", status.getIsLeader())
                    .withData("term",      status.getTerm())
                    .build();
        } catch (Exception e) {
            return HealthCheckResponse.named("cpp-raft-engine")
                    .down()
                    .withData("error", e.getMessage())
                    .build();
        }
    }
}
