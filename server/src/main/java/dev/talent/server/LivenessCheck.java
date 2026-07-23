package dev.talent.server;

import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

@Liveness
public class LivenessCheck implements HealthCheck {
    @Inject
    CoordinatorNode node;

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("coordinator-runtime")
                .status(node.live())
                .build();
    }
}
