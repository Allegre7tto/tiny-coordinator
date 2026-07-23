package dev.talent.server;

import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

@Readiness
public class ReadinessCheck implements HealthCheck {
    @Inject
    CoordinatorNode node;

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("coordinator-caught-up")
                .status(node.ready())
                .withData("appliedIndex", node.stateMachine().appliedIndex())
                .withData("commitIndex", node.runtime().status().commitIndex())
                .build();
    }
}
