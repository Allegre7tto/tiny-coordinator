package dev.talent.server;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class GrpcRegistrationTest {
    @Inject
    CoordinatorNode node;

    @Inject
    @GrpcService
    CoordinatorGrpcApi coordinatorApi;

    @Inject
    @GrpcService
    PeerGrpcService peerApi;

    @Test
    void bothGrpcServicesAreRegisteredAndRuntimeIsLive() {
        assertNotNull(coordinatorApi);
        assertNotNull(peerApi);
        assertTrue(node.live());
    }
}
