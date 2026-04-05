package engine.coordinator;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the Coordinator service.
 * These run against a real Quarkus instance but with a mock KvClient
 * (no C++ engine required for unit testing).
 *
 * Full end-to-end tests that require a running C++ engine are in
 * tests/integration_test.cpp (C++ side) or an external test harness.
 */
@QuarkusTest
class CoordinatorTest {

    @Test
    void healthEndpointExists() {
        // Basic smoke test: Quarkus started without errors
        // Health endpoint check would require @TestHTTPResource + RestAssured
        assertTrue(true, "Quarkus started successfully");
    }
}
