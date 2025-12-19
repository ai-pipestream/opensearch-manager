package ai.pipestream.schemamanager.util;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

/**
 * Test resource that starts the Pipestream WireMock server container.
 * <p>
 * This provides a real gRPC server that mocks the platform-registration-service
 * and other services. It handles both unary and streaming gRPC calls.
 */
public class WireMockTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> wireMockContainer;

    @Override
    public Map<String, String> start() {
        wireMockContainer = new GenericContainer<>(
                DockerImageName.parse("docker.io/pipestreamai/pipestream-wiremock-server:0.1.21"))
                .withExposedPorts(50052)
                .waitingFor(Wait.forLogMessage(".*Direct Streaming gRPC Server started.*", 1));
        
        wireMockContainer.start();

        int grpcPort = wireMockContainer.getMappedPort(50052);

        return Map.of(
            // Disable real service registration
            "pipestream.registration.enabled", "false",
            
            // Route platform-registration client to the mock
            "pipestream.registration.registration-service.host", wireMockContainer.getHost(),
            "pipestream.registration.registration-service.port", String.valueOf(grpcPort)
        );
    }

    @Override
    public void stop() {
        if (wireMockContainer != null) {
            wireMockContainer.stop();
        }
    }
}
