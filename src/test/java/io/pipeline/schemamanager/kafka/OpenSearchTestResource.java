package io.pipeline.schemamanager.kafka;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.Map;

public class OpenSearchTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> opensearch;

    @Override
    public Map<String, String> start() {
        opensearch = new GenericContainer<>("opensearchproject/opensearch:2.11.0")
                .withExposedPorts(9200)
                .withEnv("discovery.type", "single-node")
                .withEnv("plugins.security.disabled", "true")
                .waitingFor(Wait.forHttp("/").forStatusCode(200));
        opensearch.start();

        return Map.of(
                "opensearch.hosts", opensearch.getHost() + ":" + opensearch.getMappedPort(9200)
        );
    }

    @Override
    public void stop() {
        if (opensearch != null) {
            opensearch.stop();
        }
    }
}
