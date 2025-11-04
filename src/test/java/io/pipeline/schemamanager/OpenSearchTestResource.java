package io.pipeline.schemamanager;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.jboss.logging.Logger;
import org.opensearch.testcontainers.OpenSearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import java.util.Map;

public class OpenSearchTestResource implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOG = Logger.getLogger(OpenSearchTestResource.class);

    private OpenSearchContainer<?> opensearch;

    @Override
    public Map<String, String> start() {
        LOG.info("Starting OpenSearch test container...");
        opensearch = new OpenSearchContainer<>(DockerImageName.parse("opensearchproject/opensearch:3")).withAccessToHost(true).withReuse(true);
        opensearch.start();
        LOG.info("OpenSearch test container started at: " + opensearch.getHost() + ":" + opensearch.getFirstMappedPort());

        String address = "http://" + opensearch.getHost() + ":" + opensearch.getFirstMappedPort();
        return Map.of("quarkus.opensearch.hosts", address);
    }

    @Override
    public void stop() {
        if (opensearch != null) {
            LOG.info("Stopping OpenSearch test container...");
            opensearch.stop();
            LOG.info("OpenSearch test container stopped.");
        }
    }

    @Override
    public void inject(Object testInstance) {
        // No-op, but method provided for completeness
    }

    /**
     * Returns the running OpenSearchContainer instance for advanced test usage.
     */
    public OpenSearchContainer<?> getOpenSearchContainer() {
        return opensearch;
    }
}
