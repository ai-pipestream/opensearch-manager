package io.pipeline.schemamanager;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.jboss.logging.Logger;
import org.opensearch.testcontainers.OpenSearchContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

public class OpenSearchTestResource implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOG = Logger.getLogger(OpenSearchTestResource.class);

    private OpenSearchContainer<?> opensearch;
    private KafkaContainer kafka;

    @Override
    public Map<String, String> start() {
        Map<String, String> config = new HashMap<>();

        // Start Kafka container
        LOG.info("Starting Kafka test container...");
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))
                .withReuse(true);
        kafka.start();
        String kafkaBootstrapServers = kafka.getBootstrapServers();
        LOG.info("Kafka test container started at: " + kafkaBootstrapServers);

        // Start OpenSearch container
        LOG.info("Starting OpenSearch test container...");
        opensearch = new OpenSearchContainer<>(DockerImageName.parse("opensearchproject/opensearch:3.3.2"))
                .withAccessToHost(true)
                .withReuse(true);
        opensearch.start();
        LOG.info("OpenSearch test container started at: " + opensearch.getHost() + ":" + opensearch.getFirstMappedPort());

        String opensearchAddress = "http://" + opensearch.getHost() + ":" + opensearch.getFirstMappedPort();

        // Configure both services
        config.put("opensearch.hosts", opensearchAddress);
        config.put("kafka.bootstrap.servers", kafkaBootstrapServers);
        config.put("mp.messaging.connector.smallrye-kafka.bootstrap.servers", kafkaBootstrapServers);

        return config;
    }

    @Override
    public void stop() {
        if (opensearch != null) {
            LOG.info("Stopping OpenSearch test container...");
            opensearch.stop();
            LOG.info("OpenSearch test container stopped.");
        }
        if (kafka != null) {
            LOG.info("Stopping Kafka test container...");
            kafka.stop();
            LOG.info("Kafka test container stopped.");
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
