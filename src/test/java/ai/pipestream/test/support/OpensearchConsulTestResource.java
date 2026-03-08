package ai.pipestream.test.support;

import org.jboss.logging.Logger;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Test resource that starts OpenSearch and Consul, then registers OpenSearch's
 * gRPC service in Consul under the logical name {@code opensearch-grpc}.
 */
public class OpensearchConsulTestResource implements QuarkusTestResourceLifecycleManager {
    private static final Logger LOG = Logger.getLogger(OpensearchConsulTestResource.class);
    private static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch:3.4.0";
    private static final String CONSUL_IMAGE = "hashicorp/consul:1.22";
    private static final String OPENSEARCH_GRPC_SERVICE_NAME = "opensearch-grpc";
    private static final int CONSUL_HTTP_PORT = 8500;
    private static final int OPENSEARCH_HTTP_PORT = 9200;
    private static final int OPENSEARCH_GRPC_PORT = 9400;

    private GenericContainer<?> opensearchContainer;
    private GenericContainer<?> consulContainer;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    @Override
    public Map<String, String> start() {
        consulContainer = new GenericContainer<>(DockerImageName.parse(CONSUL_IMAGE))
                .withCommand("agent", "-dev", "-ui", "-client=0.0.0.0", "-log-level=INFO")
                .withExposedPorts(CONSUL_HTTP_PORT)
                .waitingFor(Wait.forHttp("/v1/status/leader").forPort(CONSUL_HTTP_PORT).forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        consulContainer.start();

        opensearchContainer = new GenericContainer<>(DockerImageName.parse(OPENSEARCH_IMAGE))
                .withEnv("DISABLE_SECURITY_PLUGIN", "true")
                .withEnv("discovery.type", "single-node")
                .withEnv("bootstrap.memory_lock", "true")
                .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                .withEnv("aux.transport.types", "[\"transport-grpc\"]")
                .withEnv("aux.transport.transport-grpc.port", String.format("%d-%d", OPENSEARCH_GRPC_PORT, OPENSEARCH_GRPC_PORT))
                .withExposedPorts(OPENSEARCH_HTTP_PORT, OPENSEARCH_GRPC_PORT)
                .waitingFor(Wait.forHttp("/").forPort(OPENSEARCH_HTTP_PORT).forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        opensearchContainer.start();

        String opensearchHost = opensearchContainer.getHost();
        int grpcPort = opensearchContainer.getMappedPort(OPENSEARCH_GRPC_PORT);
        int httpPort = opensearchContainer.getMappedPort(OPENSEARCH_HTTP_PORT);
        String httpHost = opensearchHost + ":" + httpPort;

        String consulHost = consulContainer.getHost();
        int consulPort = consulContainer.getMappedPort(CONSUL_HTTP_PORT);

        registerOpensearchGrpcServiceInConsul(opensearchHost, grpcPort, consulHost, consulPort);

        Map<String, String> config = new HashMap<>();
        config.put("opensearch.hosts", httpHost);
        config.put("opensearch.protocol", "http");
        config.put("quarkus.compose.devservices.enabled", "false");

        // Dynamic gRPC Consul configuration
        config.put("quarkus.dynamic-grpc.consul.host", consulHost);
        config.put("quarkus.dynamic-grpc.consul.port", String.valueOf(consulPort));
        config.put("quarkus.dynamic-grpc.consul.refresh-period", "2s");
        config.put("quarkus.dynamic-grpc.consul.use-health-checks", "false");

        // Keep compatibility with Quarkus/stork property style if present
        config.put("stork.opensearch-grpc.service-discovery.type", "consul");
        config.put("stork.opensearch-grpc.service-discovery.application", OPENSEARCH_GRPC_SERVICE_NAME);
        config.put("stork.opensearch-grpc.service-discovery.consul-host", consulHost);
        config.put("stork.opensearch-grpc.service-discovery.consul-port", String.valueOf(consulPort));
        config.put("stork.opensearch-grpc.service-discovery.refresh-period", "2s");
        config.put("stork.opensearch-grpc.service-discovery.use-health-checks", "false");

        LOG.infof("Started OpenSearch at %s:%d (gRPC=%s:%d) and Consul at %s:%d",
                opensearchHost, httpPort, opensearchHost, grpcPort, consulHost, consulPort);
        return config;
    }

    private void registerOpensearchGrpcServiceInConsul(String serviceHost, int servicePort, String consulHost, int consulPort) {
        String serviceId = OPENSEARCH_GRPC_SERVICE_NAME + "-" + UUID.randomUUID();
        String payload = String.format(
                """
                        {
                          "ID": "%s",
                          "Name": "%s",
                          "Address": "%s",
                          "Port": %d,
                          "Tags": ["grpc", "opensearch", "opensearch-grpc"]
                        }
                        """,
                serviceId, OPENSEARCH_GRPC_SERVICE_NAME, serviceHost, servicePort
        );

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d/v1/agent/service/register", consulHost, consulPort)))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .method("PUT", HttpRequest.BodyPublishers.ofString(payload))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("Consul service registration failed: " + response.statusCode() + " " + response.body());
            }
            LOG.infof("Registered service %s in Consul (id=%s) at %s:%d", OPENSEARCH_GRPC_SERVICE_NAME, serviceId, serviceHost, servicePort);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Failed to register opensearch-grpc service in Consul", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to register opensearch-grpc service in Consul", e);
        }
    }

    @Override
    public void stop() {
        if (opensearchContainer != null) {
            opensearchContainer.stop();
        }
        if (consulContainer != null) {
            consulContainer.stop();
        }
    }
}

