package ai.pipestream.schemamanager.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;

import javax.net.ssl.SSLContext;
import java.util.Optional;

/**
 * CDI producer for OpenSearch client beans with configurable connection settings.
 *
 * <p>This producer creates and configures singleton instances of OpenSearch clients
 * (both synchronous and asynchronous) with customizable connection parameters including
 * multi-host support, authentication, SSL/TLS configuration, and timeouts.
 *
 * <p><strong>Configuration Properties:</strong>
 * <ul>
 *   <li><strong>opensearch.hosts</strong> - Comma-separated list of hosts (default: "localhost:9200")</li>
 *   <li><strong>opensearch.protocol</strong> - Protocol to use: "http" or "https" (default: "http")</li>
 *   <li><strong>opensearch.username</strong> - Optional basic auth username</li>
 *   <li><strong>opensearch.password</strong> - Optional basic auth password</li>
 *   <li><strong>opensearch.ssl.verify</strong> - Verify SSL certificates (default: true)</li>
 *   <li><strong>opensearch.connection-timeout</strong> - Connection timeout in ms (default: 5000)</li>
 *   <li><strong>opensearch.socket-timeout</strong> - Socket/response timeout in ms (default: 10000)</li>
 * </ul>
 *
 * <p><strong>Features:</strong>
 * <ul>
 *   <li>Multi-host configuration for high availability</li>
 *   <li>Basic authentication support</li>
 *   <li>SSL/TLS with optional certificate verification bypass (for dev/test)</li>
 *   <li>Configurable connection and socket timeouts</li>
 *   <li>Connection pooling via Apache HttpClient 5</li>
 * </ul>
 *
 * <p><strong>Security Warning:</strong><br>
 * Setting {@code opensearch.ssl.verify=false} disables SSL certificate verification
 * and should only be used in development/test environments with self-signed certificates.
 *
 * @author PipeStream.ai
 * @version 1.0
 * @see OpenSearchClient
 * @see OpenSearchAsyncClient
 */
@ApplicationScoped
public class OpenSearchClientProducer {

    /**
     * Comma-separated list of OpenSearch hosts (e.g., "host1:9200,host2:9200").
     * Defaults to "localhost:9200".
     */
    @ConfigProperty(name = "opensearch.hosts", defaultValue = "localhost:9200")
    String hosts;

    /**
     * Protocol to use for OpenSearch connections ("http" or "https").
     * Defaults to "http".
     */
    @ConfigProperty(name = "opensearch.protocol", defaultValue = "http")
    String protocol;

    /**
     * Optional username for basic authentication.
     * If present and non-blank, basic auth will be configured.
     */
    @ConfigProperty(name = "opensearch.username")
    Optional<String> username;

    /**
     * Optional password for basic authentication.
     * Used in conjunction with username.
     */
    @ConfigProperty(name = "opensearch.password")
    Optional<String> password;

    /**
     * Whether to verify SSL certificates when using HTTPS.
     * Set to false to disable verification (development/test only).
     * Defaults to true.
     */
    @ConfigProperty(name = "opensearch.ssl.verify", defaultValue = "true")
    boolean sslVerify;

    /**
     * Connection timeout in milliseconds.
     * Maximum time to wait for connection establishment.
     * Defaults to 5000ms (5 seconds).
     */
    @ConfigProperty(name = "opensearch.connection-timeout", defaultValue = "5000")
    int connectTimeout;

    /**
     * Socket/response timeout in milliseconds.
     * Maximum time to wait for a response after sending a request.
     * Defaults to 10000ms (10 seconds).
     */
    @ConfigProperty(name = "opensearch.socket-timeout", defaultValue = "10000")
    int socketTimeout;

    /**
     * Produces a configured Apache HttpClient 5 transport for OpenSearch.
     *
     * <p>This method creates the underlying HTTP transport layer used by both
     * synchronous and asynchronous OpenSearch clients. The transport is configured with:
     * <ul>
     *   <li>Multiple hosts for failover and load balancing</li>
     *   <li>Basic authentication credentials (if provided)</li>
     *   <li>SSL/TLS configuration with optional certificate verification bypass</li>
     *   <li>Connection and socket timeouts</li>
     *   <li>Jackson JSON mapper for request/response serialization</li>
     * </ul>
     *
     * <p>The transport is created as a singleton and shared by all OpenSearch clients.
     *
     * @return a configured ApacheHttpClient5Transport instance
     * @throws RuntimeException if transport creation fails (e.g., SSL setup error)
     */
    @Produces
    @Singleton
    public ApacheHttpClient5Transport openSearchTransport() {
        try {
            String[] hostParts = hosts.split(",");
            HttpHost[] httpHosts = new HttpHost[hostParts.length];
            for (int i = 0; i < hostParts.length; i++) {
                httpHosts[i] = HttpHost.create(hostParts[i]);
            }

            SSLContext sslContext = "https".equals(protocol) && !sslVerify
                    ? SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build()
                    : null;

            var transportBuilder = ApacheHttpClient5TransportBuilder.builder(httpHosts);
            transportBuilder.setMapper(new JacksonJsonpMapper());
            transportBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                if (username.isPresent() && password.isPresent() && 
                    !username.get().isBlank() && !password.get().isBlank()) {
                    final var credentialsProvider = new BasicCredentialsProvider();
                    for (final var httpHost : httpHosts) {
                        credentialsProvider.setCredentials(new AuthScope(httpHost), 
                            new UsernamePasswordCredentials(username.get(), password.get().toCharArray()));
                    }
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
                if (sslContext != null) {
                    final var tlsStrategy = ClientTlsStrategyBuilder.create().setSslContext(sslContext).setHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
                    final var connectionManager = PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build();
                    httpClientBuilder.setConnectionManager(connectionManager);
                }

                // Set timeouts from application.properties
                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(connectTimeout))
                        .setResponseTimeout(Timeout.ofMilliseconds(socketTimeout))
                        .build();
                httpClientBuilder.setDefaultRequestConfig(requestConfig);

                return httpClientBuilder;
            });

            return transportBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create OpenSearch transport", e);
        }
    }

    /**
     * Produces a synchronous OpenSearch client for blocking operations.
     *
     * <p>This client is suitable for:
     * <ul>
     *   <li>Index management operations (create, delete, get mappings)</li>
     *   <li>Synchronous indexing in worker threads</li>
     *   <li>Administrative operations</li>
     * </ul>
     *
     * <p>The client is created as a singleton and uses the shared transport.
     *
     * @param transport the Apache HttpClient 5 transport to use
     * @return a configured synchronous OpenSearchClient
     */
    @Produces
    @Singleton
    public OpenSearchClient openSearchClient(ApacheHttpClient5Transport transport) {
        return new OpenSearchClient(transport);
    }

    /**
     * Produces an asynchronous OpenSearch client for non-blocking operations.
     *
     * <p>This client is suitable for:
     * <ul>
     *   <li>High-throughput indexing operations</li>
     *   <li>Search queries in reactive pipelines</li>
     *   <li>Operations requiring CompletableFuture-based async execution</li>
     * </ul>
     *
     * <p>The client is created as a singleton and uses the shared transport.
     *
     * @param transport the Apache HttpClient 5 transport to use
     * @return a configured asynchronous OpenSearchAsyncClient
     */
    @Produces
    @Singleton
    public OpenSearchAsyncClient openSearchAsyncClient(ApacheHttpClient5Transport transport) {
        return new OpenSearchAsyncClient(transport);
    }
}
