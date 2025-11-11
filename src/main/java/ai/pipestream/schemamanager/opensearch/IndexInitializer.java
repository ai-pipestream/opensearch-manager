package ai.pipestream.schemamanager.opensearch;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TextProperty;
import org.opensearch.client.opensearch._types.mapping.KeywordProperty;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch._types.mapping.DynamicMapping;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static ai.pipestream.schemamanager.opensearch.IndexConstants.Index;

/**
 * Initializes OpenSearch indices on application startup.
 *
 * <p>This service observes the {@link StartupEvent} and ensures that required
 * OpenSearch indices exist with proper mappings before the application begins
 * processing requests. If indices already exist, they are left unchanged.
 *
 * <p><strong>Initialized Indices:</strong>
 * <ul>
 *   <li><strong>repository-pipedocs</strong> - PipeDoc documents with metadata and tags</li>
 *   <li><strong>filesystem-nodes</strong> - File and folder nodes with path hierarchy</li>
 *   <li><strong>filesystem-drives</strong> - Storage drive configurations</li>
 * </ul>
 *
 * <p><strong>Index Configuration:</strong><br>
 * All indices are created with:
 * <ul>
 *   <li>1 primary shard (suitable for development and small deployments)</li>
 *   <li>0 replica shards (no replication for development)</li>
 *   <li>Type mappings appropriate for each entity type</li>
 * </ul>
 *
 * <p><strong>Retry Strategy:</strong><br>
 * Operations are retried up to 6 times with exponential backoff starting at 250ms
 * to handle transient connection issues during startup. This is especially useful
 * when OpenSearch is starting up at the same time as the application.
 *
 * <p><strong>Error Handling:</strong><br>
 * Index creation errors are logged but do not prevent application startup. This allows
 * the application to start even if OpenSearch is temporarily unavailable, though
 * indexing operations will fail until connectivity is restored.
 *
 * @author PipeStream.ai
 * @version 1.0
 * @see IndexConstants.Index
 */
@ApplicationScoped
public class IndexInitializer {

    private static final Logger LOG = Logger.getLogger(IndexInitializer.class);

    /**
     * Synchronous OpenSearch client for index management operations.
     * Injected via CDI.
     */
    @Inject
    OpenSearchClient client;

    /**
     * Observes the application startup event and initializes OpenSearch indices.
     *
     * <p>This method is called automatically by Quarkus during application startup.
     * It creates three core indices if they don't already exist. Errors are logged
     * but do not prevent the application from starting.
     *
     * @param ev the startup event (unused)
     */
    public void onStart(@Observes StartupEvent ev) {
        try {
            ensureIndex(Index.REPOSITORY_PIPEDOCS.getIndexName(), buildPipeDocsMapping());
            ensureIndex(Index.FILESYSTEM_NODES.getIndexName(), buildFilesystemNodesMapping());
            ensureIndex(Index.FILESYSTEM_DRIVES.getIndexName(), buildFilesystemDrivesMapping());
        } catch (Exception e) {
            LOG.error("Failed to initialize OpenSearch indices", e);
        }
    }

    /**
     * Ensures an OpenSearch index exists with the specified mapping.
     *
     * <p>This method:
     * <ol>
     *   <li>Checks if the index already exists (with retry)</li>
     *   <li>If not, creates the index with 1 shard, 0 replicas, and the provided mapping</li>
     * </ol>
     *
     * <p>Both the existence check and index creation use exponential backoff retry
     * to handle transient connectivity issues.
     *
     * @param indexName the name of the index to create
     * @param mapping the type mapping for the index
     * @throws IOException if index creation fails after all retries
     */
    private void ensureIndex(String indexName, TypeMapping mapping) throws IOException {
        boolean exists = executeWithRetry("check index existence for " + indexName,
            () -> client.indices().exists(new ExistsRequest.Builder().index(indexName).build()).value());
        if (exists) {
            return;
        }
        LOG.infof("Creating OpenSearch index '%s' (1 shard, 0 replicas)", indexName);
        var settings = new IndexSettings.Builder()
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
        CreateIndexRequest req = new CreateIndexRequest.Builder()
                .index(indexName)
                .settings(settings)
                .mappings(mapping)
                .build();
        executeWithRetry("create index " + indexName, () -> {
            client.indices().create(req);
            return null;
        });
    }

    /**
     * Builds the type mapping for the repository-pipedocs index.
     *
     * <p>This mapping includes:
     * <ul>
     *   <li><strong>doc_id</strong> - Keyword (exact match)</li>
     *   <li><strong>storage_id</strong> - Keyword (exact match)</li>
     *   <li><strong>title</strong> - Text (full-text search)</li>
     *   <li><strong>title_raw</strong> - Keyword (sorting/aggregations)</li>
     *   <li><strong>author</strong> - Text (full-text search)</li>
     *   <li><strong>author_raw</strong> - Keyword (sorting/aggregations)</li>
     *   <li><strong>description</strong> - Text (full-text search)</li>
     *   <li><strong>tags</strong> - Object with dynamic mapping (flexible metadata)</li>
     *   <li><strong>created_at, updated_at, indexed_at</strong> - Date fields</li>
     * </ul>
     *
     * @return the configured TypeMapping for PipeDocs
     */
    private TypeMapping buildPipeDocsMapping() {
        // Basic metadata-centric mapping for PipeDocs (keyword search for now)
        TypeMapping.Builder b = new TypeMapping.Builder();
        b.properties("doc_id", Property.of(p -> p.keyword(KeywordProperty.of(k -> k))));
        b.properties("storage_id", Property.of(p -> p.keyword(KeywordProperty.of(k -> k))));
        b.properties("title", Property.of(p -> p.text(TextProperty.of(t -> t))));
        b.properties("title_raw", Property.of(p -> p.keyword(KeywordProperty.of(k -> k))));
        b.properties("author", Property.of(p -> p.text(TextProperty.of(t -> t))));
        b.properties("author_raw", Property.of(p -> p.keyword(KeywordProperty.of(k -> k))));
        b.properties("description", Property.of(p -> p.text(TextProperty.of(t -> t))));
        b.properties("tags", Property.of(p -> p.object(o -> o.dynamic(DynamicMapping.True))));
        b.properties("created_at", Property.of(p -> p.date(d -> d)));
        b.properties("updated_at", Property.of(p -> p.date(d -> d)));
        b.properties("indexed_at", Property.of(p -> p.date(d -> d)));
        return b.build();
    }

    /**
     * Builds the type mapping for the filesystem-nodes index.
     *
     * <p>This mapping includes:
     * <ul>
     *   <li><strong>node_id</strong> - Keyword (exact match)</li>
     *   <li><strong>drive</strong> - Keyword (filtering by drive)</li>
     *   <li><strong>name</strong> - Text (full-text search)</li>
     *   <li><strong>name_text</strong> - Text (duplicate for analysis)</li>
     *   <li><strong>path</strong> - Keyword (exact path matching)</li>
     *   <li><strong>path_text</strong> - Text (path components search)</li>
     *   <li><strong>node_type</strong> - Keyword (FILE/FOLDER filtering)</li>
     *   <li><strong>tags</strong> - Object with dynamic mapping</li>
     *   <li><strong>created_at, updated_at, indexed_at</strong> - Date fields</li>
     * </ul>
     *
     * @return the configured TypeMapping for filesystem nodes
     */
    private TypeMapping buildFilesystemNodesMapping() {
        // Core fields for filesystem nodes to support simple querying
        TypeMapping.Builder b = new TypeMapping.Builder();
        b.properties("node_id", Property.of(p -> p.keyword(KeywordProperty.of(k -> k))));
        b.properties("drive", Property.of(p -> p.keyword(KeywordProperty.of(k -> k))));
        b.properties("name", Property.of(p -> p.text(TextProperty.of(t -> t))));
        b.properties("name_text", Property.of(p -> p.text(TextProperty.of(t -> t))));
        b.properties("path", Property.of(p -> p.keyword(KeywordProperty.of(k -> k))));
        b.properties("path_text", Property.of(p -> p.text(TextProperty.of(t -> t))));
        b.properties("node_type", Property.of(p -> p.keyword(KeywordProperty.of(k -> k))));
        b.properties("tags", Property.of(p -> p.object(o -> o.dynamic(DynamicMapping.True))));
        b.properties("created_at", Property.of(p -> p.date(d -> d)));
        b.properties("updated_at", Property.of(p -> p.date(d -> d)));
        b.properties("indexed_at", Property.of(p -> p.date(d -> d)));
        return b.build();
    }
    /**
     * Builds the type mapping for the filesystem-drives index.
     *
     * <p>This mapping includes:
     * <ul>
     *   <li><strong>name</strong> - Keyword (unique drive identifier)</li>
     *   <li><strong>description</strong> - Text (full-text search)</li>
     *   <li><strong>total_size</strong> - Long (numeric aggregations)</li>
     *   <li><strong>node_count</strong> - Long (numeric aggregations)</li>
     *   <li><strong>metadata</strong> - Object with dynamic mapping</li>
     *   <li><strong>created_at, last_accessed, indexed_at</strong> - Date fields</li>
     * </ul>
     *
     * @return the configured TypeMapping for filesystem drives
     */
    private TypeMapping buildFilesystemDrivesMapping() {
        TypeMapping.Builder b = new TypeMapping.Builder();
        b.properties("name", Property.of(p -> p.keyword(KeywordProperty.of(k -> k))));
        b.properties("description", Property.of(p -> p.text(TextProperty.of(t -> t))));
        b.properties("total_size", Property.of(p -> p.long_(l -> l)));
        b.properties("node_count", Property.of(p -> p.long_(l -> l)));
        b.properties("metadata", Property.of(p -> p.object(o -> o.dynamic(DynamicMapping.True))));
        b.properties("created_at", Property.of(p -> p.date(d -> d)));
        b.properties("last_accessed", Property.of(p -> p.date(d -> d)));
        b.properties("indexed_at", Property.of(p -> p.date(d -> d)));
        return b.build();
    }

    /**
     * Executes an OpenSearch operation with exponential backoff retry.
     *
     * <p>This method retries {@link IOException} failures up to 6 times with
     * exponential backoff starting at 250ms and increasing linearly (250ms, 500ms,
     * 750ms, 1000ms, 1250ms, 1500ms).
     *
     * <p>This strategy is effective for handling:
     * <ul>
     *   <li>OpenSearch service starting up</li>
     *   <li>Temporary network connectivity issues</li>
     *   <li>Connection pool exhaustion</li>
     * </ul>
     *
     * @param <T> the return type of the operation
     * @param description a human-readable description for logging
     * @param operation the operation to execute
     * @return the result of the operation if successful
     * @throws IOException if the operation fails after all retries
     */
    private <T> T executeWithRetry(String description, Callable<T> operation) throws IOException {
        int attempts = 0;
        while (true) {
            try {
                return operation.call();
            } catch (Exception e) {
                attempts++;
                if (!isRetryable(e) || attempts >= 6) {
                    if (e instanceof IOException ioException) {
                        throw ioException;
                    }
                    if (e instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    }
                    throw new IOException("Failed to " + description, e);
                }
                long delayMillis = Duration.ofMillis(250L * attempts).toMillis();
                LOG.warnf("Retrying %s due to %s (attempt %d, delay %d ms)", description, e.getMessage(), attempts, delayMillis);
                try {
                    TimeUnit.MILLISECONDS.sleep(delayMillis);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while retrying " + description, interruptedException);
                }
            }
        }
    }

    /**
     * Determines if an exception is retryable.
     *
     * <p>Currently, only {@link IOException} is considered retryable, as it
     * typically indicates transient network issues rather than permanent errors
     * like authentication failures or malformed requests.
     *
     * @param throwable the exception to check
     * @return true if the exception is an IOException, false otherwise
     */
    private boolean isRetryable(Throwable throwable) {
        return throwable instanceof IOException;
    }
}
