package io.pipeline.schemamanager.opensearch;

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

import static io.pipeline.schemamanager.opensearch.IndexConstants.Index;

@ApplicationScoped
public class IndexInitializer {

    private static final Logger LOG = Logger.getLogger(IndexInitializer.class);

    @Inject
    OpenSearchClient client;

    public void onStart(@Observes StartupEvent ev) {
        try {
            ensureIndex(Index.REPOSITORY_PIPEDOCS.getIndexName(), buildPipeDocsMapping());
            ensureIndex(Index.FILESYSTEM_NODES.getIndexName(), buildFilesystemNodesMapping());
            ensureIndex(Index.FILESYSTEM_DRIVES.getIndexName(), buildFilesystemDrivesMapping());
        } catch (Exception e) {
            LOG.error("Failed to initialize OpenSearch indices", e);
        }
    }

    private void ensureIndex(String indexName, TypeMapping mapping) throws IOException {
        boolean exists = client.indices().exists(new ExistsRequest.Builder().index(indexName).build()).value();
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
        client.indices().create(req);
    }

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
}
