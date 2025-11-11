package ai.pipestream.schemamanager.opensearch;

import ai.pipestream.opensearch.v1.KnnMethodDefinition;
import ai.pipestream.opensearch.v1.VectorFieldDefinition;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.hc.client5.http.HttpHostConnectException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.GetMappingResponse;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.opensearch._types.mapping.KnnVectorProperty;
import org.opensearch.client.opensearch._types.mapping.NestedProperty;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TextProperty;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.json.JsonData;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link OpenSearchSchemaService} for managing index schemas and mappings.
 *
 * <p>This service provides robust schema management with automatic retry logic for
 * handling transient OpenSearch connectivity issues. All operations run on worker
 * threads to avoid blocking the event loop.
 *
 * <p><strong>Key Features:</strong>
 * <ul>
 *   <li>Automatic retry with exponential backoff (250ms to 3s, up to 6 attempts)</li>
 *   <li>Worker thread execution for blocking I/O operations</li>
 *   <li>KNN-enabled index creation with customizable vector field configuration</li>
 *   <li>Support for multiple distance metrics (L2, cosine similarity, inner product)</li>
 *   <li>HNSW algorithm configuration (M, ef_construction, ef_search parameters)</li>
 * </ul>
 *
 * <p><strong>Retry Strategy:</strong><br>
 * Operations retry on {@link IOException} and {@link HttpHostConnectException},
 * which typically indicate transient network issues or OpenSearch being temporarily unavailable.
 * The backoff starts at 250ms and doubles up to 3 seconds, with a maximum of 6 attempts.
 *
 * @author PipeStream.ai
 * @version 1.0
 * @see OpenSearchSchemaService
 */
@ApplicationScoped
public class OpenSearchSchemaServiceImpl implements OpenSearchSchemaService {

    /**
     * Synchronous OpenSearch client for index management operations.
     * Injected via CDI.
     */
    @Inject
    OpenSearchClient client;

    /**
     * {@inheritDoc}
     *
     * <p>This implementation:
     * <ol>
     *   <li>Checks if the index exists</li>
     *   <li>If it exists, retrieves the index mapping</li>
     *   <li>Searches for a property with the specified name that has nested type</li>
     * </ol>
     *
     * <p>The operation runs on a worker thread and retries up to 6 times with exponential
     * backoff on connectivity failures.
     */
    @Override
    public Uni<Boolean> nestedMappingExists(String indexName, String nestedFieldName) {
        return Uni.createFrom().item(() -> {
            try {
                boolean exists = client.indices().exists(new ExistsRequest.Builder().index(indexName).build()).value();
                if (!exists) {
                    return false;
                }

                var mapping = client.indices().getMapping(b -> b.index(indexName));
                return mappingContainsNestedField(mapping, nestedFieldName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onFailure(this::isRetryable)
        .retry()
        .withBackOff(Duration.ofMillis(250), Duration.ofSeconds(3))
        .atMost(6);
    }

    /**
     * Checks if the mapping response contains a nested field with the specified name.
     *
     * @param mapping the GetMappingResponse from OpenSearch
     * @param nestedFieldName the name of the nested field to look for
     * @return true if a nested field with that name exists in any of the index mappings
     */
    private boolean mappingContainsNestedField(GetMappingResponse mapping, String nestedFieldName) {
        return mapping.result().values().stream()
                .anyMatch(indexMapping -> {
                    var properties = indexMapping.mappings().properties();
                    return properties.containsKey(nestedFieldName) && 
                           properties.get(nestedFieldName).isNested();
                });
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation creates a comprehensive nested mapping structure:
     * <pre>
     * {
     *   "mappings": {
     *     "properties": {
     *       "&lt;nestedFieldName&gt;": {
     *         "type": "nested",
     *         "properties": {
     *           "vector": { "type": "knn_vector", "dimension": N, ... },
     *           "source_text": { "type": "text" },
     *           "context_text": { "type": "text" },
     *           "chunk_config_id": { "type": "keyword" },
     *           "embedding_id": { "type": "keyword" },
     *           "is_primary": { "type": "boolean" }
     *         }
     *       }
     *     }
     *   },
     *   "settings": {
     *     "index": {
     *       "knn": true
     *     }
     *   }
     * }
     * </pre>
     *
     * <p>The operation runs on a worker thread and retries up to 6 times with exponential
     * backoff on connectivity failures.
     */
    @Override
    public Uni<Boolean> createIndexWithNestedMapping(String indexName, String nestedFieldName, VectorFieldDefinition vectorFieldDefinition) {
        return Uni.createFrom().item(() -> {
            try {
                var settings = new IndexSettings.Builder().knn(true).build();

                var mapping = new TypeMapping.Builder()
                    .properties(nestedFieldName, Property.of(property -> property
                        .nested(NestedProperty.of(nested -> nested
                            .properties(Map.of(
                                "vector", Property.of(p -> p.knnVector(createKnnVectorProperty(vectorFieldDefinition))),
                                "source_text", Property.of(p -> p.text(TextProperty.of(t -> t))),
                                "context_text", Property.of(p -> p.text(TextProperty.of(t -> t))),
                                "chunk_config_id", Property.of(p -> p.keyword(k -> k)),
                                "embedding_id", Property.of(p -> p.keyword(k -> k)),
                                "is_primary", Property.of(p -> p.boolean_(b -> b))
                            ))
                        ))
                    ))
                    .build();

                var createRequest = new org.opensearch.client.opensearch.indices.CreateIndexRequest.Builder()
                    .index(indexName)
                    .settings(settings)
                    .mappings(mapping)
                    .build();

                var response = client.indices().create(createRequest);
                return response.acknowledged();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onFailure(this::isRetryable)
        .retry()
        .withBackOff(Duration.ofMillis(250), Duration.ofSeconds(3))
        .atMost(6);
    }
    
    /**
     * Creates a KNN vector property configuration from a vector field definition.
     *
     * <p>This method configures the knn_vector field with:
     * <ul>
     *   <li>Vector dimension</li>
     *   <li>Distance metric (space type: L2, cosine, inner product)</li>
     *   <li>Indexing engine (e.g., nmslib, faiss, lucene)</li>
     *   <li>HNSW algorithm parameters (M, ef_construction, ef_search)</li>
     * </ul>
     *
     * @param vectorDef the vector field definition from the protobuf request
     * @return a configured KnnVectorProperty for OpenSearch mapping
     */
    private KnnVectorProperty createKnnVectorProperty(VectorFieldDefinition vectorDef) {
        return KnnVectorProperty.of(knn -> {
            knn.dimension(vectorDef.getDimension());
            
            if (vectorDef.hasKnnMethod()) {
                var method = vectorDef.getKnnMethod();
                knn.method(methodDef -> {
                    methodDef.name(getMethodName(method.getSpaceType()));
                    methodDef.engine(method.getEngine().name().toLowerCase());
                    methodDef.spaceType(mapSpaceType(method.getSpaceType()));
                    
                    if (method.hasParameters()) {
                        var params = method.getParameters();
                        var paramsMap = new HashMap<String, JsonData>();
                        
                        if (params.hasM()) {
                            paramsMap.put("m", JsonData.of(params.getM().getValue()));
                        }
                        if (params.hasEfConstruction()) {
                            paramsMap.put("ef_construction", JsonData.of(params.getEfConstruction().getValue()));
                        }
                        if (params.hasEfSearch()) {
                            paramsMap.put("ef_search", JsonData.of(params.getEfSearch().getValue()));
                        }
                        if (!paramsMap.isEmpty()) {
                            methodDef.parameters(paramsMap);
                        }
                    }
                    
                    return methodDef;
                });
            }
            
            return knn;
        });
    }
    
    /**
     * Maps protobuf space type enum to OpenSearch space type string.
     *
     * <p>Supported mappings:
     * <ul>
     *   <li>L2 → "l2" (Euclidean distance)</li>
     *   <li>COSINESIMIL → "cosinesimil" (Cosine similarity)</li>
     *   <li>INNERPRODUCT → "innerproduct" (Dot product)</li>
     *   <li>UNRECOGNIZED → "cosinesimil" (default fallback)</li>
     * </ul>
     *
     * @param spaceType the protobuf space type enum
     * @return the OpenSearch space type string
     */
    private String mapSpaceType(KnnMethodDefinition.SpaceType spaceType) {
        return switch (spaceType) {
            case L2 -> "l2";
            case COSINESIMIL -> "cosinesimil";
            case INNERPRODUCT -> "innerproduct";
            case UNRECOGNIZED -> "cosinesimil";
        };
    }
    
    /**
     * Determines the KNN algorithm method name based on space type.
     *
     * <p>Currently, all space types use the HNSW (Hierarchical Navigable Small World)
     * algorithm, which provides a good balance of speed and accuracy for approximate
     * nearest neighbor search.
     *
     * @param spaceType the protobuf space type enum (currently unused)
     * @return "hnsw" for all space types
     */
    private String getMethodName(KnnMethodDefinition.SpaceType spaceType) {
        return switch (spaceType) {
            case L2 -> "hnsw";
            case COSINESIMIL -> "hnsw";
            case INNERPRODUCT -> "hnsw";
            case UNRECOGNIZED -> "hnsw";
        };
    }

    /**
     * Determines if an exception is retryable (transient network issue).
     *
     * <p>This method identifies exceptions that are likely to be resolved by retrying,
     * specifically {@link IOException} and {@link HttpHostConnectException}, which
     * typically indicate:
     * <ul>
     *   <li>OpenSearch service temporarily unavailable</li>
     *   <li>Network connectivity issues</li>
     *   <li>Connection timeouts</li>
     * </ul>
     *
     * <p>The method unwraps {@link RuntimeException} to check the root cause.
     *
     * @param throwable the exception to check
     * @return true if the exception is retryable, false otherwise
     */
    private boolean isRetryable(Throwable throwable) {
        Throwable root = throwable;
        if (throwable instanceof RuntimeException && throwable.getCause() != null) {
            root = throwable.getCause();
        }
        return root instanceof IOException || root instanceof HttpHostConnectException;
    }
}
