package ai.pipestream.schemamanager.opensearch;

import ai.pipestream.opensearch.v1.VectorFieldDefinition;
import io.smallrye.mutiny.Uni;

/**
 * Service interface for managing OpenSearch index schemas and mappings.
 *
 * <p>This interface defines operations for checking and creating index mappings,
 * specifically focused on nested field structures used for storing vector embeddings.
 *
 * <p>All operations are reactive and return {@link Uni} types for non-blocking execution.
 * Implementations should handle OpenSearch connectivity issues gracefully with retries.
 *
 * <p><strong>Key Responsibilities:</strong>
 * <ul>
 *   <li>Verify existence of nested field mappings in indices</li>
 *   <li>Create indices with complex nested mappings for embeddings</li>
 *   <li>Configure KNN vector fields with appropriate dimensions and algorithms</li>
 * </ul>
 *
 * @author PipeStream.ai
 * @version 1.0
 * @see OpenSearchSchemaServiceImpl
 * @see VectorFieldDefinition
 */
public interface OpenSearchSchemaService {

    /**
     * Checks if a specific nested field mapping exists within a given index.
     *
     * <p>This method verifies both that the index exists and that it contains a
     * nested field with the specified name. This is useful for ensuring schema
     * compatibility before indexing documents with embeddings.
     *
     * <p>The operation is idempotent and safe to call multiple times.
     *
     * @param indexName the name of the index to check (e.g., "documents")
     * @param nestedFieldName the name of the nested field to verify (e.g., "embeddings_384")
     * @return a {@link Uni} that resolves to true if the nested mapping exists, false otherwise
     */
    Uni<Boolean> nestedMappingExists(String indexName, String nestedFieldName);

    /**
     * Creates an OpenSearch index with a nested mapping for storing vector embeddings.
     *
     * <p>This method creates a new index with KNN enabled and configures a nested field
     * structure suitable for storing embeddings with metadata. The nested structure includes:
     * <ul>
     *   <li><strong>vector</strong> - knn_vector field with specified dimensions and algorithm</li>
     *   <li><strong>source_text</strong> - the original text that was embedded</li>
     *   <li><strong>context_text</strong> - additional context for the embedding</li>
     *   <li><strong>chunk_config_id</strong> - identifier for chunking configuration</li>
     *   <li><strong>embedding_id</strong> - unique identifier for the embedding</li>
     *   <li><strong>is_primary</strong> - flag indicating if this is the primary embedding</li>
     * </ul>
     *
     * <p>The vector field is configured according to the provided {@link VectorFieldDefinition},
     * which specifies dimension, distance metric (L2, cosine, inner product), and algorithm
     * parameters (HNSW: M, ef_construction, ef_search).
     *
     * <p><strong>Note:</strong> If the index already exists, this operation will fail. Use
     * {@link #nestedMappingExists(String, String)} to check before creating.
     *
     * @param indexName the name of the index to create (e.g., "documents")
     * @param nestedFieldName the name of the nested field (e.g., "embeddings_384")
     * @param vectorFieldDefinition the configuration for the knn_vector field dimensions and algorithm
     * @return a {@link Uni} that resolves to true if creation succeeded, false otherwise
     */
    Uni<Boolean> createIndexWithNestedMapping(String indexName, String nestedFieldName, VectorFieldDefinition vectorFieldDefinition);

}
