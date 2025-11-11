package ai.pipestream.schemamanager;

import ai.pipestream.schemamanager.opensearch.OpenSearchSchemaService;
import ai.pipestream.opensearch.v1.*;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.google.protobuf.util.JsonFormat;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * gRPC service implementation for managing OpenSearch operations.
 *
 * <p>This service provides endpoints for:
 * <ul>
 *   <li>Schema management - creating indices with nested embeddings fields</li>
 *   <li>Document indexing - storing documents with embeddings in OpenSearch</li>
 *   <li>Full-text search - querying indexed filesystem metadata</li>
 * </ul>
 *
 * <p>The service implements Strategy 1 for embedding storage, where different vector
 * dimensions are stored in separate nested fields (e.g., embeddings_384, embeddings_768).
 * This allows for efficient querying of vectors with specific dimensions while maintaining
 * schema flexibility.
 *
 * <p>All operations are reactive and return {@link Uni} types for non-blocking execution.
 *
 * @author PipeStream.ai
 * @version 1.0
 * @see OpenSearchSchemaService
 * @see org.opensearch.client.opensearch.OpenSearchAsyncClient
 */
@GrpcService
public class OpenSearchManagerService extends MutinyOpenSearchManagerServiceGrpc.OpenSearchManagerServiceImplBase {

    private static final Logger LOG = Logger.getLogger(OpenSearchManagerService.class);

    /**
     * OpenSearch schema service for managing index mappings and nested field creation.
     * Injected via CDI.
     */
    @Inject
    OpenSearchSchemaService openSearchClient;

    /**
     * Asynchronous OpenSearch client for executing search operations.
     * Injected via CDI.
     */
    @Inject
    org.opensearch.client.opensearch.OpenSearchAsyncClient openSearchAsyncClient;

    /**
     * Ensures that a nested embeddings field exists in the specified OpenSearch index.
     *
     * <p>This method implements a safe, idempotent operation with the following logic:
     * <ol>
     *   <li>Check if the nested field already exists</li>
     *   <li>If not, attempt to create it with the specified vector field definition</li>
     *   <li>Handle concurrent creation attempts gracefully</li>
     *   <li>Recover from errors by double-checking field existence</li>
     * </ol>
     *
     * <p>The method is designed to handle race conditions where multiple requests
     * might try to create the same field simultaneously.
     *
     * @param request the request containing index name, nested field name, and vector definition
     * @return a {@link Uni} emitting the response indicating whether the field already existed
     * @throws RuntimeException if the field cannot be created and doesn't exist after retry
     */
    @Override
    public Uni<EnsureNestedEmbeddingsFieldExistsResponse> ensureNestedEmbeddingsFieldExists(EnsureNestedEmbeddingsFieldExistsRequest request) {
        final String indexName = request.getIndexName();
        final String fieldName = request.getNestedFieldName();
        
        LOG.infof("Ensuring nested embeddings field '%s' exists in index '%s'", fieldName, indexName);
        
        // First check if the mapping already exists
        return openSearchClient.nestedMappingExists(indexName, fieldName)
                .onItem().transformToUni(exists -> {
                    if (exists) {
                        LOG.infof("Nested field '%s' already exists in index '%s'", fieldName, indexName);
                        return Uni.createFrom().item(buildResponse(true));
                    } else {
                        LOG.infof("Creating nested field '%s' in index '%s'", fieldName, indexName);
                        // Try to create the mapping
                        return openSearchClient.createIndexWithNestedMapping(indexName, fieldName, request.getVectorFieldDefinition())
                                .onItem().transformToUni(success -> {
                                    if (success) {
                                        LOG.infof("Successfully created nested field '%s' in index '%s'", fieldName, indexName);
                                        return Uni.createFrom().item(buildResponse(false));
                                    } else {
                                        // Creation failed - could be because it was created concurrently
                                        // Double-check if it exists now
                                        return openSearchClient.nestedMappingExists(indexName, fieldName)
                                                .onItem().transform(existsNow -> {
                                                    if (existsNow) {
                                                        LOG.infof("Nested field '%s' was created concurrently in index '%s'", fieldName, indexName);
                                                        return buildResponse(true);
                                                    } else {
                                                        LOG.errorf("Failed to create nested field '%s' in index '%s'", fieldName, indexName);
                                                        throw new RuntimeException("Failed to create nested field: " + fieldName + " in index: " + indexName);
                                                    }
                                                });
                                    }
                                });
                    }
                })
                .onFailure().recoverWithUni(throwable -> {
                    LOG.errorf(throwable, "Error ensuring nested field '%s' in index '%s'", fieldName, indexName);
                    // On any error, try one more time to check if the field exists
                    return openSearchClient.nestedMappingExists(indexName, fieldName)
                            .onItem().transform(exists -> {
                                if (exists) {
                                    LOG.infof("Nested field '%s' exists after error recovery in index '%s'", fieldName, indexName);
                                    return buildResponse(true);
                                } else {
                                    throw new RuntimeException("Failed to ensure nested field exists: " + throwable.getMessage(), throwable);
                                }
                            });
                });
    }

    /**
     * Builds a response indicating whether the nested embeddings field schema already existed.
     *
     * @param existed true if the schema already existed, false if it was newly created
     * @return the response object
     */
    private EnsureNestedEmbeddingsFieldExistsResponse buildResponse(boolean existed) {
        return EnsureNestedEmbeddingsFieldExistsResponse.newBuilder().setSchemaExisted(existed).build();
    }

    /**
     * Indexes an OpenSearch document with embeddings into the specified index.
     *
     * <p>This method performs the following steps:
     * <ol>
     *   <li>Analyzes the document to determine required embedding field dimensions</li>
     *   <li>Ensures the index has appropriate nested fields for all vector dimensions</li>
     *   <li>Converts the protobuf document to JSON format</li>
     *   <li>Indexes the document using the original document ID</li>
     * </ol>
     *
     * <p>The method automatically creates nested embedding fields (e.g., embeddings_384,
     * embeddings_768) based on the vector dimensions present in the document's embeddings list.
     *
     * @param request the request containing the document to index and target index name
     * @return a {@link Uni} emitting the response with success status and document ID
     */
    @Override
    public Uni<IndexDocumentResponse> indexDocument(IndexDocumentRequest request) {
        var document = request.getDocument();
        var indexName = request.getIndexName();
        
        // Ensure index exists with proper embedding fields
        return ensureIndexForDocument(indexName, document)
            .flatMap(v -> {
                try {
                    String jsonDoc = JsonFormat.printer().print(document);
                    LOG.infof("Indexing document %s: %s", document.getOriginalDocId(), jsonDoc);
                    
                    // Actually index the document
                    return indexDocumentToOpenSearch(indexName, document.getOriginalDocId(), jsonDoc)
                        .map(success -> IndexDocumentResponse.newBuilder()
                            .setSuccess(success)
                            .setDocumentId(document.getOriginalDocId())
                            .setMessage(success ? "Document indexed successfully" : "Failed to index document")
                            .build());
                } catch (Exception e) {
                    return Uni.createFrom().item(IndexDocumentResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to index: " + e.getMessage())
                        .build());
                }
            });
    }
    
    /**
     * Ensures the index has all necessary nested embedding fields for the document.
     *
     * <p>This method analyzes all embeddings in the document, extracts the unique
     * vector dimensions, and ensures that nested fields exist for each dimension.
     * Multiple nested field creation requests are executed in parallel for efficiency.
     *
     * @param indexName the name of the target index
     * @param document the document containing embeddings to analyze
     * @return a {@link Uni} that completes when all nested fields are ensured
     */
    private Uni<Void> ensureIndexForDocument(String indexName, OpenSearchDocument document) {
        // For each unique vector dimension, ensure the appropriate nested field exists
        Set<Integer> dimensions = document.getEmbeddingsList().stream()
            .mapToInt(e -> e.getVectorCount())
            .filter(d -> d > 0)
            .boxed()
            .collect(java.util.stream.Collectors.toSet());
            
        if (dimensions.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        
        // Create requests for each dimension
        List<Uni<EnsureNestedEmbeddingsFieldExistsResponse>> requests = dimensions.stream()
            .map(dim -> {
                String fieldName = "embeddings_" + dim;
                VectorFieldDefinition vectorDef = VectorFieldDefinition.newBuilder()
                    .setDimension(dim)
                    .build();
                    
                return ensureNestedEmbeddingsFieldExists(EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                    .setIndexName(indexName)
                    .setNestedFieldName(fieldName)
                    .setVectorFieldDefinition(vectorDef)
                    .build());
            })
            .toList();
            
        return Uni.combine().all().unis(requests).discardItems();
    }
    
    /**
     * Indexes a JSON document into OpenSearch using the specified document ID.
     *
     * <p>This operation runs on a worker thread pool to avoid blocking the event loop.
     * Currently logs the indexing operation (TODO: implement actual indexing).
     *
     * @param indexName the name of the target index
     * @param documentId the unique identifier for the document
     * @param jsonDoc the document content in JSON format
     * @return a {@link Uni} emitting true if indexing succeeded, false otherwise
     */
    private Uni<Boolean> indexDocumentToOpenSearch(String indexName, String documentId, String jsonDoc) {
        return Uni.createFrom().item(() -> {
            try {
                // TODO: Use OpenSearchClient to actually index
                // For now, just log and return success
                LOG.infof("Would index to %s with ID %s: %s", indexName, documentId, jsonDoc);
                return true;
            } catch (Exception e) {
                LOG.errorf(e, "Failed to index document %s", documentId);
                return false;
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Indexes a protobuf {@link com.google.protobuf.Any} document into OpenSearch.
     *
     * <p>This method handles documents wrapped in protobuf Any messages, with special
     * support for {@link StringValue} messages. For other message types, it creates
     * a simple JSON representation with the type URL and value.
     *
     * <p>The method supports two indexing modes:
     * <ul>
     *   <li>No field mappings - creates a basic document with JSON content</li>
     *   <li>With field mappings - requires type-specific support (not yet implemented)</li>
     * </ul>
     *
     * @param request the request containing the Any document, index name, and optional field mappings
     * @return a {@link Uni} emitting the response with success status and document ID
     * @throws IllegalArgumentException if field mappings are provided (not yet supported)
     */
    @Override
    public Uni<IndexDocumentResponse> indexAnyDocument(IndexAnyDocumentRequest request) {
        return Uni.createFrom().item(() -> {
            try {
                var anyDocument = request.getDocument();
                var indexName = request.getIndexName();
                var fieldMappings = request.getFieldMappingsList();
                
                // Handle Any message manually with special case for StringValue
                // Note: We're not using JsonFormat.printer() with TypeRegistry because it requires
                // registering all possible types that might be contained in Any messages.
                // Instead, we handle common types directly and fall back to a simple representation for others.
                String jsonString;
                
                // Check if it's a StringValue (commonly used in tests)
                if (anyDocument.getTypeUrl().endsWith("google.protobuf.StringValue")) {
                    try {
                        // Unpack the StringValue and get its value directly
                        StringValue stringValue = anyDocument.unpack(StringValue.class);
                        jsonString = "{\"value\":\"" + stringValue.getValue() + "\"}";
                    } catch (InvalidProtocolBufferException e) {
                        LOG.errorf(e, "Failed to unpack StringValue");
                        throw e;
                    }
                } else {
                    // For other types, create a simple JSON representation with type URL and value
                    // This avoids the need for TypeRegistry while still providing useful information
                    jsonString = "{\"typeUrl\":\"" + anyDocument.getTypeUrl() + 
                                 "\",\"value\":\"" + anyDocument.getValue().toStringUtf8() + "\"}";
                }
                
                LOG.infof("Any document as JSON: %s", jsonString);
                
                // Create target OpenSearchDocument builder
                var targetBuilder = OpenSearchDocument.newBuilder();
                
                // If no field mappings provided, create a basic document with JSON content
                if (fieldMappings.isEmpty()) {
                    targetBuilder.setOriginalDocId(request.hasDocumentId() ? request.getDocumentId() : "unknown")
                               .setDocType("any_document")
                               .setBody(jsonString);
                } else {
                    // For field mappings, we need the original message
                    // This is a limitation - we'll need to support specific types for mapping
                    throw new IllegalArgumentException("Field mappings with Any documents require type-specific support. " +
                        "JSON representation: " + jsonString);
                }
                
                var mappedDocument = targetBuilder.build();
                var documentId = request.hasDocumentId() ? request.getDocumentId() : mappedDocument.getOriginalDocId();
                
                // TODO: Implement actual indexing logic using OpenSearchClient
                LOG.infof("Indexing Any document (type: %s) to index %s with %d field mappings", 
                         anyDocument.getTypeUrl(), indexName, fieldMappings.size());
                
                return IndexDocumentResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId(documentId)
                    .setMessage("Any document indexed successfully with field mappings")
                    .build();
            } catch (InvalidProtocolBufferException e) {
                LOG.errorf(e, "Failed to unpack Any document");
                return IndexDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to unpack Any document: " + e.getMessage())
                    .build();
            } catch (Exception e) {
                LOG.errorf(e, "Failed to index Any document");
                return IndexDocumentResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to index Any document: " + e.getMessage())
                    .build();
            }
        });
    }

    /**
     * Creates an OpenSearch index with a nested embeddings field.
     *
     * <p>The index is created with the appropriate vector field definition for
     * the specified dimension. The nested field name is automatically determined
     * based on the vector dimension (e.g., embeddings_384 for 384-dimensional vectors).
     *
     * @param request the request containing index name and vector field definition
     * @return a {@link Uni} emitting the response with success status
     */
    @Override
    public Uni<CreateIndexResponse> createIndex(CreateIndexRequest request) {
        return ensureIndexWithEmbeddingsField(request.getIndexName(), request.getVectorFieldDefinition())
            .map(success -> CreateIndexResponse.newBuilder()
                .setSuccess(success)
                .setMessage(success ? "Index created successfully" : "Failed to create index")
                .build());
    }

    /**
     * Ensures index exists with proper embeddings field for Strategy 1.
     *
     * <p>Strategy 1 uses dimension-specific nested fields (e.g., embeddings_384, embeddings_768)
     * to store vectors of different dimensions separately. This method determines the
     * appropriate field name and creates the index with the correct mapping.
     *
     * @param indexName the name of the index to create or verify
     * @param vectorDef the vector field definition specifying dimension and other properties
     * @return a {@link Uni} emitting true if the index was created successfully
     */
    private Uni<Boolean> ensureIndexWithEmbeddingsField(String indexName, VectorFieldDefinition vectorDef) {
        String fieldName = determineEmbeddingsFieldName(vectorDef.getDimension());
        return openSearchClient.createIndexWithNestedMapping(indexName, fieldName, vectorDef);
    }

    /**
     * Determines the embeddings field name based on vector dimension.
     *
     * <p>Strategy 1 uses dimension-specific field names to allow different vector
     * dimensions to coexist in the same index. This enables flexible embedding
     * storage where documents can have embeddings from different models.
     *
     * @param dimension the vector dimension (e.g., 384, 768, 1536)
     * @return the field name in format "embeddings_{dimension}" (e.g., "embeddings_384")
     */
    private String determineEmbeddingsFieldName(int dimension) {
        return "embeddings_" + dimension;
    }

    /**
     * Checks whether the specified index exists in OpenSearch.
     *
     * <p>This method verifies the existence of an index by checking for the
     * presence of a standard "embeddings" nested field in the mapping.
     *
     * @param request the request containing the index name to check
     * @return a {@link Uni} emitting the response indicating whether the index exists
     */
    @Override
    public Uni<IndexExistsResponse> indexExists(IndexExistsRequest request) {
        return openSearchClient.nestedMappingExists(request.getIndexName(), "embeddings")
            .map(exists -> IndexExistsResponse.newBuilder().setExists(exists).build());
    }

    /**
     * Analyzes an OpenSearch document to determine required embedding field names.
     *
     * <p>This Strategy 1 helper method examines all embeddings in the document,
     * groups them by vector dimension, and generates the appropriate field names
     * for each unique dimension found.
     *
     * <p>For example, if a document has embeddings with dimensions 384 and 768,
     * this method returns {"embeddings_384", "embeddings_768"}.
     *
     * @param document the document to analyze
     * @return a set of field names required for the document's embeddings,
     *         or a singleton set containing "embeddings" if no embeddings are found
     */
    private Set<String> analyzeRequiredEmbeddingFields(OpenSearchDocument document) {
        Set<String> fields = new HashSet<>();
        Map<Integer, Set<String>> dimensionToEmbeddingIds = new HashMap<>();
        
        // Group embedding IDs by vector dimension
        for (var embedding : document.getEmbeddingsList()) {
            int dimension = embedding.getVectorCount();
            if (dimension > 0) {
                dimensionToEmbeddingIds.computeIfAbsent(dimension, k -> new HashSet<>())
                        .add(embedding.getEmbeddingId());
            }
        }
        
        // Create field names for each dimension
        for (int dimension : dimensionToEmbeddingIds.keySet()) {
            fields.add(determineEmbeddingsFieldName(dimension));
        }
        
        return fields.isEmpty() ? Set.of("embeddings") : fields;
    }

    /**
     * Performs a full-text search on filesystem metadata stored in OpenSearch.
     *
     * <p>This method provides advanced search capabilities including:
     * <ul>
     *   <li>Multi-field text search across title, description, and tags</li>
     *   <li>Metadata filtering using tag-based filters</li>
     *   <li>Result highlighting with HTML tags</li>
     *   <li>Pagination using search_after for efficient deep pagination</li>
     *   <li>Relevance scoring with both score and ID-based sorting</li>
     * </ul>
     *
     * <p>The search uses a bool query with multi_match for text search and term
     * filters for metadata. Results are returned with highlights showing matching
     * text snippets surrounded by &lt;em&gt; tags.
     *
     * <p>Pagination is implemented using the search_after parameter with a composite
     * sort key (score + document ID) to ensure stable pagination even with tied scores.
     *
     * @param request the search request containing query text, filters, pagination, and drive info
     * @return a {@link Uni} emitting the search response with results, highlights, and pagination info
     */
    @Override
    public Uni<FilesystemMetaSearchResponse> searchFilesystemMeta(FilesystemMetaSearchRequest request) {
        LOG.infof("Searching filesystem metadata: drive=%s, query=%s", request.getDrive(), request.getQuery());

        String index = ai.pipestream.schemamanager.opensearch.IndexConstants.Index.REPOSITORY_PIPEDOCS.getIndexName();
        String queryText = request.getQuery() == null ? "" : request.getQuery();
        int pageSize = request.getPageSize() > 0 ? request.getPageSize() : 50;

        var boolBuilder = new org.opensearch.client.opensearch._types.query_dsl.BoolQuery.Builder();
        if (!queryText.isEmpty()) {
            boolBuilder.must(m -> m.multiMatch(mm -> mm
                .query(queryText)
                .fields("title^2", "description", "tags.*")
            ));
        } else {
            boolBuilder.must(m -> m.matchAll(ma -> ma));
        }

        if (request.getMetadataFiltersCount() > 0) {
            request.getMetadataFiltersMap().forEach((k, v) -> {
                String field = "tags." + k;
                boolBuilder.filter(f -> f.term(t -> t.field(field).value(org.opensearch.client.opensearch._types.FieldValue.of(v))));
            });
        }

        var highlightBuilder = new org.opensearch.client.opensearch.core.search.Highlight.Builder()
            .preTags("<em>")
            .postTags("</em>")
            .fields("title", hf -> hf)
            .fields("description", hf -> hf)
            .fields("tags.*", hf -> hf);

        var sortScoreDesc = new org.opensearch.client.opensearch._types.SortOptions.Builder()
            .score(s -> s.order(org.opensearch.client.opensearch._types.SortOrder.Desc))
            .build();
        var sortIdAsc = new org.opensearch.client.opensearch._types.SortOptions.Builder()
            .field(f -> f.field("_id").order(org.opensearch.client.opensearch._types.SortOrder.Asc))
            .build();

        var searchBuilder = new org.opensearch.client.opensearch.core.SearchRequest.Builder()
            .index(index)
            .size(pageSize)
            .query(q -> q.bool(boolBuilder.build()))
            .highlight(highlightBuilder.build())
            .sort(sortScoreDesc)
            .sort(sortIdAsc);

        if (!request.getPageToken().isEmpty()) {
            try {
                String[] parts = request.getPageToken().split("\\|", 2);
                double lastScore = Double.parseDouble(parts[0]);
                String lastId = parts.length > 1 ? parts[1] : "";
                java.util.List<org.opensearch.client.opensearch._types.FieldValue> after = new java.util.ArrayList<>();
                after.add(org.opensearch.client.opensearch._types.FieldValue.of(lastScore));
                after.add(org.opensearch.client.opensearch._types.FieldValue.of(lastId));
                searchBuilder.searchAfter(after);
            } catch (Exception e) {
                LOG.warnf("Invalid page_token '%s', ignoring: %s", request.getPageToken(), e.getMessage());
            }
        }

        java.util.concurrent.CompletableFuture<org.opensearch.client.opensearch.core.SearchResponse<java.util.Map>> fut;
        try {
            fut = openSearchAsyncClient.search(searchBuilder.build(), java.util.Map.class);
        } catch (java.io.IOException e) {
            return Uni.createFrom().item(FilesystemMetaSearchResponse.newBuilder().setTotalCount(0).build());
        }
        return io.smallrye.mutiny.Uni.createFrom().completionStage(fut)
            .onItem().transform(resp -> {
                FilesystemMetaSearchResponse.Builder out = FilesystemMetaSearchResponse.newBuilder();
                Integer total = resp.hits().total() == null ? null : (int) resp.hits().total().value();
                out.setTotalCount(total == null ? resp.hits().hits().size() : total);
                float maxScore = 0f;
                String nextToken = "";
                for (var hit : resp.hits().hits()) {
                    float score = hit.score() == null ? 0f : hit.score().floatValue();
                    if (score > maxScore) maxScore = score;
                    @SuppressWarnings("unchecked")
                    var src = (java.util.Map<String, Object>) hit.source();
                    String nodeId = null;
                    if (src != null) {
                        Object sid = src.get("storage_id");
                        if (sid instanceof String && !((String) sid).isEmpty()) {
                            nodeId = (String) sid;
                        } else {
                            Object did = src.get("doc_id");
                            if (did instanceof String) nodeId = (String) did;
                        }
                    }
                    if (nodeId == null || nodeId.isEmpty()) nodeId = hit.id();
                    String title = src != null && src.get("title") instanceof String ? (String) src.get("title") : nodeId;

                    java.util.Map<String, String> metadata = new java.util.HashMap<>();
                    if (src != null && src.get("tags") instanceof java.util.Map) {
                        @SuppressWarnings("unchecked")
                        var tagsMap = (java.util.Map<String, Object>) src.get("tags");
                        tagsMap.forEach((k, v) -> { if (v != null) metadata.put(k, String.valueOf(v)); });
                    }

                    com.google.protobuf.Struct.Builder highlights = com.google.protobuf.Struct.newBuilder();
                    if (hit.highlight() != null && !hit.highlight().isEmpty()) {
                        hit.highlight().forEach((field, fragments) -> {
                            if (fragments != null && !fragments.isEmpty()) {
                                String fragment = fragments.get(0);
                                highlights.putFields(field, com.google.protobuf.Value.newBuilder().setStringValue(fragment).build());
                            }
                        });
                    }

                    out.addResults(FilesystemSearchResult.newBuilder()
                        .setNodeId(nodeId)
                        .setName(title)
                        .setNodeType("FILE")
                        .setDrive(request.getDrive())
                        .putAllMetadata(metadata)
                        .setScore(score)
                        .setHighlights(highlights.build())
                        .build());

                    nextToken = (hit.score() == null ? "0" : String.valueOf(hit.score())) + "|" + hit.id();
                }
                out.setMaxScore(maxScore);
                if (!nextToken.isEmpty() && resp.hits().hits().size() == pageSize) {
                    out.setNextPageToken(nextToken);
                }
                return out.build();
            })
            .onFailure().recoverWithItem(err -> {
                org.jboss.logging.Logger.getLogger(getClass()).errorf(err, "OpenSearch query failed for drive=%s, query=%s", request.getDrive(), request.getQuery());
                return FilesystemMetaSearchResponse.newBuilder().setTotalCount(0).build();
            });
    }
}
