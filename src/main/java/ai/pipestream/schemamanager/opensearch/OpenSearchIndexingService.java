package ai.pipestream.schemamanager.opensearch;

import com.google.protobuf.util.JsonFormat;
import ai.pipestream.config.v1.*; // Includes all enums and messages
import ai.pipestream.repository.filesystem.Drive;
import ai.pipestream.repository.filesystem.Node;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import ai.pipestream.repository.v1.ProcessRequestUpdateNotification;
import ai.pipestream.repository.v1.ProcessResponseUpdateNotification;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;

import java.util.*;

import static ai.pipestream.schemamanager.opensearch.IndexConstants.*;

/**
 * Core service for indexing and deleting repository entities in OpenSearch.
 *
 * <p>This service provides comprehensive indexing capabilities for all entity types
 * in the PipeStream repository system. It transforms protobuf messages into OpenSearch
 * documents with enriched metadata and maintains indices for efficient querying.
 *
 * <p><strong>Supported Entity Types:</strong>
 * <ul>
 *   <li><strong>Filesystem Drives</strong> - Storage endpoints (S3, local, etc.)</li>
 *   <li><strong>Filesystem Nodes</strong> - Files and folders with path hierarchy</li>
 *   <li><strong>Modules</strong> - Processing module definitions</li>
 *   <li><strong>PipeDocs</strong> - Document metadata with tags and embeddings</li>
 *   <li><strong>Process Requests</strong> - Pipeline execution requests</li>
 *   <li><strong>Process Responses</strong> - Pipeline execution results</li>
 *   <li><strong>Graphs</strong> - Pipeline graph structures</li>
 *   <li><strong>Graph Nodes</strong> - Individual nodes in pipeline graphs</li>
 *   <li><strong>Graph Edges</strong> - Connections between graph nodes</li>
 * </ul>
 *
 * <p><strong>Key Features:</strong>
 * <ul>
 *   <li>Reactive operations using {@link Uni} for non-blocking execution</li>
 *   <li>Automatic metadata enrichment (path components, MIME categories, etc.)</li>
 *   <li>Composite document IDs for hierarchical data (e.g., drive/nodeId)</li>
 *   <li>Timestamp tracking (created, updated, indexed)</li>
 *   <li>Tag and metadata support for all entity types</li>
 *   <li>Protobuf-to-JSON conversion for simple indexing</li>
 * </ul>
 *
 * <p>All index and delete operations return {@link Uni}{@code <Void>} and include
 * comprehensive error logging for troubleshooting.
 *
 * @author PipeStream.ai
 * @version 1.0
 * @see IndexConstants
 * @see org.opensearch.client.opensearch.OpenSearchAsyncClient
 */
@ApplicationScoped
public class OpenSearchIndexingService {

    private static final Logger LOG = Logger.getLogger(OpenSearchIndexingService.class);

    /**
     * Asynchronous OpenSearch client for executing index and delete operations.
     * Injected via CDI.
     */
    @Inject
    OpenSearchAsyncClient openSearchClient;

    // ========== FILESYSTEM DRIVE OPERATIONS ==========

    /**
     * Indexes a filesystem drive with enriched metadata into OpenSearch.
     *
     * <p>This method creates a comprehensive document containing drive metadata
     * including name, description, custom metadata, and timestamps. The document
     * is indexed using the drive name as the document ID.
     *
     * <p>The indexed document includes:
     * <ul>
     *   <li>Basic fields: name, description</li>
     *   <li>Custom metadata map</li>
     *   <li>Timestamps: created_at (from drive), indexed_at (current time)</li>
     * </ul>
     *
     * @param drive the Drive protobuf message to index
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     */
    public Uni<Void> indexDrive(Drive drive) {
        LOG.infof("Indexing drive using Map approach: %s", drive.getName());

        Map<String, Object> document = new HashMap<>();

        // Basic drive fields
        document.put("name", drive.getName());
        document.put("description", drive.getDescription());
        if (!drive.getMetadata().isEmpty()) {
            document.put("metadata", drive.getMetadata());
        }

        // Timestamps
        if (drive.hasCreatedAt()) {
            document.put("created_at", drive.getCreatedAt().getSeconds() * 1000);
        }
        document.put("indexed_at", System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.FILESYSTEM_DRIVES.getIndexName())
                    .id(drive.getName())
                    .document(document)
                )
            ).replaceWithVoid()
            .onItem().invoke(() -> LOG.infof("Successfully indexed drive: %s", drive.getName()))
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index drive: %s", drive.getName()));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Deletes a filesystem drive from the OpenSearch index.
     *
     * <p>This operation removes the drive document identified by the drive name
     * from the filesystem-drives index.
     *
     * @param driveName the name of the drive to delete (used as document ID)
     * @return a {@link Uni} that completes when deletion succeeds, or fails with an exception
     */
    public Uni<Void> deleteDrive(String driveName) {
        try {
            return Uni.createFrom().completionStage(
                openSearchClient.delete(r -> r
                    .index(Index.FILESYSTEM_DRIVES.getIndexName())
                    .id(driveName)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to delete drive: %s", driveName));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    // ========== FILESYSTEM NODE OPERATIONS ==========

    /**
     * Indexes a filesystem node with comprehensive enriched metadata.
     *
     * <p>This method creates a highly detailed document for files and folders, including:
     * <ul>
     *   <li><strong>Identity:</strong> node ID, name, drive, type (FILE/FOLDER)</li>
     *   <li><strong>Hierarchy:</strong> path, path components for tree queries, parent path/ID, depth</li>
     *   <li><strong>File metadata:</strong> size, MIME type, extension, category</li>
     *   <li><strong>S3 metadata:</strong> lastModified, contentType, size (extensible)</li>
     *   <li><strong>Payload info:</strong> has payload flag, type URL, class name, size</li>
     *   <li><strong>UI fields:</strong> icon SVG, service type</li>
     *   <li><strong>Custom data:</strong> metadata map</li>
     *   <li><strong>Timestamps:</strong> created_at, updated_at, indexed_at</li>
     * </ul>
     *
     * <p>The document ID is a composite key: {@code drive/nodeId} to ensure uniqueness
     * across drives. Path components enable efficient hierarchical queries (e.g., find all
     * descendants of a folder).
     *
     * <p><strong>Example path components:</strong><br>
     * Path "/drive/documents/2024" becomes: ["/", "/drive", "/drive/documents", "/drive/documents/2024"]
     *
     * @param node the Node protobuf message to index
     * @param drive the drive name this node belongs to
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     * @see #buildPathComponents(String)
     */
    public Uni<Void> indexNode(Node node, String drive) {
        Map<String, Object> document = new HashMap<>();

        // Basic node fields
        document.put(NodeFields.NODE_ID.getFieldName(), String.valueOf(node.getId()));
        document.put(CommonFields.NAME.getFieldName(), node.getName());
        document.put(NodeFields.NAME_TEXT.getFieldName(), node.getName()); // Text field for search
        document.put(NodeFields.DRIVE.getFieldName(), drive);
        document.put(NodeFields.NODE_TYPE.getFieldName(), node.getType().name());

        // Path fields
        String path = node.getPath();
        document.put(NodeFields.PATH.getFieldName(), path);
        document.put(NodeFields.PATH_TEXT.getFieldName(), path); // Text field for search

        // Build path components for efficient tree queries
        List<String> pathComponents = buildPathComponents(path);
        document.put(NodeFields.PATH_COMPONENTS.getFieldName(), pathComponents);
        document.put(NodeFields.PATH_DEPTH.getFieldName(), pathComponents.size());

        // Parent path (immediate parent)
        if (path.contains("/")) {
            String parentPath = path.substring(0, path.lastIndexOf("/"));
            if (parentPath.isEmpty()) parentPath = "/";
            document.put(NodeFields.PARENT_PATH.getFieldName(), parentPath);
        }

        // Parent ID
        if (node.getParentId() > 0) {
            document.put(NodeFields.PARENT_ID.getFieldName(), String.valueOf(node.getParentId()));
        }

        // Size fields (separate for different purposes)
        document.put(NodeFields.FILE_SIZE.getFieldName(), node.getSizeBytes());
        document.put(NodeFields.TOTAL_SIZE.getFieldName(), node.getSizeBytes()); // Can be aggregated for folders

        // File metadata
        if (!node.getContentType().isEmpty()) {
            document.put(NodeFields.MIME_TYPE.getFieldName(), node.getContentType());
            document.put(NodeFields.MIME_TYPE_TEXT.getFieldName(), node.getContentType());
            document.put(NodeFields.MIME_TYPE_CATEGORY.getFieldName(), getMimeCategory(node.getContentType()));
        }

        // Extract file extension
        String extension = extractFileExtension(node.getName());
        if (!extension.isEmpty()) {
            document.put(NodeFields.FILE_EXTENSION.getFieldName(), extension);
        }

        // Build S3 metadata object (placeholder - would be enriched from actual S3 API calls)
        Map<String, Object> s3Metadata = new HashMap<>();
        s3Metadata.put("lastModified", node.getUpdatedAt().getSeconds() * 1000);
        s3Metadata.put("contentType", node.getContentType());
        s3Metadata.put("size", node.getSizeBytes());
        // In production, add more S3 metadata from actual S3 API calls (etag, version, etc)
        document.put(NodeFields.S3_METADATA.getFieldName(), s3Metadata);

        // Payload metadata (without the actual payload)
        if (node.hasPayload()) {
            document.put(NodeFields.HAS_PAYLOAD.getFieldName(), true);
            String typeUrl = node.getPayload().getTypeUrl();
            document.put(NodeFields.PAYLOAD_TYPE_URL.getFieldName(), typeUrl);
            document.put(NodeFields.PAYLOAD_CLASS_NAME.getFieldName(), extractClassName(typeUrl));
            document.put(NodeFields.PAYLOAD_SIZE.getFieldName(), node.getPayload().getValue().size());
        } else {
            document.put(NodeFields.HAS_PAYLOAD.getFieldName(), false);
        }

        // UI/Display fields
        if (!node.getIconSvg().isEmpty()) {
            document.put(NodeFields.ICON_SVG.getFieldName(), node.getIconSvg());
        }
        node.getServiceType();
        if (!node.getServiceType().isEmpty()) {
            document.put(NodeFields.SERVICE_TYPE.getFieldName(), node.getServiceType());
        }

        // Custom metadata
        if (!node.getMetadata().isEmpty()) {
            document.put(CommonFields.METADATA.getFieldName(), node.getMetadata());
        }

        // Timestamps
        document.put(CommonFields.CREATED_AT.getFieldName(), node.getCreatedAt().getSeconds() * 1000);
        document.put(CommonFields.UPDATED_AT.getFieldName(), node.getUpdatedAt().getSeconds() * 1000);
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        // Create composite ID using drive and node ID
        String docId = drive + "/" + node.getId();

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.FILESYSTEM_NODES.getIndexName())
                    .id(docId)
                    .document(document)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index node: %s/%s", drive, node.getId()));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Deletes a filesystem node from the OpenSearch index.
     *
     * <p>This operation removes the node document using the composite ID
     * {@code drive/nodeId} from the filesystem-nodes index.
     *
     * @param nodeId the unique identifier of the node within the drive
     * @param drive the drive name the node belongs to
     * @return a {@link Uni} that completes when deletion succeeds, or fails with an exception
     */
    public Uni<Void> deleteNode(String nodeId, String drive) {
        String docId = drive + "/" + nodeId;
        try {
            return Uni.createFrom().completionStage(
                openSearchClient.delete(r -> r
                    .index(Index.FILESYSTEM_NODES.getIndexName())
                    .id(docId)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to delete node: %s", docId));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    // ========== MODULE OPERATIONS ==========

    /**
     * Indexes a processing module definition into OpenSearch.
     *
     * <p>This method creates a document containing module metadata including:
     * <ul>
     *   <li>Module ID and implementation name</li>
     *   <li>gRPC service name for RPC calls</li>
     *   <li>Visibility (PUBLIC, PRIVATE, etc.)</li>
     *   <li>Timestamps: created_at, modified_at, indexed_at</li>
     * </ul>
     *
     * @param module the ModuleDefinition protobuf message to index
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     */
    public Uni<Void> indexModule(ModuleDefinition module) {
        Map<String, Object> document = new HashMap<>();

        document.put(ModuleFields.MODULE_ID.getFieldName(), module.getModuleId());
        document.put(ModuleFields.IMPLEMENTATION_NAME.getFieldName(), module.getImplementationName());
        document.put(ModuleFields.GRPC_SERVICE_NAME.getFieldName(), module.getGrpcServiceName());

        if (module.getVisibility() != ModuleVisibility.MODULE_VISIBILITY_UNSPECIFIED) {
            document.put(ModuleFields.VISIBILITY.getFieldName(), module.getVisibility().name());
        }

        // Timestamps
        document.put(CommonFields.CREATED_AT.getFieldName(), module.getCreatedAt());
        document.put(CommonFields.MODIFIED_AT.getFieldName(), module.getModifiedAt());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.REPOSITORY_MODULES.getIndexName())
                    .id(module.getModuleId())
                    .document(document)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index module: %s", module.getModuleId()));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Deletes a module definition from the OpenSearch index.
     *
     * @param moduleId the unique identifier of the module to delete
     * @return a {@link Uni} that completes when deletion succeeds, or fails with an exception
     */
    public Uni<Void> deleteModule(String moduleId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchClient.delete(r -> r
                    .index(Index.REPOSITORY_MODULES.getIndexName())
                    .id(moduleId)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to delete module: %s", moduleId));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    // ========== PIPEDOC OPERATIONS ==========

    /**
     * Indexes a PipeDoc document into OpenSearch.
     *
     * <p>This method creates a document from a PipeDoc update notification containing:
     * <ul>
     *   <li>Storage ID and document ID</li>
     *   <li>Title, author, and description</li>
     *   <li>Tags as a nested map structure</li>
     *   <li>Timestamps: created_at, updated_at, indexed_at</li>
     * </ul>
     *
     * <p>The storage ID is used as the document ID for indexing.
     *
     * @param notification the PipeDocUpdateNotification containing document metadata
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     */
    public Uni<Void> indexPipeDoc(PipeDocUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();

        document.put(PipeDocFields.STORAGE_ID.getFieldName(), notification.getStorageId());
        document.put(PipeDocFields.DOC_ID.getFieldName(), notification.getDocId());
        document.put(CommonFields.DESCRIPTION.getFieldName(), notification.getDescription());

        // Title and author (with text duplicates for search)
        document.put(PipeDocFields.TITLE.getFieldName(), notification.getTitle());
        document.put(PipeDocFields.AUTHOR.getFieldName(), notification.getAuthor());

        // Tags as a map
        if (notification.hasTags()) {
            document.put(CommonFields.TAGS.getFieldName(), notification.getTags().getTagDataMap());
        }

        // Timestamps
        document.put(CommonFields.CREATED_AT.getFieldName(), notification.getCreatedAt().getSeconds() * 1000);
        document.put(CommonFields.UPDATED_AT.getFieldName(), notification.getUpdatedAt().getSeconds() * 1000);
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.REPOSITORY_PIPEDOCS.getIndexName())
                    .id(notification.getStorageId())
                    .document(document)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index pipedoc: %s", notification.getDocId()));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Deletes a PipeDoc document from the OpenSearch index.
     *
     * @param storageId the storage ID of the document to delete (used as document ID)
     * @return a {@link Uni} that completes when deletion succeeds, or fails with an exception
     */
    public Uni<Void> deletePipeDoc(String storageId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchClient.delete(r -> r
                    .index(Index.REPOSITORY_PIPEDOCS.getIndexName())
                    .id(storageId)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to delete pipedoc: storageId=%s", storageId));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    // ========== PROCESS REQUEST OPERATIONS ==========

    /**
     * Indexes a process request into OpenSearch.
     *
     * <p>This method creates a document from a process request update notification containing:
     * <ul>
     *   <li>Storage ID and request ID</li>
     *   <li>Name and description</li>
     *   <li>Module ID and processor ID (if specified)</li>
     *   <li>Tags as a nested map structure</li>
     *   <li>Timestamps: created_at, updated_at, indexed_at</li>
     * </ul>
     *
     * <p>The request ID is used as the document ID for indexing.
     *
     * @param notification the ProcessRequestUpdateNotification containing request metadata
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     */
    public Uni<Void> indexProcessRequest(ProcessRequestUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();

        document.put(ProcessFields.STORAGE_ID.getFieldName(), notification.getStorageId());
        document.put(ProcessFields.REQUEST_ID.getFieldName(), notification.getRequestId());
        document.put(CommonFields.NAME.getFieldName(), notification.getName());
        document.put(CommonFields.DESCRIPTION.getFieldName(), notification.getDescription());

        if (!notification.getModuleId().isEmpty()) {
            document.put(ProcessFields.MODULE_ID.getFieldName(), notification.getModuleId());
        }

        if (!notification.getProcessorId().isEmpty()) {
            document.put(ProcessFields.PROCESSOR_ID.getFieldName(), notification.getProcessorId());
        }

        if (notification.hasTags()) {
            document.put(CommonFields.TAGS.getFieldName(), notification.getTags().getTagDataMap());
        }

        // Timestamps
        document.put(CommonFields.CREATED_AT.getFieldName(), notification.getCreatedAt().getSeconds() * 1000);
        document.put(CommonFields.UPDATED_AT.getFieldName(), notification.getUpdatedAt().getSeconds() * 1000);
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName())
                    .id(notification.getRequestId())
                    .document(document)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index process request: %s", notification.getRequestId()));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Deletes a process request from the OpenSearch index.
     *
     * @param requestId the unique identifier of the request to delete
     * @return a {@link Uni} that completes when deletion succeeds, or fails with an exception
     */
    public Uni<Void> deleteProcessRequest(String requestId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchClient.delete(r -> r
                    .index(Index.REPOSITORY_PROCESS_REQUESTS.getIndexName())
                    .id(requestId)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to delete process request: %s", requestId));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    // ========== PROCESS RESPONSE OPERATIONS ==========

    /**
     * Indexes a process response into OpenSearch.
     *
     * <p>This method creates a document from a process response update notification containing:
     * <ul>
     *   <li>Storage ID and response ID</li>
     *   <li>Name and description</li>
     *   <li>Request ID (linking back to the originating request)</li>
     *   <li>Status (SUCCESS, FAILED, etc.)</li>
     *   <li>Tags as a nested map structure</li>
     *   <li>Timestamps: created_at, updated_at, indexed_at</li>
     * </ul>
     *
     * <p>The response ID is used as the document ID for indexing.
     *
     * @param notification the ProcessResponseUpdateNotification containing response metadata
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     */
    public Uni<Void> indexProcessResponse(ProcessResponseUpdateNotification notification) {
        Map<String, Object> document = new HashMap<>();

        document.put(ProcessFields.STORAGE_ID.getFieldName(), notification.getStorageId());
        document.put(ProcessFields.RESPONSE_ID.getFieldName(), notification.getResponseId());
        document.put(CommonFields.NAME.getFieldName(), notification.getName());
        document.put(CommonFields.DESCRIPTION.getFieldName(), notification.getDescription());

        if (!notification.getRequestId().isEmpty()) {
            document.put(ProcessFields.REQUEST_ID.getFieldName(), notification.getRequestId());
        }

        if (!notification.getStatus().isEmpty()) {
            document.put(ProcessFields.STATUS.getFieldName(), notification.getStatus());
        }

        if (notification.hasTags()) {
            document.put(CommonFields.TAGS.getFieldName(), notification.getTags().getTagDataMap());
        }

        // Timestamps
        document.put(CommonFields.CREATED_AT.getFieldName(), notification.getCreatedAt().getSeconds() * 1000);
        document.put(CommonFields.UPDATED_AT.getFieldName(), notification.getUpdatedAt().getSeconds() * 1000);
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName())
                    .id(notification.getResponseId())
                    .document(document)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index process response: %s", notification.getResponseId()));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Deletes a process response from the OpenSearch index.
     *
     * @param responseId the unique identifier of the response to delete
     * @return a {@link Uni} that completes when deletion succeeds, or fails with an exception
     */
    public Uni<Void> deleteProcessResponse(String responseId) {
        try {
            return Uni.createFrom().completionStage(
                openSearchClient.delete(r -> r
                    .index(Index.REPOSITORY_PROCESS_RESPONSES.getIndexName())
                    .id(responseId)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to delete process response: %s", responseId));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    // ========== GRAPH OPERATIONS ==========

    /**
     * Indexes a pipeline graph structure into OpenSearch.
     *
     * <p>This method creates a document representing a complete pipeline graph containing:
     * <ul>
     *   <li>Graph ID and cluster ID</li>
     *   <li>Name and description</li>
     *   <li>Array of node IDs in the graph</li>
     *   <li>Edge count</li>
     *   <li>Graph mode (EDIT, EXECUTE, etc.)</li>
     *   <li>Timestamps: created_at, modified_at, indexed_at</li>
     * </ul>
     *
     * <p>The graph ID is used as the document ID for indexing.
     *
     * @param graph the PipelineGraph protobuf message to index
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     * @see #indexGraphNode(GraphNode, String)
     * @see #indexGraphEdge(GraphEdge, String)
     */
    public Uni<Void> indexGraph(PipelineGraph graph) {
        Map<String, Object> document = new HashMap<>();

        document.put(GraphFields.GRAPH_ID.getFieldName(), graph.getGraphId());
        document.put(GraphFields.CLUSTER_ID.getFieldName(), graph.getClusterId());
        document.put(CommonFields.NAME.getFieldName(), graph.getName());
        document.put(CommonFields.DESCRIPTION.getFieldName(), graph.getDescription());

        // Node IDs as array
        if (graph.getNodeIdsCount() > 0) {
            document.put("node_ids", graph.getNodeIdsList());
        }

        // Edge count
        document.put("edge_count", graph.getEdgesCount());

        // Mode
        if (graph.getMode() != GraphMode.GRAPH_MODE_UNSPECIFIED) {
            document.put(GraphFields.MODE.getFieldName(), graph.getMode().name());
        }

        // Timestamps
        document.put(CommonFields.CREATED_AT.getFieldName(), graph.getCreatedAt());
        document.put(CommonFields.MODIFIED_AT.getFieldName(), graph.getModifiedAt());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.REPOSITORY_GRAPHS.getIndexName())
                    .id(graph.getGraphId())
                    .document(document)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index graph: %s", graph.getGraphId()));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Indexes an individual graph node into OpenSearch.
     *
     * <p>This method creates a document for a single node in a pipeline graph containing:
     * <ul>
     *   <li>Node ID and cluster ID</li>
     *   <li>Name and node type</li>
     *   <li>Module ID (the processing module this node uses)</li>
     *   <li>Visibility (PUBLIC, PRIVATE, etc.)</li>
     *   <li>Node mode (STREAMING, BATCH, etc.)</li>
     *   <li>Timestamps: created_at, modified_at, indexed_at</li>
     * </ul>
     *
     * <p>The document ID is a composite key: {@code clusterId/nodeId}.
     *
     * @param node the GraphNode protobuf message to index
     * @param clusterId the cluster this node belongs to
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     */
    public Uni<Void> indexGraphNode(GraphNode node, String clusterId) {
        Map<String, Object> document = new HashMap<>();

        document.put(GraphFields.NODE_ID.getFieldName(), node.getNodeId());
        document.put(GraphFields.CLUSTER_ID.getFieldName(), clusterId);
        document.put(CommonFields.NAME.getFieldName(), node.getName());
        document.put(GraphFields.NODE_TYPE.getFieldName(), node.getNodeType().name());
        document.put(GraphFields.MODULE_ID.getFieldName(), node.getModuleId());

        if (node.getVisibility() != ClusterVisibility.CLUSTER_VISIBILITY_UNSPECIFIED) {
            document.put(GraphFields.VISIBILITY.getFieldName(), node.getVisibility().name());
        }

        if (node.getMode() != NodeMode.NODE_MODE_UNSPECIFIED) {
            document.put(GraphFields.MODE.getFieldName(), node.getMode().name());
        }

        // Timestamps
        document.put(CommonFields.CREATED_AT.getFieldName(), node.getCreatedAt());
        document.put(CommonFields.MODIFIED_AT.getFieldName(), node.getModifiedAt());
        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        String docId = clusterId + "/" + node.getNodeId();

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.REPOSITORY_GRAPH_NODES.getIndexName())
                    .id(docId)
                    .document(document)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index graph node: %s", docId));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Indexes a graph edge (connection between nodes) into OpenSearch.
     *
     * <p>This method creates a document for an edge connecting two graph nodes containing:
     * <ul>
     *   <li>Edge ID, cluster ID</li>
     *   <li>From node ID and to node ID</li>
     *   <li>To cluster ID (for cross-cluster edges)</li>
     *   <li>Condition (conditional routing expression)</li>
     *   <li>Priority (for edge ordering)</li>
     *   <li>Is cross-cluster flag</li>
     *   <li>Timestamp: indexed_at</li>
     * </ul>
     *
     * <p>The document ID is a composite key: {@code clusterId/edgeId}.
     *
     * @param edge the GraphEdge protobuf message to index
     * @param clusterId the cluster this edge belongs to
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     */
    public Uni<Void> indexGraphEdge(GraphEdge edge, String clusterId) {
        Map<String, Object> document = new HashMap<>();

        document.put(GraphFields.EDGE_ID.getFieldName(), edge.getEdgeId());
        document.put(GraphFields.CLUSTER_ID.getFieldName(), clusterId);
        document.put(GraphFields.FROM_NODE_ID.getFieldName(), edge.getFromNodeId());
        document.put(GraphFields.TO_NODE_ID.getFieldName(), edge.getToNodeId());

        if (!edge.getToClusterId().isEmpty()) {
            document.put(GraphFields.TO_CLUSTER_ID.getFieldName(), edge.getToClusterId());
        }

        if (!edge.getCondition().isEmpty()) {
            document.put(GraphFields.CONDITION.getFieldName(), edge.getCondition());
        }

        document.put(GraphFields.PRIORITY.getFieldName(), edge.getPriority());
        document.put(GraphFields.IS_CROSS_CLUSTER.getFieldName(), edge.getIsCrossCluster());

        document.put(CommonFields.INDEXED_AT.getFieldName(), System.currentTimeMillis());

        String docId = clusterId + "/" + edge.getEdgeId();

        try {
            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index(Index.REPOSITORY_GRAPH_EDGES.getIndexName())
                    .id(docId)
                    .document(document)
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index graph edge: %s", docId));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    // ========== HELPER METHODS ==========

    /**
     * Builds a path components array for efficient hierarchical tree queries.
     *
     * <p>This method decomposes a file path into all its ancestor paths, enabling
     * efficient queries like "find all files under /drive/documents" by matching
     * against the path_components field.
     *
     * <p><strong>Example:</strong><br>
     * Input: {@code "/drive/documents/projects/2024"}<br>
     * Output: {@code ["/", "/drive", "/drive/documents", "/drive/documents/projects", "/drive/documents/projects/2024"]}
     *
     * <p>This approach allows OpenSearch to efficiently query all descendants of a folder
     * using a term query on the path_components field.
     *
     * @param path the full file path (e.g., "/drive/folder/file.txt")
     * @return a list of all ancestor paths including the given path, or {@code ["/"]} if path is null/empty
     */
    private List<String> buildPathComponents(String path) {
        if (path == null || path.isEmpty()) {
            return Collections.singletonList("/");
        }

        List<String> components = new ArrayList<>();
        components.add("/");

        if (!path.equals("/")) {
            String[] parts = path.split("/");
            StringBuilder builder = new StringBuilder();

            for (String part : parts) {
                if (!part.isEmpty()) {
                    if (!builder.isEmpty()) {
                        builder.append("/");
                    }
                    builder.append(part);
                    components.add("/" + builder.toString());
                }
            }
        }

        return components;
    }

    /**
     * Extracts the file extension from a filename.
     *
     * <p>The extension is converted to lowercase for consistency.
     *
     * @param filename the filename to extract from (e.g., "document.PDF")
     * @return the lowercase extension without the dot (e.g., "pdf"), or empty string if no extension
     */
    private String extractFileExtension(String filename) {
        if (filename == null || filename.isEmpty()) {
            return "";
        }

        int lastDot = filename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < filename.length() - 1) {
            return filename.substring(lastDot + 1).toLowerCase();
        }

        return "";
    }

    /**
     * Extracts the class name from a protobuf type URL.
     *
     * <p>Protobuf Any messages use type URLs like "type.googleapis.com/package.ClassName".
     * This method extracts just the class name portion for easier searching and filtering.
     *
     * @param typeUrl the protobuf type URL (e.g., "type.googleapis.com/ai.pipestream.Node")
     * @return the class name portion (e.g., "ai.pipestream.Node"), or the full URL if no slash found
     */
    private String extractClassName(String typeUrl) {
        if (typeUrl == null || typeUrl.isEmpty()) {
            return "";
        }

        int lastSlash = typeUrl.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < typeUrl.length() - 1) {
            return typeUrl.substring(lastSlash + 1);
        }

        return typeUrl;
    }

    /**
     * Determines the high-level category of a MIME type for faceting and filtering.
     *
     * <p>This method maps detailed MIME types to broad categories suitable for
     * user-facing filters and aggregations.
     *
     * <p><strong>Supported categories:</strong>
     * image, video, audio, text, pdf, document, spreadsheet, presentation,
     * archive, application, other, unknown
     *
     * @param mimeType the MIME type to categorize (e.g., "application/pdf")
     * @return the category name (e.g., "pdf"), or "unknown" if null/empty
     */
    private String getMimeCategory(String mimeType) {
        if (mimeType == null || mimeType.isEmpty()) {
            return "unknown";
        }

        if (mimeType.startsWith("image/")) return "image";
        if (mimeType.startsWith("video/")) return "video";
        if (mimeType.startsWith("audio/")) return "audio";
        if (mimeType.startsWith("text/")) return "text";
        if (mimeType.contains("pdf")) return "pdf";
        if (mimeType.contains("word") || mimeType.contains("document")) return "document";
        if (mimeType.contains("sheet") || mimeType.contains("excel")) return "spreadsheet";
        if (mimeType.contains("presentation") || mimeType.contains("powerpoint")) return "presentation";
        if (mimeType.contains("zip") || mimeType.contains("tar") || mimeType.contains("compressed")) return "archive";
        if (mimeType.startsWith("application/")) return "application";

        return "other";
    }

    // ========== SIMPLE JSON INDEXING FOR DEMO ==========

    /**
     * Indexes a drive using simple protobuf-to-JSON conversion (demo/test method).
     *
     * <p>This method provides a simplified indexing approach that directly converts
     * the protobuf message to JSON without field enrichment. Useful for testing
     * and prototyping.
     *
     * <p><strong>Note:</strong> This indexes to a separate "drives-simple" index and does
     * not include the metadata enrichment of {@link #indexDrive(Drive)}.
     *
     * @param drive the Drive protobuf message to index
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     * @see #indexDrive(Drive) for production indexing with enrichment
     */
    public Uni<Void> indexDriveSimple(Drive drive) {
        LOG.infof("Indexing drive with simple JSON conversion: %s", drive.getName());

        try {
            String jsonDocument = JsonFormat.printer().print(drive);

            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index("drives-simple")
                    .id(drive.getName())
                    .document(jsonDocument)
                )
            ).replaceWithVoid()
            .onItem().invoke(() -> LOG.infof("Successfully indexed drive (simple): %s", drive.getName()))
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index drive (simple): %s", drive.getName()));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Indexes a node using simple protobuf-to-JSON conversion (demo/test method).
     *
     * <p>This method provides a simplified indexing approach that directly converts
     * the protobuf message to JSON without field enrichment. Useful for testing
     * and prototyping.
     *
     * <p><strong>Note:</strong> This indexes to a separate "nodes-simple" index and does
     * not include the metadata enrichment of {@link #indexNode(Node, String)}.
     *
     * @param node the Node protobuf message to index
     * @param drive the drive name (currently unused in simple indexing)
     * @return a {@link Uni} that completes when indexing succeeds, or fails with an exception
     * @see #indexNode(Node, String) for production indexing with enrichment
     */
    public Uni<Void> indexNodeSimple(Node node, String drive) {
        LOG.infof("Indexing node with simple JSON conversion: %s", String.valueOf(node.getId()));

        try {
            String jsonDocument = JsonFormat.printer().print(node);

            return Uni.createFrom().completionStage(
                openSearchClient.index(r -> r
                    .index("nodes-simple")
                    .id(String.valueOf(node.getId()))
                    .document(jsonDocument)
                )
            ).replaceWithVoid()
            .onItem().invoke(() -> LOG.infof("Successfully indexed node (simple): %s", String.valueOf(node.getId())))
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index node (simple): %s", String.valueOf(node.getId())));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }
}
