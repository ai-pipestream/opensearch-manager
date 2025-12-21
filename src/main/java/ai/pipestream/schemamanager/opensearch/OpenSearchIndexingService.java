package ai.pipestream.schemamanager.opensearch;

import com.google.protobuf.util.JsonFormat;
import ai.pipestream.config.v1.*; // Includes all enums and messages
import ai.pipestream.repository.filesystem.v1.Drive;
import ai.pipestream.repository.filesystem.v1.Node;
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
 * Service for indexing repository entities into OpenSearch
 */
@ApplicationScoped
public class OpenSearchIndexingService {

    private static final Logger LOG = Logger.getLogger(OpenSearchIndexingService.class);

    @Inject
    OpenSearchAsyncClient openSearchClient;

    // ========== FILESYSTEM DRIVE OPERATIONS ==========

    /**
     * Index a filesystem drive with enriched metadata
     */
    public Uni<Void> indexDrive(Drive drive, java.util.UUID key) {
        LOG.infof("Indexing drive using Map approach: %s, key=%s", drive.getName(), key);

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
                    .id(key.toString())
                    .document(document)
                )
            ).replaceWithVoid()
            .onItem().invoke(() -> LOG.infof("Successfully indexed drive: %s, key=%s", drive.getName(), key))
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to index drive: %s, key=%s", drive.getName(), key));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Delete a filesystem drive from the index
     */
    public Uni<Void> deleteDrive(java.util.UUID key) {
        try {
            return Uni.createFrom().completionStage(
                openSearchClient.delete(r -> r
                    .index(Index.FILESYSTEM_DRIVES.getIndexName())
                    .id(key.toString())
                )
            ).replaceWithVoid()
            .onFailure().invoke(e -> LOG.errorf(e, "Failed to delete drive: key=%s", key));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    // ========== FILESYSTEM NODE OPERATIONS ==========

    /**
     * Index a filesystem node with enriched metadata including S3 info and path components
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
     * Delete a filesystem node from the index
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
     * Index a module definition
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
     * Delete a module from the index
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
     * Index a PipeDoc
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
     * Delete a PipeDoc from the index
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
     * Index a ProcessRequest
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
     * Delete a ProcessRequest from the index
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
     * Index a ProcessResponse
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
     * Delete a ProcessResponse from the index
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
     * Index a PipelineGraph
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
     * Index a GraphNode
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
     * Index a GraphEdge
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
     * Build path components array for efficient tree queries
     * Example: "/drive/documents/projects/2024" becomes:
     * ["/", "/drive", "/drive/documents", "/drive/documents/projects", "/drive/documents/projects/2024"]
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
     * Extract file extension from filename
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
     * Extract class name from protobuf type URL
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
     * Get MIME type category for faceting
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
     * Simple drive indexing using protobuf-to-JSON conversion
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
     * Simple node indexing using protobuf-to-JSON conversion
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
