package ai.pipestream.schemamanager.opensearch;

/**
 * Centralized constants for OpenSearch index names and field names.
 *
 * <p>This class provides type-safe enums for all OpenSearch indices and their
 * field names used throughout the application. Using these constants ensures
 * consistency across indexing, querying, and mapping operations.
 *
 * <p><strong>Benefits:</strong>
 * <ul>
 *   <li>Single source of truth for index and field names</li>
 *   <li>Compile-time safety (no typos in string literals)</li>
 *   <li>IDE autocomplete support</li>
 *   <li>Easy refactoring and renaming</li>
 * </ul>
 *
 * <p>The class is organized into multiple enums, each representing a different
 * scope of constants:
 * <ul>
 *   <li>{@link Index} - OpenSearch index names</li>
 *   <li>{@link CommonFields} - Fields used across all/most indices</li>
 *   <li>{@link DriveFields} - Filesystem drive-specific fields</li>
 *   <li>{@link NodeFields} - Filesystem node-specific fields</li>
 *   <li>{@link PipeDocFields} - PipeDoc document-specific fields</li>
 *   <li>{@link ProcessFields} - Process request/response fields</li>
 *   <li>{@link GraphFields} - Pipeline graph-specific fields</li>
 *   <li>{@link ModuleFields} - Module definition fields</li>
 * </ul>
 *
 * @author PipeStream.ai
 * @version 1.0
 */
public class IndexConstants {

    /**
     * OpenSearch index names for different entity types.
     *
     * <p>This enum defines all indices used by the application, organized by
     * functional area (filesystem, repository, graphs).
     */
    public enum Index {
        // Filesystem indices
        FILESYSTEM_DRIVES("filesystem-drives"),
        FILESYSTEM_NODES("filesystem-nodes"),
        
        // Repository service indices
        REPOSITORY_MODULES("repository-modules"),
        REPOSITORY_PIPEDOCS("repository-pipedocs"),
        REPOSITORY_PROCESS_REQUESTS("repository-process-requests"),
        REPOSITORY_PROCESS_RESPONSES("repository-process-responses"),
        REPOSITORY_GRAPHS("repository-graphs"),
        REPOSITORY_GRAPH_NODES("repository-graph-nodes"),
        REPOSITORY_GRAPH_EDGES("repository-graph-edges");
        
        private final String indexName;
        
        Index(String indexName) {
            this.indexName = indexName;
        }

        /**
         * Returns the actual OpenSearch index name.
         *
         * @return the index name string (e.g., "filesystem-drives")
         */
        public String getIndexName() {
            return indexName;
        }
    }
    
    /**
     * Common field names used across multiple indices.
     *
     * <p>These fields represent standard attributes that most entities share,
     * such as identifiers, timestamps, and metadata.
     */
    public enum CommonFields {
        ID("id"),
        INDEXED_AT("indexed_at"),
        CREATED_AT("created_at"),
        UPDATED_AT("updated_at"),
        MODIFIED_AT("modified_at"),
        NAME("name"),
        DESCRIPTION("description"),
        METADATA("metadata"),
        TAGS("tags");
        
        private final String fieldName;
        
        CommonFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the actual OpenSearch field name.
         *
         * @return the field name string (e.g., "created_at")
         */
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Field names specific to the filesystem-drives index.
     *
     * <p>These fields describe storage drive configurations including S3 settings,
     * encryption, versioning, and access control.
     */
    public enum DriveFields {
        DRIVE_NAME("drive_name"),
        DRIVE_NAME_TEXT("drive_name_text"),
        ENDPOINT("endpoint"),
        BUCKET("bucket"),
        ACCESS_KEY("access_key"),
        REGION("region"),
        TYPE("type"),
        STATUS("status"),
        TOTAL_OBJECTS("total_objects"),
        TOTAL_SIZE("total_size"),
        S3_METADATA("s3_metadata"),
        IS_PUBLIC("is_public"),
        ENCRYPTION_STATUS("encryption_status"),
        VERSIONING_ENABLED("versioning_enabled");
        
        private final String fieldName;
        
        DriveFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the actual OpenSearch field name.
         *
         * @return the field name string (e.g., "bucket")
         */
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Field names specific to the filesystem-nodes index.
     *
     * <p>These fields describe files and folders with comprehensive metadata including:
     * <ul>
     *   <li>Hierarchical path information (path, components, depth)</li>
     *   <li>Size metrics (file, payload, metadata, total)</li>
     *   <li>S3/storage details (bucket, key, etag, version)</li>
     *   <li>File metadata (MIME type, extension, category)</li>
     *   <li>Payload information (type, class, size)</li>
     *   <li>UI/display fields (icon, service type)</li>
     * </ul>
     */
    public enum NodeFields {
        NODE_ID("node_id"),
        DRIVE("drive"),
        PARENT_ID("parent_id"),
        NODE_TYPE("node_type"),
        PATH("path"),
        PATH_COMPONENTS("path_components"),
        PATH_DEPTH("path_depth"),
        PARENT_PATH("parent_path"),
        RELATIVE_PATH("relative_path"),
        
        // Size fields
        FILE_SIZE("file_size"),
        PAYLOAD_SIZE("payload_size"),
        METADATA_SIZE("metadata_size"),
        TOTAL_SIZE("total_size"),
        
        // S3/Storage fields
        BUCKET_NAME("bucket_name"),
        S3_KEY("s3_key"),
        S3_ENDPOINT("s3_endpoint"),
        S3_METADATA("s3_metadata"),
        ETAG("etag"),
        VERSION_ID("version_id"),
        STORAGE_CLASS("storage_class"),
        CONTENT_TYPE("content_type"),
        CHECKSUM("checksum"),
        
        // File metadata
        MIME_TYPE("mime_type"),
        MIME_TYPE_TEXT("mime_type_text"),
        FILE_EXTENSION("file_extension"),
        MIME_TYPE_CATEGORY("mime_type_category"),
        
        // Payload metadata
        HAS_PAYLOAD("has_payload"),
        PAYLOAD_TYPE_URL("payload_type_url"),
        PAYLOAD_CLASS_NAME("payload_class_name"),
        
        // UI/Display fields
        ICON_SVG("icon_svg"),
        SERVICE_TYPE("service_type"),
        
        // Search duplicates for text analysis
        NAME_TEXT("name_text"),
        PATH_TEXT("path_text");
        
        private final String fieldName;
        
        NodeFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the actual OpenSearch field name.
         *
         * @return the field name string (e.g., "path_components")
         */
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Field names specific to the repository-pipedocs index.
     *
     * <p>These fields describe PipeDoc documents which contain processed document
     * metadata, embeddings, and extracted entities.
     */
    public enum PipeDocFields {
        STORAGE_ID("storage_id"),
        DOC_ID("doc_id"),
        TITLE("title"),
        AUTHOR("author"),
        CONTENT_SUMMARY("content_summary"),
        ENTITY_COUNT("entity_count"),
        DOCUMENT_TYPE("document_type");
        
        private final String fieldName;
        
        PipeDocFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the actual OpenSearch field name.
         *
         * @return the field name string (e.g., "storage_id")
         */
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Field names for process request and response indices.
     *
     * <p>These fields describe pipeline execution requests and their corresponding
     * responses, linking them to modules, processors, and streams.
     */
    public enum ProcessFields {
        STORAGE_ID("storage_id"),
        REQUEST_ID("request_id"),
        RESPONSE_ID("response_id"),
        MODULE_ID("module_id"),
        PROCESSOR_ID("processor_id"),
        STATUS("status"),
        STREAM_ID("stream_id");
        
        private final String fieldName;
        
        ProcessFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the actual OpenSearch field name.
         *
         * @return the field name string (e.g., "request_id")
         */
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Field names for pipeline graph indices (graphs, nodes, edges).
     *
     * <p>These fields describe the structure and connections of pipeline graphs,
     * including graph metadata, node configurations, and edge routing logic.
     */
    public enum GraphFields {
        GRAPH_ID("graph_id"),
        CLUSTER_ID("cluster_id"),
        NODE_ID("node_id"),
        EDGE_ID("edge_id"),
        FROM_NODE_ID("from_node_id"),
        TO_NODE_ID("to_node_id"),
        TO_CLUSTER_ID("to_cluster_id"),
        NODE_TYPE("node_type"),
        MODULE_ID("module_id"),
        CONDITION("condition"),
        PRIORITY("priority"),
        IS_CROSS_CLUSTER("is_cross_cluster"),
        MODE("mode"),
        VISIBILITY("visibility");
        
        private final String fieldName;
        
        GraphFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the actual OpenSearch field name.
         *
         * @return the field name string (e.g., "graph_id")
         */
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Field names specific to the repository-modules index.
     *
     * <p>These fields describe processing module definitions including their
     * implementation details, gRPC interfaces, and configuration schemas.
     */
    public enum ModuleFields {
        MODULE_ID("module_id"),
        IMPLEMENTATION_NAME("implementation_name"),
        GRPC_SERVICE_NAME("grpc_service_name"),
        VISIBILITY("visibility"),
        CONFIG_SCHEMA("config_schema"),
        DEFAULT_CONFIG("default_config");
        
        private final String fieldName;
        
        ModuleFields(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Returns the actual OpenSearch field name.
         *
         * @return the field name string (e.g., "module_id")
         */
        public String getFieldName() {
            return fieldName;
        }
    }
}