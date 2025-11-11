package ai.pipestream.schemamanager.kafka;

import ai.pipestream.repository.filesystem.DriveUpdateNotification;
import ai.pipestream.repository.filesystem.RepositoryEvent;
import ai.pipestream.repository.filesystem.MutinyFilesystemServiceGrpc;
import ai.pipestream.repository.filesystem.GetNodeRequest;
import ai.pipestream.dynamic.grpc.client.GrpcClientProvider;
import ai.pipestream.repository.v1.ModuleUpdateNotification;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import ai.pipestream.repository.v1.ProcessRequestUpdateNotification;
import ai.pipestream.repository.v1.ProcessResponseUpdateNotification;
import ai.pipestream.config.v1.GraphUpdateNotification;
import ai.pipestream.schemamanager.opensearch.OpenSearchIndexingService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

/**
 * Kafka consumer for repository update notifications with OpenSearch indexing.
 *
 * <p>This service consumes update notifications from multiple Kafka topics and
 * maintains the OpenSearch indices in sync with repository state changes. It acts
 * as the primary integration point between the Kafka event stream and the OpenSearch
 * indexing layer.
 *
 * <p><strong>Consumed Kafka Topics:</strong>
 * <ul>
 *   <li><strong>drive-updates-in</strong> - Filesystem drive create/update/delete events</li>
 *   <li><strong>repository-document-events-in</strong> - Document change events (requires gRPC fetch)</li>
 *   <li><strong>module-updates-in</strong> - Module definition changes</li>
 *   <li><strong>pipedoc-updates-in</strong> - PipeDoc metadata updates</li>
 *   <li><strong>process-request-updates-in</strong> - Process request creation/updates</li>
 *   <li><strong>process-response-updates-in</strong> - Process response updates</li>
 *   <li><strong>graph-updates-in</strong> - Pipeline graph structure changes</li>
 * </ul>
 *
 * <p><strong>Update Type Handling:</strong><br>
 * Each notification includes an update type that determines the action:
 * <ul>
 *   <li><strong>CREATED/UPDATED</strong> - Index or re-index the entity</li>
 *   <li><strong>DELETED</strong> - Remove the entity from the index</li>
 * </ul>
 *
 * <p><strong>Error Handling:</strong><br>
 * All indexing errors are logged but do not prevent message acknowledgment, ensuring
 * Kafka consumer progress. Failed indexing operations can be retried through
 * re-publishing or manual re-indexing.
 *
 * <p><strong>Special Case - Document Events:</strong><br>
 * Document events contain minimal information (document ID and account ID) and require
 * a gRPC call to the repository service to fetch full metadata before indexing. The
 * payload is excluded to reduce memory and network overhead.
 *
 * @author PipeStream.ai
 * @version 1.0
 * @see OpenSearchIndexingService
 * @see GrpcClientProvider
 */
@ApplicationScoped
public class RepositoryUpdateConsumer {

    private static final Logger LOG = Logger.getLogger(RepositoryUpdateConsumer.class);

    /**
     * OpenSearch indexing service for performing index and delete operations.
     * Injected via CDI.
     */
    @Inject
    OpenSearchIndexingService indexingService;

    /**
     * Dynamic gRPC client provider for calling repository services.
     * Used to fetch full node metadata when processing document events.
     * Injected via CDI.
     */
    @Inject
    GrpcClientProvider grpcClientProvider;
    
    /**
     * Consumes drive update notifications from Kafka and indexes them in OpenSearch.
     *
     * <p>This method handles CREATED, UPDATED, and DELETED events for filesystem drives.
     * Drive updates include configuration changes for storage endpoints (S3, local, etc.).
     *
     * @param message the Kafka message containing the DriveUpdateNotification
     * @return a {@link Uni} that completes after indexing and message acknowledgment
     */
    @Incoming("drive-updates-in")
    public Uni<Void> consumeDriveUpdate(Message<DriveUpdateNotification> message) {
        DriveUpdateNotification notification = message.getPayload();
        LOG.infof("Received drive update: type=%s, drive=%s", 
                notification.getUpdateType(), notification.getDrive().getName());
        
        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexDrive(notification.getDrive()),
                () -> indexingService.deleteDrive(notification.getDrive().getName()),
                "drive " + notification.getDrive().getName()
        )
        .onItemOrFailure().transformToUni((result, error) -> {
            if (error != null) {
                return Uni.createFrom().failure(error);
            }
            return Uni.createFrom().completionStage(message.ack());
        });
    }
    
    /**
     * Consumes repository document events and indexes node metadata in OpenSearch.
     *
     * <p>This method handles document change events by:
     * <ol>
     *   <li>Receiving a lightweight event with document ID and account ID</li>
     *   <li>Making a gRPC call to the repository service to fetch full node metadata</li>
     *   <li>Indexing the node metadata (without payload) in OpenSearch</li>
     * </ol>
     *
     * <p>The metadata-only fetch (includePayload=false) reduces network overhead while
     * providing sufficient information for search and discovery.
     *
     * @param message the Kafka message containing the RepositoryEvent
     * @return a {@link Uni} that completes after fetching, indexing, and message acknowledgment
     */
    @Incoming("repository-document-events-in")
    public Uni<Void> consumeDocumentEvent(Message<RepositoryEvent> message) {
        RepositoryEvent event = message.getPayload();
        LOG.infof("*** OPENSEARCH-MANAGER RECEIVED REPOSITORY EVENT: documentId=%s, accountId=%s ***",
                event.getDocumentId(), event.getAccountId());

        // Use dynamic gRPC to call repository service and get node metadata (without payload)
        return grpcClientProvider.getClientForService(
                MutinyFilesystemServiceGrpc.MutinyFilesystemServiceStub.class,
                "repository-service")
            .flatMap(repoClient -> {
                GetNodeRequest getNodeRequest = GetNodeRequest.newBuilder()
                    .setDrive(event.getAccountId())
                    .setDocumentId(event.getDocumentId())
                    .setIncludePayload(false) // Metadata only for indexing
                    .build();

                return repoClient.getNode(getNodeRequest);
            })
            .flatMap(node -> {
                // Index the node metadata in OpenSearch
                return indexingService.indexNode(node, event.getAccountId());
            })
        .onItemOrFailure().transformToUni((result, error) -> {
            if (error != null) {
                return Uni.createFrom().failure(error);
            }
            return Uni.createFrom().completionStage(message.ack());
        });
    }
    
    /**
     * Consumes module update notifications and indexes them in OpenSearch.
     *
     * <p>This method handles CREATED, UPDATED, and DELETED events for processing modules.
     * Modules define the processing capabilities available in the pipeline system.
     *
     * @param message the Kafka message containing the ModuleUpdateNotification
     * @return a {@link Uni} that completes after indexing and message acknowledgment
     */
    @Incoming("module-updates-in")
    public Uni<Void> consumeModuleUpdate(Message<ModuleUpdateNotification> message) {
        ModuleUpdateNotification notification = message.getPayload();
        LOG.infof("Received module update: type=%s, module=%s", 
                notification.getUpdateType(), notification.getModule().getModuleId());
        
        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexModule(notification.getModule()),
                () -> indexingService.deleteModule(notification.getModule().getModuleId()),
                "module " + notification.getModule().getModuleId()
        )
        .onItemOrFailure().transformToUni((result, error) -> {
            if (error != null) {
                return Uni.createFrom().failure(error);
            }
            return Uni.createFrom().completionStage(message.ack());
        });
    }
    
    /**
     * Consumes PipeDoc update notifications and indexes them in OpenSearch.
     *
     * <p>This method handles CREATED, UPDATED, and DELETED events for PipeDoc documents,
     * which contain document metadata, tags, and potentially embeddings for semantic search.
     *
     * @param message the Kafka message containing the PipeDocUpdateNotification
     * @return a {@link Uni} that completes after indexing and message acknowledgment
     */
    @Incoming("pipedoc-updates-in")
    public Uni<Void> consumePipeDocUpdate(Message<PipeDocUpdateNotification> message) {
        PipeDocUpdateNotification notification = message.getPayload();
        LOG.infof("Received pipedoc update: type=%s, docId=%s", 
                notification.getUpdateType(), notification.getDocId());
        
        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexPipeDoc(notification),
                () -> indexingService.deletePipeDoc(notification.getStorageId()),
                "pipedoc storageId=" + notification.getStorageId()
        )
        .onItemOrFailure().transformToUni((result, error) -> {
            if (error != null) {
                return Uni.createFrom().failure(error);
            }
            return Uni.createFrom().completionStage(message.ack());
        });
    }
    
    /**
     * Consumes process request update notifications and indexes them in OpenSearch.
     *
     * <p>This method handles CREATED, UPDATED, and DELETED events for process requests,
     * which represent pipeline execution requests with input parameters.
     *
     * @param message the Kafka message containing the ProcessRequestUpdateNotification
     * @return a {@link Uni} that completes after indexing and message acknowledgment
     */
    @Incoming("process-request-updates-in")
    public Uni<Void> consumeProcessRequestUpdate(Message<ProcessRequestUpdateNotification> message) {
        ProcessRequestUpdateNotification notification = message.getPayload();
        LOG.infof("Received process request update: type=%s, requestId=%s", 
                notification.getUpdateType(), notification.getRequestId());
        
        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexProcessRequest(notification),
                () -> indexingService.deleteProcessRequest(notification.getRequestId()),
                "process request " + notification.getRequestId()
        )
        .onItemOrFailure().transformToUni((result, error) -> {
            if (error != null) {
                return Uni.createFrom().failure(error);
            }
            return Uni.createFrom().completionStage(message.ack());
        });
    }
    
    /**
     * Consumes process response update notifications and indexes them in OpenSearch.
     *
     * <p>This method handles CREATED, UPDATED, and DELETED events for process responses,
     * which contain the results and status of pipeline executions.
     *
     * @param message the Kafka message containing the ProcessResponseUpdateNotification
     * @return a {@link Uni} that completes after indexing and message acknowledgment
     */
    @Incoming("process-response-updates-in")
    public Uni<Void> consumeProcessResponseUpdate(Message<ProcessResponseUpdateNotification> message) {
        ProcessResponseUpdateNotification notification = message.getPayload();
        LOG.infof("Received process response update: type=%s, responseId=%s", 
                notification.getUpdateType(), notification.getResponseId());
        
        return processUpdate(
                notification.getUpdateType(),
                () -> indexingService.indexProcessResponse(notification),
                () -> indexingService.deleteProcessResponse(notification.getResponseId()),
                "process response " + notification.getResponseId()
        )
        .onItemOrFailure().transformToUni((result, error) -> {
            if (error != null) {
                return Uni.createFrom().failure(error);
            }
            return Uni.createFrom().completionStage(message.ack());
        });
    }
    
    /**
     * Consumes graph update notifications and indexes graph components in OpenSearch.
     *
     * <p>This method handles updates for pipeline graph structures, including:
     * <ul>
     *   <li><strong>Graph</strong> - Complete graph metadata with node/edge counts</li>
     *   <li><strong>GraphNode</strong> - Individual processing nodes in the graph</li>
     *   <li><strong>GraphEdge</strong> - Connections between nodes with routing logic</li>
     * </ul>
     *
     * <p>The notification payload indicates which component type to index by using
     * protobuf oneof fields (hasGraph(), hasNode(), hasEdge()).
     *
     * @param message the Kafka message containing the GraphUpdateNotification
     * @return a {@link Uni} that completes after indexing and message acknowledgment
     */
    @Incoming("graph-updates-in")
    public Uni<Void> consumeGraphUpdate(Message<GraphUpdateNotification> message) {
        GraphUpdateNotification notification = message.getPayload();
        LOG.infof("Received graph update: type=%s, clusterId=%s", 
                notification.getUpdateType(), notification.getClusterId());
        
        return Uni.createFrom().deferred(() -> {
            // Handle different graph update types
            if (notification.hasNode()) {
                return indexingService.indexGraphNode(notification.getNode(), notification.getClusterId());
            } else if (notification.hasEdge()) {
                return indexingService.indexGraphEdge(notification.getEdge(), notification.getClusterId());
            } else if (notification.hasGraph()) {
                return indexingService.indexGraph(notification.getGraph());
            }
            return Uni.createFrom().voidItem();
        })
        .onItemOrFailure().transformToUni((result, error) -> {
            if (error != null) {
                LOG.errorf(error, "Failed to process graph update for cluster %s", notification.getClusterId());
                return Uni.createFrom().failure(error);
            }
            return Uni.createFrom().completionStage(message.ack());
        });
    }
    
    /**
     * Processes an update notification by routing to the appropriate index or delete action.
     *
     * <p>This helper method implements the common pattern for handling CREATED, UPDATED,
     * and DELETED events across all entity types.
     *
     * <p><strong>Routing logic:</strong>
     * <ul>
     *   <li><strong>CREATED/UPDATED</strong> - Executes createOrUpdateAction (index operation)</li>
     *   <li><strong>DELETED</strong> - Executes deleteAction (delete operation)</li>
     *   <li><strong>Unknown</strong> - Logs warning and returns successfully (no-op)</li>
     * </ul>
     *
     * <p>All failures are logged with the entity description for troubleshooting.
     *
     * @param updateType the update type string ("CREATED", "UPDATED", "DELETED")
     * @param createOrUpdateAction supplier for the index operation
     * @param deleteAction supplier for the delete operation
     * @param entityDescription human-readable entity description for logging (e.g., "drive my-drive")
     * @return a {@link Uni} that completes after executing the appropriate action
     */
    private Uni<Void> processUpdate(String updateType,
                                    java.util.function.Supplier<Uni<Void>> createOrUpdateAction,
                                    java.util.function.Supplier<Uni<Void>> deleteAction,
                                    String entityDescription) {
        switch (updateType) {
            case "CREATED":
            case "UPDATED":
                return createOrUpdateAction.get()
                    .onFailure().invoke(e -> 
                        LOG.errorf(e, "Failed to index %s", entityDescription));
            case "DELETED":
                return deleteAction.get()
                    .onFailure().invoke(e -> 
                        LOG.errorf(e, "Failed to delete %s", entityDescription));
            default:
                LOG.warnf("Unknown update type: %s for %s", updateType, entityDescription);
                return Uni.createFrom().voidItem();
        }
    }
}