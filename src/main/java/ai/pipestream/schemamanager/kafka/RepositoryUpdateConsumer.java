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
 * Consumes repository update notifications from Kafka and indexes them in OpenSearch
 */
@ApplicationScoped
public class RepositoryUpdateConsumer {
    
    private static final Logger LOG = Logger.getLogger(RepositoryUpdateConsumer.class);
    
    @Inject
    OpenSearchIndexingService indexingService;

    @Inject
    GrpcClientProvider grpcClientProvider;
    
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