package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.entity.ChunkerConfigEntity;
import ai.pipestream.schemamanager.entity.EmbeddingModelConfig;
import ai.pipestream.schemamanager.entity.VectorSetEntity;
import ai.pipestream.schemamanager.entity.VectorSetIndexBindingEntity;
import ai.pipestream.schemamanager.kafka.SemanticMetadataEventProducer;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

/**
 * Core business logic for VectorSet management.
 */
@ApplicationScoped
public class VectorSetServiceEngine {

    private static final Logger LOG = Logger.getLogger(VectorSetServiceEngine.class);
    private static final String DEFAULT_RESULT_SET_NAME = "default";

    @Inject
    SemanticMetadataEventProducer eventProducer;

    @WithTransaction
    public Uni<CreateVectorSetResponse> createVectorSet(CreateVectorSetRequest request) {
        return Panache.withTransaction(() -> {
            return ChunkerConfigEntity.<ChunkerConfigEntity>findById(request.getChunkerConfigId())
                    .onItem().transformToUni(cc -> {
                        if (cc == null) {
                            return Uni.createFrom().<VectorSetEntity>failure(Status.NOT_FOUND
                                    .withDescription("Chunker config not found: " + request.getChunkerConfigId())
                                    .asRuntimeException());
                        }
                        return EmbeddingModelConfig.<EmbeddingModelConfig>findById(request.getEmbeddingModelConfigId())
                                .onItem().transformToUni(emc -> {
                                    if (emc == null) {
                                        return Uni.createFrom().<VectorSetEntity>failure(Status.NOT_FOUND
                                                .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                                                .asRuntimeException());
                                    }
                                    String id = request.hasId() && !request.getId().isBlank()
                                            ? request.getId()
                                            : UUID.randomUUID().toString();
                                    VectorSetEntity entity = new VectorSetEntity();
                                    entity.id = id;
                                    entity.name = request.getName();
                                    entity.chunkerConfig = cc;
                                    entity.embeddingModelConfig = emc;
                                    entity.fieldName = request.getFieldName();
                                    entity.resultSetName = normalizeResultSetName(
                                            request.hasResultSetName() ? request.getResultSetName() : null);
                                    entity.sourceField = request.getSourceField();
                                    entity.vectorDimensions = emc.dimensions;
                                    entity.metadata = request.hasMetadata() ? structToJson(request.getMetadata()) : null;
                                    
                                    return entity.<VectorSetEntity>persist().onItem().transformToUni(saved -> {
                                        if (request.getIndexName() != null && !request.getIndexName().isBlank()) {
                                            VectorSetIndexBindingEntity binding = new VectorSetIndexBindingEntity();
                                            binding.id = UUID.randomUUID().toString();
                                            binding.vectorSet = saved;
                                            binding.indexName = request.getIndexName();
                                            binding.status = "ACTIVE";
                                            return binding.persist().replaceWith(saved);
                                        }
                                        return Uni.createFrom().item(saved);
                                    });
                                });
                    });
        })
                .onItem().transform(saved -> toVectorSetProto(saved, request.getIndexName()))
                .call(vs -> eventProducer.publishVectorSetCreated(vs))
                .map(vs -> CreateVectorSetResponse.newBuilder().setVectorSet(vs).build());
    }

    @WithSession
    public Uni<GetVectorSetResponse> getVectorSet(GetVectorSetRequest request) {
        Uni<VectorSetEntity> lookup = request.getByName()
                ? VectorSetEntity.findByName(request.getId())
                : VectorSetEntity.findById(request.getId());
        return lookup.onItem().transformToUni(e -> e != null
                        ? Uni.createFrom().item(GetVectorSetResponse.newBuilder()
                                .setVectorSet(toVectorSetProto((VectorSetEntity) e)).build())
                        : Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("VectorSet not found: " + request.getId())
                                .asRuntimeException()));
    }

    @WithTransaction
    public Uni<UpdateVectorSetResponse> updateVectorSet(UpdateVectorSetRequest request) {
        return Panache.withTransaction(() -> VectorSetEntity.<VectorSetEntity>findById(request.getId())
                .onItem().transformToUni(e -> {
                    if (e == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("VectorSet not found: " + request.getId())
                                .asRuntimeException());
                    }
                    VectorSet previous = toVectorSetProto(e);

                    if (request.hasName()) e.name = request.getName();
                    if (request.hasSourceField()) e.sourceField = request.getSourceField();
                    if (request.hasResultSetName()) e.resultSetName = normalizeResultSetName(request.getResultSetName());
                    if (request.hasMetadata()) e.metadata = structToJson(request.getMetadata());

                    Uni<VectorSetEntity> afterChunker;
                    if (request.hasChunkerConfigId()) {
                        afterChunker = ChunkerConfigEntity.<ChunkerConfigEntity>findById(request.getChunkerConfigId())
                                .onItem().transformToUni(cc -> {
                                    if (cc == null) {
                                        return Uni.createFrom().<VectorSetEntity>failure(Status.NOT_FOUND
                                                .withDescription("Chunker config not found: " + request.getChunkerConfigId())
                                                .asRuntimeException());
                                    }
                                    e.chunkerConfig = cc;
                                    return Uni.createFrom().item(e);
                                });
                    } else {
                        afterChunker = Uni.createFrom().item(e);
                    }

                    return afterChunker.onItem().transformToUni(entity -> {
                        if (request.hasEmbeddingModelConfigId()) {
                            return EmbeddingModelConfig.<EmbeddingModelConfig>findById(request.getEmbeddingModelConfigId())
                                    .onItem().transformToUni(emc -> {
                                        if (emc == null) {
                                            return Uni.createFrom().failure(Status.NOT_FOUND
                                                    .withDescription("Embedding model config not found: " + request.getEmbeddingModelConfigId())
                                                    .asRuntimeException());
                                        }
                                        entity.embeddingModelConfig = emc;
                                        entity.vectorDimensions = emc.dimensions;
                                        return entity.persist()
                                                .replaceWith(Uni.createFrom().item(new Object[]{previous, entity}));
                                    });
                        }
                        return entity.persist()
                                .replaceWith(Uni.createFrom().item(new Object[]{previous, entity}));
                    });
                }))
                .onItem().transformToUni(pair -> {
                    var prev = (VectorSet) ((Object[]) pair)[0];
                    var entity = (VectorSetEntity) ((Object[]) pair)[1];
                    var current = toVectorSetProto(entity);
                    return eventProducer.publishVectorSetUpdated(prev, current)
                            .replaceWith(UpdateVectorSetResponse.newBuilder().setVectorSet(current).build());
                });
    }

    @WithTransaction
    public Uni<DeleteVectorSetResponse> deleteVectorSet(DeleteVectorSetRequest request) {
        return Panache.withTransaction(() ->
                VectorSetEntity.<VectorSetEntity>findById(request.getId())
                        .onItem().transformToUni(e -> {
                            if (e == null) {
                                return Uni.createFrom().item(DeleteVectorSetResponse.newBuilder()
                                        .setSuccess(false)
                                        .setMessage("Not found: " + request.getId()).build());
                            }
                            return e.delete()
                                    .replaceWith(DeleteVectorSetResponse.newBuilder()
                                            .setSuccess(true)
                                            .setMessage("Deleted").build())
                                    .call(() -> eventProducer.publishVectorSetDeleted(request.getId()));
                        }));
    }

    @WithSession
    public Uni<ListVectorSetsResponse> listVectorSets(ListVectorSetsRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        int page = parsePageToken(request.getPageToken());

        if (request.hasIndexName()) {
            return VectorSetIndexBindingEntity.<VectorSetIndexBindingEntity>list("indexName", request.getIndexName())
                    .onItem().transform(bindings -> ListVectorSetsResponse.newBuilder()
                        .addAllVectorSets(bindings.stream().map(b -> toVectorSetProto(b.vectorSet, b.indexName)).toList())
                        .build());
        }

        Uni<List<VectorSetEntity>> query;
        if (request.hasChunkerConfigId()) {
            query = VectorSetEntity.findByChunkerConfigId(request.getChunkerConfigId());
        } else if (request.hasEmbeddingModelConfigId()) {
            query = VectorSetEntity.findByEmbeddingModelConfigId(request.getEmbeddingModelConfigId());
        } else {
            query = VectorSetEntity.listOrderedByCreatedDesc(page, pageSize);
        }

        return query.onItem().transform(entities -> ListVectorSetsResponse.newBuilder()
                        .addAllVectorSets(entities.stream().map(e -> toVectorSetProto(e, null)).toList())
                        .setNextPageToken(entities.size() == pageSize ? String.valueOf(page + 1) : "")
                        .build());
    }

    @WithSession
    public Uni<ResolveVectorSetResponse> resolveVectorSet(ResolveVectorSetRequest request) {
        String resultSetName = normalizeResultSetName(
                request.hasResultSetName() ? request.getResultSetName() : null);
        return VectorSetIndexBindingEntity.findBindingByDetails(request.getIndexName(), request.getFieldName(), resultSetName)
                        .onItem().transformToUni(binding -> {
                            if (binding != null) {
                                return Uni.createFrom().item(binding.vectorSet);
                            }
                            if (!DEFAULT_RESULT_SET_NAME.equals(resultSetName)) {
                                return VectorSetIndexBindingEntity.findBindingByDetails(
                                        request.getIndexName(), request.getFieldName(), DEFAULT_RESULT_SET_NAME)
                                        .onItem().transform(b -> b != null ? b.vectorSet : null);
                            }
                            return Uni.createFrom().item((VectorSetEntity) null);
                        })
                .onItem().transform(vs -> {
                    if (vs != null) {
                        return ResolveVectorSetResponse.newBuilder()
                                .setVectorSet(toVectorSetProto(vs, request.getIndexName()))
                                .setFound(true)
                                .build();
                    }
                    return ResolveVectorSetResponse.newBuilder()
                            .setFound(false)
                            .build();
                });
    }

    // --- Proto conversion ---

    private VectorSet toVectorSetProto(VectorSetEntity e) {
        return toVectorSetProto(e, null);
    }

    private VectorSet toVectorSetProto(VectorSetEntity e, String indexName) {
        var b = VectorSet.newBuilder()
                .setId(e.id)
                .setName(e.name)
                .setChunkerConfigId(e.chunkerConfig != null ? e.chunkerConfig.id : "")
                .setEmbeddingModelConfigId(e.embeddingModelConfig != null ? e.embeddingModelConfig.id : "")
                .setIndexName(indexName != null ? indexName : "")
                .setFieldName(e.fieldName)
                .setResultSetName(e.resultSetName)
                .setSourceField(e.sourceField);
        b.setVectorDimensions(e.vectorDimensions);
        if (e.createdAt != null) b.setCreatedAt(toTimestamp(e.createdAt));
        if (e.updatedAt != null) b.setUpdatedAt(toTimestamp(e.updatedAt));
        if (e.metadata != null && !e.metadata.isBlank()) {
            try {
                Struct.Builder sb = Struct.newBuilder();
                JsonFormat.parser().merge(e.metadata, sb);
                b.setMetadata(sb.build());
            } catch (InvalidProtocolBufferException ex) {
                LOG.warnf("Could not parse metadata for VectorSet %s: %s", e.id, ex.getMessage());
            }
        }
        return b.build();
    }

    private com.google.protobuf.Timestamp toTimestamp(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private String structToJson(Struct s) {
        if (s == null || s.getFieldsCount() == 0) return null;
        try {
            return JsonFormat.printer().print(s);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    private int parsePageToken(String token) {
        if (token == null || token.isBlank()) return 0;
        try {
            return Math.max(0, Integer.parseInt(token));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private String normalizeResultSetName(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_RESULT_SET_NAME;
        }
        return value;
    }
}
