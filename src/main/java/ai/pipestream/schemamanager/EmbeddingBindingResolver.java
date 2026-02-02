package ai.pipestream.schemamanager;

import ai.pipestream.schemamanager.entity.IndexEmbeddingBinding;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Resolves embedding dimensions from IndexEmbeddingBinding (index + field) lookup.
 * Used when EnsureNestedEmbeddingsFieldExists is called without explicit VectorFieldDefinition.
 */
@ApplicationScoped
public class EmbeddingBindingResolver {

    private static final Logger LOG = Logger.getLogger(EmbeddingBindingResolver.class);

    /**
     * Resolves VectorFieldDefinition for the given index and field by looking up
     * IndexEmbeddingBinding and its EmbeddingModelConfig.
     *
     * @return Uni of VectorFieldDefinition if binding found with valid dimensions, otherwise empty Uni
     */
    @WithSession
    public Uni<VectorFieldDefinition> resolve(String indexName, String fieldName) {
        return IndexEmbeddingBinding.findByIndexAndField(indexName, fieldName)
                .onItem().transformToUni(binding -> {
                    if (binding == null || binding.embeddingModelConfig == null) {
                        return Uni.createFrom().item((VectorFieldDefinition) null);
                    }
                    Integer dims = binding.embeddingModelConfig.dimensions;
                    if (dims == null || dims <= 0) {
                        LOG.warnf("Embedding model config %s has no dimensions, cannot resolve for %s/%s",
                                binding.embeddingModelConfig.id, indexName, fieldName);
                        return Uni.createFrom().item((VectorFieldDefinition) null);
                    }
                    return Uni.createFrom().item(
                            VectorFieldDefinition.newBuilder()
                                    .setDimension(dims)
                                    .build());
                });
    }
}
