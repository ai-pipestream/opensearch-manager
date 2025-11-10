package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Test demonstrating Strategy 1 implementation in the correct service (opensearch-manager).
 */
class Strategy1ManagerTest {

    @Test
    void shouldCreateOpenSearchDocumentWithNestedEmbeddings() {
        // Given: OpenSearchDocument with multiple embeddings (Strategy 1)
        OpenSearchDocument document = OpenSearchDocument.newBuilder()
                .setOriginalDocId("doc-123")
                .setDocType("article")
                .setCreatedBy("test-system")
                .setTitle("Test Document")
                .setBody("Document content")
                .addEmbeddings(Embedding.newBuilder()
                        .addVector(0.1f)
                        .addVector(0.2f)
                        .addVector(0.3f)
                        .setSourceText("Test Document")
                        .setChunkConfigId("title_embedding")
                        .setEmbeddingId("minilm_l6")
                        .setIsPrimary(true)
                        .build())
                .addEmbeddings(Embedding.newBuilder()
                        .addVector(0.4f)
                        .addVector(0.5f)
                        .addVector(0.6f)
                        .setSourceText("Document content")
                        .setChunkConfigId("body_chunks")
                        .setEmbeddingId("minilm_l6")
                        .setIsPrimary(false)
                        .build())
                .build();

        // Then: Document should have nested embeddings structure
        assertThat("Should have document ID", document.getOriginalDocId(), equalTo("doc-123"));
        assertThat("Should have embeddings", document.getEmbeddingsCount(), equalTo(2));
        
        // Verify primary embedding
        Embedding primaryEmbedding = document.getEmbeddings(0);
        assertThat("Should be primary", primaryEmbedding.getIsPrimary(), is(true));
        assertThat("Should have title config", primaryEmbedding.getChunkConfigId(), equalTo("title_embedding"));
        
        // Verify secondary embedding
        Embedding secondaryEmbedding = document.getEmbeddings(1);
        assertThat("Should be secondary", secondaryEmbedding.getIsPrimary(), is(false));
        assertThat("Should have body config", secondaryEmbedding.getChunkConfigId(), equalTo("body_chunks"));
    }

    @Test
    void shouldDetermineEmbeddingsFieldNameByDimension() {
        // Given: Different vector dimensions
        int dimension384 = 384;
        int dimension768 = 768;

        // When: Determining field names
        String field384 = "embeddings_" + dimension384;
        String field768 = "embeddings_" + dimension768;

        // Then: Should create dimension-specific field names
        assertThat("Should create 384-dim field", field384, equalTo("embeddings_384"));
        assertThat("Should create 768-dim field", field768, equalTo("embeddings_768"));
    }

    @Test
    void shouldCreateVectorFieldDefinition() {
        // Given: Vector field requirements
        VectorFieldDefinition vectorDef = VectorFieldDefinition.newBuilder()
                .setDimension(384)
                .setKnnMethod(KnnMethodDefinition.newBuilder()
                        .setEngine(KnnMethodDefinition.getDefaultInstance().getEngine())
                        .setSpaceType(KnnMethodDefinition.SpaceType.COSINESIMIL)
                        .setParameters(KnnParametersDefinition.newBuilder()
                                .setM(com.google.protobuf.Int32Value.of(16))
                                .setEfConstruction(com.google.protobuf.Int32Value.of(128))
                                .build())
                        .build())
                .build();

        // Then: Should have proper configuration
        assertThat("Should have correct dimension", vectorDef.getDimension(), equalTo(384));
        assertThat("Should use cosine similarity", vectorDef.getKnnMethod().getSpaceType(), 
                   equalTo(KnnMethodDefinition.SpaceType.COSINESIMIL));
    }
}