package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.CreateEmbeddingModelConfigRequest;
import ai.pipestream.opensearch.v1.CreateIndexEmbeddingBindingRequest;
import ai.pipestream.opensearch.v1.MutinyEmbeddingConfigServiceGrpc;
import ai.pipestream.opensearch.v1.MutinyOpenSearchManagerServiceGrpc;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest;
import ai.pipestream.schemamanager.v1.KnnMethodDefinition;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import ai.pipestream.test.support.OpensearchContainerTestResource;
import ai.pipestream.test.support.OpensearchWireMockTestResource;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(OpensearchWireMockTestResource.class)
@QuarkusTestResource(OpensearchContainerTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaManagerServiceTest {

    @GrpcClient
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub openSearchManagerService;

    @GrpcClient
    MutinyEmbeddingConfigServiceGrpc.MutinyEmbeddingConfigServiceStub embeddingConfigClient;

    @Test
    void testEnsureNestedEmbeddingsFieldExists() {
        // Create a test request
        var vectorFieldDef = VectorFieldDefinition.newBuilder()
                .setDimension(384)
                .setKnnMethod(KnnMethodDefinition.newBuilder()
                        .setEngine(KnnMethodDefinition.KnnEngine.KNN_ENGINE_UNSPECIFIED)
                        .setSpaceType(KnnMethodDefinition.SpaceType.SPACE_TYPE_COSINESIMIL)
                        .build())
                .build();

        var request = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName("test-index-" + UUID.randomUUID())
                .setNestedFieldName("embeddings")
                .setVectorFieldDefinition(vectorFieldDef)
                .build();

        // Execute the request
        var response = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request)
                .await().indefinitely();

        // Verify response
        assertThat("Response should not be null", response, notNullValue());
        // First call should create the schema (schema_existed = false)
        // The schema is being created for the first time, so it shouldn't exist yet
        assertThat("Schema should not exist yet", response.getSchemaExisted(), is(false));
    }

    @Test
    void testEnsureNestedEmbeddingsFieldExistsIdempotent() {
        // Create a test request
        var vectorFieldDef = VectorFieldDefinition.newBuilder()
                .setDimension(768)
                .setKnnMethod(KnnMethodDefinition.newBuilder()
                        .setEngine(KnnMethodDefinition.KnnEngine.KNN_ENGINE_UNSPECIFIED)
                        .setSpaceType(KnnMethodDefinition.SpaceType.SPACE_TYPE_UNSPECIFIED)
                        .build())
                .build();

        String indexName = "test-index-idempotent-" + UUID.randomUUID();

        var request = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName(indexName)
                .setNestedFieldName("embeddings")
                .setVectorFieldDefinition(vectorFieldDef)
                .build();

        // Execute the request twice
        var response1 = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request)
                .await().indefinitely();
        var response2 = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request)
                .await().indefinitely();

        // Verify both responses are successful
        assertNotNull(response1);
        assertNotNull(response2);

        // Second call should find existing schema (from cache)
        assertTrue(response2.getSchemaExisted());
    }

    @Test
    void testEnsureNestedEmbeddingsFieldExists_resolvesDimensionsFromBinding() {
        String indexName = "test-index-binding-" + UUID.randomUUID();
        String fieldName = "embeddings_384";

        // Create embedding config and binding so dimensions can be resolved from DB
        var createConfigResp = embeddingConfigClient.createEmbeddingModelConfig(
                CreateEmbeddingModelConfigRequest.newBuilder()
                        .setName("binding-test-model-" + UUID.randomUUID())
                        .setModelIdentifier("test/model")
                        .setDimensions(384)
                        .build()
        ).await().indefinitely();
        String configId = createConfigResp.getConfig().getId();

        embeddingConfigClient.createIndexEmbeddingBinding(
                CreateIndexEmbeddingBindingRequest.newBuilder()
                        .setIndexName(indexName)
                        .setEmbeddingModelConfigId(configId)
                        .setFieldName(fieldName)
                        .build()
        ).await().indefinitely();

        // Call without vector_field_definition - should resolve from binding
        var request = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName(indexName)
                .setNestedFieldName(fieldName)
                .build();

        var response = openSearchManagerService.ensureNestedEmbeddingsFieldExists(request)
                .await().indefinitely();

        assertNotNull(response);
        assertThat("Schema should be created", response.getSchemaExisted(), is(false));
    }
}