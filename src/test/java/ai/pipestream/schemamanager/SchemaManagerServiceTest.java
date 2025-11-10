package ai.pipestream.schemamanager;

import ai.pipestream.opensearch.v1.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaManagerServiceTest {

    @GrpcClient
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub openSearchManagerService;

    @Test
    void testEnsureNestedEmbeddingsFieldExists() {
        // Create a test request
        var vectorFieldDef = VectorFieldDefinition.newBuilder()
                .setDimension(384)
                .setKnnMethod(KnnMethodDefinition.newBuilder()
                        .setEngine(KnnMethodDefinition.KnnEngine.LUCENE)
                        .setSpaceType(KnnMethodDefinition.SpaceType.COSINESIMIL)
                        .build())
                .build();

        var request = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName("test-index")
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
                        .setEngine(KnnMethodDefinition.KnnEngine.LUCENE)
                        .setSpaceType(KnnMethodDefinition.SpaceType.L2)
                        .build())
                .build();

        var request = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName("test-index-idempotent")
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
}