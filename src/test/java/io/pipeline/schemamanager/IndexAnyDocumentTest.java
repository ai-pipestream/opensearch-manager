package io.pipeline.schemamanager;

import io.pipeline.opensearch.v1.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IndexAnyDocumentTest {

    @GrpcClient
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub openSearchManagerService;

    @Test
    void testIndexAnyDocumentAsJson() {
        // Create a simple protobuf message to index
        var sourceMessage = StringValue.newBuilder()
            .setValue("Test content for indexing")
            .build();

        // Pack it into Any
        var anyDocument = Any.pack(sourceMessage);

        var request = IndexAnyDocumentRequest.newBuilder()
            .setIndexName("test-any-index")
            .setDocument(anyDocument)
            .setDocumentId("test-doc-123")
            .build(); // No field mappings - will be indexed as JSON

        // Execute the request
        var response = openSearchManagerService.indexAnyDocument(request)
            .await().indefinitely();

        // Verify response
        assertThat("Response should not be null", response, notNullValue());
        assertThat("Response should indicate success", response.getSuccess(), is(true));
        assertThat("Document ID should match the requested ID", response.getDocumentId(), equalTo("test-doc-123"));
        assertThat("Response message should indicate successful indexing", response.getMessage(), containsString("Any document indexed successfully"));
    }

    @Test
    void testIndexAnyDocumentWithoutMappings() {
        // Create a simple protobuf message
        var sourceMessage = StringValue.newBuilder()
            .setValue("Simple test content")
            .build();

        var anyDocument = Any.pack(sourceMessage);

        var request = IndexAnyDocumentRequest.newBuilder()
            .setIndexName("test-any-simple")
            .setDocument(anyDocument)
            .setDocumentId("simple-doc-456")
            .build(); // No field mappings

        // Execute the request
        var response = openSearchManagerService.indexAnyDocument(request)
            .await().indefinitely();

        // Verify response
        assertThat("Response should not be null", response, notNullValue());
        assertThat("Response should indicate success", response.getSuccess(), is(true));
        assertThat("Document ID should match the requested ID", response.getDocumentId(), equalTo("simple-doc-456"));
    }
}