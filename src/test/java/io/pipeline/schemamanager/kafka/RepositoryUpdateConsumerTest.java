package io.pipeline.schemamanager.kafka;

import io.pipeline.opensearch.v1.MutinyOpenSearchManagerServiceGrpc;
import io.pipeline.repository.filesystem.Drive;
import io.pipeline.repository.filesystem.DriveUpdateNotification;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;

@QuarkusTest
@QuarkusTestResource(OpenSearchTestResource.class)
public class RepositoryUpdateConsumerTest {

    @Inject
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @Inject
    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.apicurio.registry.url")
    String apicurioRegistryUrl;

    @InjectMock
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub openSearchManagerService;

    private KafkaProducer<String, Object> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtobufKafkaSerializer.class.getName());
        props.put("apicurio.registry.url", apicurioRegistryUrl);
        props.put("apicurio.registry.auto-register", "true");
        props.put("apicurio.registry.artifact-id", "drive-updates-value");
        return new KafkaProducer<>(props);
    }

    @Test
    public void testConsumer_onDriveCreated_indexesDrive() {
        Drive drive = Drive.newBuilder().setName("test-drive-1").build();
        DriveUpdateNotification notification = DriveUpdateNotification.newBuilder()
                .setDrive(drive)
                .setUpdateType("CREATED")
                .build();

        try (KafkaProducer<String, Object> producer = createProducer()) {
            producer.send(new ProducerRecord<>("drive-updates", drive.getName(), notification));
        }

        Mockito.verify(openSearchManagerService, Mockito.timeout(5000).times(1))
                .indexDocument(any());
    }
}