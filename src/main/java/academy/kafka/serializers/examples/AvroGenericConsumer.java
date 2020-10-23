package academy.kafka.serializers.examples;

import academy.kafka.serializers.AvroGenericDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public final class AvroGenericConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("schema", AvroPerson.getClassSchema());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props = ConsumerConfig.addDeserializerToConfig(props, new StringDeserializer(), new AvroGenericDeserializer());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro_person_group");

        Consumer<String, SpecificRecordBase> consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList("avro_generic_persons"));
            while (true) {
                final ConsumerRecords<String, SpecificRecordBase> consumerRecords = consumer.poll(Duration.ofMillis(100));

                consumerRecords.forEach(record -> {
                    GenericRecord person= (GenericRecord) record.value();
                    System.out.printf("Consumer Record:(%s, %s, %s, %s)\n", record.key(), person,
                            record.partition(), record.offset());
                });
                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        consumer.close();

    }
}
