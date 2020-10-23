package academy.kafka.serializers.examples;

import academy.kafka.serializers.AvroGenericSerializer;
import java.util.Properties;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class AvroGenericProducer {

    public static void main(final String[] args) {
        Properties props = new Properties();
        props.put("schema", AvroPerson.getClassSchema());
        props = ProducerConfig.addSerializerToConfig(props, new StringSerializer(), new AvroGenericSerializer());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        GenericRecord person = new GenericData.Record(AvroPerson.getClassSchema());
        person.put("bsn", "BSN");
        person.put("firstName", "Pietje");
        person.put("lastName", "Puk");
        person.put("bancAccount", "123");

    
        final Producer<String, GenericRecord> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            final ProducerRecord<String, GenericRecord> rec = new ProducerRecord<String, GenericRecord>("avro_generic_persons",
                    Integer.toString(i), person);
            producer.send(rec);
            System.out.println("produced " + i + "  " + rec.toString());
        }

        producer.close();
    }

}
