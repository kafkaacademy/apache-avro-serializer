package academy.kafka.serializers.examples;

import academy.kafka.serializers.AvroSerializer;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class AvroProducer{

    static private ObjectMapper objectMapper = new ObjectMapper();

    public static void main(final String[] args) {
        Properties props = new Properties();
        props.put("schema", AvroPerson.getClassSchema());     
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props = ProducerConfig.addSerializerToConfig(props, new StringSerializer(), new AvroSerializer());
      
        final AvroPerson person = new AvroPerson("BSNBSN", "Pietje", "Puk", "IBANIBAN");

        final Producer<String, AvroPerson> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            final ProducerRecord<String, AvroPerson> rec = new ProducerRecord<String, AvroPerson>("avro_persons",
                    Integer.toString(i), person);
            producer.send(rec);
            System.out.println("produced " + i + "  " + rec.toString());
        }

        producer.close();
    }

}
