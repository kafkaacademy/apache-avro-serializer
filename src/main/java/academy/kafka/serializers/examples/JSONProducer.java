package academy.kafka.serializers.examples;

import academy.kafka.serializers.JSONSerializer;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class JSONProducer {

   
    public static void main(final String[] args) {
        Properties props = new Properties();
        props.put("JSONClass", JSONPerson.class);
        props = ProducerConfig.addSerializerToConfig(props, new StringSerializer(), new JSONSerializer<JSONPerson>());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        final JSONPerson person = new JSONPerson("BSNBSN", "Pietje", "Puk", "IBANIBAN");

        final Producer<String, Object> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            final ProducerRecord<String, Object> rec = new ProducerRecord<String, Object>("json_persons",
                    Integer.toString(i), person);
            producer.send(rec);
            System.out.println("produced " + i + "  " + rec.toString());
        }

        producer.close();
    }

}
