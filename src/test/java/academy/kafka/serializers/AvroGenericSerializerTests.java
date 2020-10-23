package academy.kafka.serializers;

import academy.kafka.serializers.examples.AvroPerson;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class AvroGenericSerializerTests {

    final static String topic = "dummy";

    @Test
    public void avroTest() throws IOException {
        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put("schema", AvroPerson.getClassSchema());
        AvroGenericSerializer serializer = new AvroGenericSerializer();
        serializer.configure(configs, false);
        AvroGenericDeserializer deserializer = new AvroGenericDeserializer();
        deserializer.configure(configs, false);

        GenericRecord source = new GenericData.Record(AvroPerson.getClassSchema());
        source.put("bsn", "BSN");
        source.put("firstName", "Pietje");
        source.put("lastName", "Puk");
        source.put("bancAccount", "123");


        final byte[] data = serializer.serialize(topic, source);
        final GenericRecord result = deserializer.deserialize(topic, data);
        assertEquals(source.toString(), result.toString());

    }

}
