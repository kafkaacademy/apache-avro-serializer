package academy.kafka.serializers;

import academy.kafka.serializers.examples.AvroPerson;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class AvroSerializerTests {

    final static String topic = "dummy";

    @Test
    public void avroTest() throws IOException {
        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put("schema", AvroPerson.getClassSchema());
        AvroSerializer<AvroPerson> serializer = new AvroSerializer<AvroPerson>();
        serializer.configure(configs, false);
        AvroDeserializer<AvroPerson> deserializer = new AvroDeserializer<AvroPerson>();
        deserializer.configure(configs, false);

        final AvroPerson source = new AvroPerson("BSNBSN", "Pietje", "Puk", "IBANIBAN");

        final byte[] data = serializer.serialize(topic, source);
        final AvroPerson result = deserializer.deserialize(topic, data);
        assertEquals(source, result);

    }

    @Test
    public void avroNoSchemaConfiguredTest() throws IOException {
        Map<String, Object> configs = new HashMap<String, Object>();
        AvroSerializer<AvroPerson> serializer = new AvroSerializer<AvroPerson>();
        serializer.configure(configs, false);
        AvroDeserializer<AvroPerson> deserializer = new AvroDeserializer<AvroPerson>();
        deserializer.configure(configs, false);

        final AvroPerson source = new AvroPerson("BSNBSN", "Pietje", "Puk", "IBANIBAN");

        Assertions.assertThrows(SerializationException.class, () -> {
            serializer.serialize(topic, source);
        });

    }
    
     @Test
    public void avroNoSchemaConfigured2Test() throws IOException {
        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put("schema", AvroPerson.getClassSchema());
        AvroSerializer<AvroPerson> serializer = new AvroSerializer<AvroPerson>();
        serializer.configure(configs, false);
        AvroDeserializer<AvroPerson> deserializer = new AvroDeserializer<AvroPerson>();
     
        final AvroPerson source = new AvroPerson("BSNBSN", "Pietje", "Puk", "IBANIBAN");

        final byte[] data = serializer.serialize(topic, source);
        Assertions.assertThrows(SerializationException.class, () -> {
          deserializer.deserialize(topic, data);
        });
      
    }

}
