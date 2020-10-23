package academy.kafka.serializers;

import academy.kafka.serializers.examples.AvroPerson;
import academy.kafka.serializers.examples.JSONPerson;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class JsonSerializerTests {

    final static String topic = "dummy";

    @Test
    public void jsonTest() {
        Map<String, Object> configs = new HashMap();
        configs.put("JSONClass", JSONPerson.class);

        JSONSerializer<JSONPerson> serializer = new JSONSerializer<JSONPerson>();
        serializer.configure(configs, false);
        JSONDeserializer<JSONPerson> deserializer = new JSONDeserializer<JSONPerson>();
        deserializer.configure(configs, false);
        final JSONPerson source = new JSONPerson("BSNBSN", "Pietje", "Puk", "IBANIBAN");

        final byte[] data = serializer.serialize(topic, source);
        final JSONPerson result = deserializer.deserialize(topic, data);
        assertEquals(source.toString(), result.toString());

    }

    @Test
    public void jsonNoConfigTest() {

        JSONSerializer<JSONPerson> serializer = new JSONSerializer<JSONPerson>();
        JSONDeserializer<JSONPerson> deserializer = new JSONDeserializer<JSONPerson>();
        final JSONPerson source = new JSONPerson("BSNBSN", "Pietje", "Puk", "IBANIBAN");

        Assertions.assertThrows(SerializationException.class, () -> {
            serializer.serialize(topic, source);

        });

    }

    @Test
    public void jsonNoConfig2Test() {
        Map<String, Object> configs = new HashMap();
        configs.put("JSONClass", JSONPerson.class);

        JSONSerializer<JSONPerson> serializer = new JSONSerializer<JSONPerson>();
        serializer.configure(configs, false);
        JSONDeserializer<JSONPerson> deserializer = new JSONDeserializer<JSONPerson>();
        final JSONPerson source = new JSONPerson("BSNBSN", "Pietje", "Puk", "IBANIBAN");
        final byte[] data = serializer.serialize(topic, source);

        Assertions.assertThrows(SerializationException.class, () -> {
            deserializer.deserialize(topic, data);

        });

    }
    

    @Test
    public void jsonWrongClassTest() {
        Map<String, Object> configs = new HashMap();
        configs.put("JSONClass", Assertions.class);//wrong class

        JSONSerializer<JSONPerson> serializer = new JSONSerializer<JSONPerson>();
        serializer.configure(configs, false);
         JSONDeserializer<JSONPerson> deserializer = new JSONDeserializer<JSONPerson>();
      deserializer.configure(configs, false);
     
        final JSONPerson source = new JSONPerson("BSNBSN", "Pietje", "Puk", "IBANIBAN");

        final byte[] data = serializer.serialize(topic, source);
     
        Assertions.assertThrows(SerializationException.class, () -> {
            deserializer.deserialize(topic, data);

        });
      
    }
}
