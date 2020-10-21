package academy.kafka.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.avro.Schema;

public class JSONSerializer implements Serializer<Object> {

    final static private ObjectMapper objectMapper = new ObjectMapper();

    private Boolean isKey;
    private Class clazz;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.clazz = (Class) configs.get("JSONClass");
    }

    @Override
    public byte[] serialize(String topic, Object object) {
        try {
            if (object == null) {
                return null;
            } else {
                String jsonString = objectMapper.writeValueAsString(object);
                return jsonString.getBytes();
            }
        } catch (Exception e) {
            throw new SerializationException("Error when serializing object of class " + clazz.getName());
        }
    }
}
