package academy.kafka.serializers;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JSONSerializer<T> implements Serializer<T> {

    final static private ObjectMapper objectMapper = new ObjectMapper();

  
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
     }

    @Override
    public byte[] serialize(String topic, T object) {
        try {
            if (object == null) {
                return null;
            } else {
                String jsonString = objectMapper.writeValueAsString(object);
                return jsonString.getBytes();
            }
        } catch (Exception e) {
            throw new SerializationException("Error when serializing object ");
        }
    }
}
