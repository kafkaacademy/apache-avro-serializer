package academy.kafka.serializers;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JSONSerializer<T> implements Serializer<T> {

    final static private ObjectMapper objectMapper = new ObjectMapper();

    private Class clazz;
    private Boolean isKey;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.clazz = (Class) configs.get("JSONClass");
    }

    @Override
    public byte[] serialize(String topic, T object) {
        if (object == null) {
            return null;
        }
        if (clazz == null) {
            throw new SerializationException("deserializing error: configuration expects that JSONClass is set");
        }
        try {
            String jsonString = objectMapper.writeValueAsString(object);
           return jsonString.getBytes();
        } catch (Exception e) {
              throw new SerializationException("Error when serializing object ");
        }
    }
}
