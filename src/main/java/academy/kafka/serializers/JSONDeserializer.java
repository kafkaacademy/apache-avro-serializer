package academy.kafka.serializers;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
public class JSONDeserializer<T> implements Deserializer<T> {

    private static ObjectMapper objectMapper = new ObjectMapper();
   // private Boolean isKey;
    private Class<?> clazz;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
       // this.isKey = isKey;
        this.clazz = (Class<?>) configs.get("JSONClass");
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (clazz == null) {
            throw new SerializationException("deserializing error: configuration expects that JSONClass is set");
        }
        try {
            return (T) objectMapper.readValue(data, clazz);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to class " + clazz.getName());
        }

    }
}
