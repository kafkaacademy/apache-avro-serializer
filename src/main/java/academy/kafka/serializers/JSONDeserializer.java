
package academy.kafka.serializers;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class JSONDeserializer implements Deserializer<Object> {
    private static ObjectMapper objectMapper = new ObjectMapper();

        private Boolean isKey;
    private Class clazz;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.clazz = (Class) configs.get("JSONClass");
    }
  
    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null)
            return null;      
        try {
          return objectMapper.readValue(data, clazz);
         } 
         catch (IOException e) {
            throw new SerializationException("Error when deserializing byte[] to class "+clazz.getName());
        }

    }
}
