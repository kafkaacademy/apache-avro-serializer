package academy.kafka.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

import java.io.IOException;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;


public class AvroDeserializer implements Deserializer<SpecificRecordBase> {
    private Boolean isKey;
    private Schema schema;
  
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey=isKey;
        this.schema = (Schema) configs.get("schema");      
    }

    @Override
    public SpecificRecordBase deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return readSpecificDataOneRecord(data, schema);
    }

    private SpecificRecordBase readSpecificDataOneRecord(byte[] bytes, Schema schema) {
        SpecificRecordBase result = null;
        ReflectDatumReader<SpecificRecordBase> datumReader = new ReflectDatumReader<SpecificRecordBase>(schema);
        SeekableByteArrayInput inputStream = new SeekableByteArrayInput(bytes);
        try {
            DataFileReader<SpecificRecordBase> dataFileReader = new DataFileReader<SpecificRecordBase>(inputStream, datumReader);

            Iterator<SpecificRecordBase> it = dataFileReader.iterator();
            if (it.hasNext()) {
                result = it.next();
            }
            dataFileReader.close();
        } catch (IOException ex) {
              throw new SerializationException("Error when deserializing to  SpecificRecordBase for schema " + schema.getName());
       }
        return result;
    }
}
