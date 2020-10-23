package academy.kafka.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

import java.io.IOException;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.kafka.common.errors.SerializationException;

public class AvroGenericDeserializer implements Deserializer<GenericRecord> {

  //  private Boolean isKey;
    private Schema schema;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    //    this.isKey = isKey;
        this.schema = (Schema) configs.get("schema");
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (schema == null) {
            throw new SerializationException("deserializing error: configuration expects that \"schema\" is set");
        }
        return readSpecificDataOneRecord(data, schema);
    }

    private GenericRecord readSpecificDataOneRecord(byte[] bytes, Schema schema) {
       GenericRecord result = null;
        ReflectDatumReader<GenericRecord> datumReader = new ReflectDatumReader<GenericRecord>(schema);
        SeekableByteArrayInput inputStream = new SeekableByteArrayInput(bytes);
        try {
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(inputStream, datumReader);

            Iterator<GenericRecord> it = dataFileReader.iterator();
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
