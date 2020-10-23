package academy.kafka.serializers;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
public class AvroGenericSerializer implements Serializer<GenericRecord> {

 //   private Boolean isKey;
    private Schema schema;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
     //   this.isKey = isKey;
        this.schema = (Schema) configs.get("schema");
    }

    @Override
    public byte[] serialize(String topic, GenericRecord record) {
        if (record == null) {
            return null;
        }
        if (schema == null) {
            throw new SerializationException("deserializing error: configuration expects that \"schema\" is set");
        }
        try {
           return writeSpecificData(record, schema);
        } catch (IOException e) {
            throw new SerializationException("Error when serializing SpecificRecordBase to byte[] for schema " + schema.getName());
        }
    }

    private byte[] writeSpecificData(GenericRecord record, Schema schema) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new ReflectDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, outputStream);
        dataFileWriter.append(record);
        dataFileWriter.flush();
        dataFileWriter.close();
        return outputStream.toByteArray();
    }
}
