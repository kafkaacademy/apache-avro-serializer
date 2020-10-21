package academy.kafka.serializers;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

public class AvroSerializer implements Serializer<SpecificRecordBase> {

    private Boolean isKey;
    private Schema schema;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.schema = (Schema) configs.get("schema");      
    }

    @Override
    public byte[] serialize(String topic, SpecificRecordBase record) {
        try {
            if (record == null) {
                return null;
            } else {
                return writeSpecificData(record, schema);
            }
        } catch (IOException e) {
            throw new SerializationException("Error when serializing SpecificRecordBase to byte[] for schema " + schema.getName());
        }
    }

    private  byte[] writeSpecificData(SpecificRecordBase record, Schema schema) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ReflectDatumWriter<SpecificRecordBase> datumWriter = new ReflectDatumWriter<SpecificRecordBase>(schema);
        DataFileWriter<SpecificRecordBase> dataFileWriter = new DataFileWriter<SpecificRecordBase>(datumWriter);
        dataFileWriter.create(schema, outputStream);
        dataFileWriter.append(record);
        dataFileWriter.flush();
        dataFileWriter.close();
        return outputStream.toByteArray();
    }
}
