
Avro Serializer
- faster than others
- because using fully tha apache avro library, no extra libraries needed.

 props = ConsumerConfig.addDeserializerToConfig(props, new StringDeserializer(), new AvroDeserializer<AvroPerson>());
       
