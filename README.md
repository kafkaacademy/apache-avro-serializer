# Apache Avro (de)serializer for Apache Kafka 
Avro (de)serializer simple and straight
Minumal code (~15 lines of code)
Maximal use of Apache Avro 

See examples how to use these serializers, we yust follow Apache Kafka standards!

For educational purposes only!

This is pure the serializer/deserializer for Apache Avro.
There is no schema registry!

## Why there is no schema registry?

1. This will be added separately still this year (2020), inclusing a professional version of there resializers.

2. But For fast straight, stable Apacha Kafka applications it can be without.

  
## How to add the serializers/deserializers

```java
 props.put("schema" , schema);
 props = ConsumerConfig.addDeserializerToConfig(props, new StringDeserializer(), new AvroDeserializer<AvroPerson>());
``` 
Other ways, like with reflection or jackon introspection can give problems.
addDeserializerToConfig is the way apache kafka built in, and better use it.

# PS Less is more!!
We have the experience that, without a schema registry, probably business critical applications can profit from it.
For many topics in bussiness critical Kafka applications, changes in the schema's should be limited.
Also it might be very handy to have special schema's for certain customer group's.
The topic has data both for the public and for special groups.
For example : employee topic has all , including salary and home address, what should not be public.
So create a big schema and keep this schema secret, and a smaller schema for public groups. Some security on top of this is still needed
but it can be a step in the right direction.
