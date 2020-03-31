package io.confluent.streams;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class StreamsDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamsDemoApplication.class, args);

        runStreams();
    }

    public static void runStreams() {
        Properties properties = getProperties();

        try {
            StreamsBuilder builder = new StreamsBuilder();

            KStream<String, String> resourceStream = builder.stream("topic_one");
            resourceStream.print(Printed.toSysOut());

            KStream<String, String> resultStream = resourceStream.mapValues(value ->
                    new Gson().fromJson(value, JsonObject.class).get("after").getAsJsonObject().toString());
            resultStream.print(Printed.toSysOut());

            Topology topology = builder.build();

            KafkaStreams streams = new KafkaStreams(topology, properties);

            streams.cleanUp();
            streams.start();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static Properties getProperties() {

        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app_id");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("schema.registry.url", "http://localhost:8081");

        return properties;
    }
}

/*

kafka-avro-console-producer --broker-list localhost:9092 --topic onlineevents --property schema.registry.url=http://localhost:8081 \
--property value.schema='{"type":"record","name":"online-events","namespace":"com.hsbc.avro","fields":[{"name":"source","type":"string"},{"name":"tokenDetails","type":{"type":"record","name":"tokenDetails","fields":[{"name":"tokenModel","type":"string"},{"name":"camLevel","type":"string"},{"name":"idvSessionId","type":"string"},{"name":"sessionCorrelationId","type":"string"}]}},{"name":"eventDetails","type":{"type":"record","name":"eventDetails","fields":[{"name":"eventCode","type":"string"},{"name":"custId","type":"string"},{"name":"userId","type":"string"},{"name":"eventTimestamp","type":"string"}]}}]}'

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic onlineevents --property schema.registry.url=http://localhost:8081 \
--property value.schema='{"type":"record","name":"onlineevents","namespace":"com.hsbc.avro","fields":[{"name":"source","type":"string"},{"name":"tokenDetails","type":{"type":"record","name":"tokenDetails","fields":[{"name":"tokenModel","type":"string"},{"name":"camLevel","type":"string"},{"name":"idvSessionId","type":"string"},{"name":"sessionCorrelationId","type":"string"}]}},{"name":"eventDetails","type":{"type":"record","name":"eventDetails","fields":[{"name":"eventCode","type":"string"},{"name":"custId","type":"string"},{"name":"userId","type":"string"},{"name":"eventTimestamp","type":"string"}]}}]}' --from-beginning

>>> STEP 1
CREATE STREAM onlineevents_str WITH (KAFKA_TOPIC='onlineevents', VALUE_FORMAT='AVRO');

>>> STEP 2
CREATE STREAM onlineevents_keystr AS SELECT *, EVENTDETAILS->USERID as USERID FROM onlineevents_str partition by USERID;

>>> STEP 3
CREATE TABLE user_cust_mapping WITH (KAFKA_TOPIC='onlineevents_keystr', VALUE_FORMAT='AVRO', KEY='USERID');

>>> STEP 4
CREATE STREAM onlineevents_joinstr AS SELECT *, EVENTDETAILS->USERID as USERID FROM onlineevents_str partition by USERID;

>>> STEP 5
SELECT * FROM ONLINEEVENTS_JOINSTR k INNER JOIN USER_CUST_MAPPING m ON m.USERID = k.USERID;

>>> STEP 6
???

>>> TESTING 1
{"source":"test ","tokenDetails":{"tokenModel":"debv","camLevel":"3","idvSessionId":"","sessionCorrelationId":"34555"},"eventDetails":{"eventCode":"test","custId":"1234567","userId":"ananthESP","eventTimestamp":"2019-09-26T12:00:11.406+00:00"}}

>>> TESTING 2
{"source":"test","tokenDetails":{"tokenModel":"debv","camLevel":"3","idvSessionId":"","sessionCorrelationId":"6789"},"eventDetails":{"eventCode":"test","custId":"","userId":"ananthESP","eventTimestamp":"2019-09-26T12:00:11.406+00:00"}}

*/
