package org.rrajesh1979.iot;

import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.rrajesh1979.DeviceData;
import org.rrajesh1979.DeviceDataKey;

import java.util.Collections;
import java.util.Properties;

@Slf4j
public class DeviceDataProc {
    public static void main(String[] args) {
        log.info("Starting Device Data Proc");
        Properties streamsConfig = getConfig();

        KafkaStreams streams = new KafkaStreams(createTopology(), streamsConfig);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    public static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        final Serde<DeviceDataKey> deviceDataKeySerde = new SpecificAvroSerde<>();
        final boolean isKeySerde = true;
        deviceDataKeySerde.configure(
                Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081"),
                isKeySerde);

        KStream<DeviceDataKey, DeviceData> deviceData = builder.stream("device-data");
        KTable<String, Long> totalReadings =
                deviceData
                        .map((key, value) -> new KeyValue<>(String.valueOf(value.getDeviceId()), value.getTemp()))
                        .groupByKey()
                        .count(Materialized.as("Counts"));

//        totalReadings.toStream().to("device-data-counts",
//                Produced.with(deviceDataKeySerde, Serdes.Long()));

        totalReadings.toStream().to("device-data-counts",
                Produced.with(Serdes.String(), Serdes.Long())
        );

        return builder.build();
    }


    public static Properties getConfig() {
        Properties streamsConfig = new Properties();

        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "device-data-processor");
        streamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, "device-data-processor-client");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        streamsConfig.put("schema.registry.url", "http://127.0.0.1:8081");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

        return streamsConfig;
    }
}
