package org.rrajesh1979.iot;

import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.rrajesh1979.DeviceData;
import org.rrajesh1979.DeviceDataKey;
import org.rrajesh1979.DeviceDataStatistics;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class DeviceDataProc {
    public static void main(String[] args) {
        DeviceDataProc proc = new DeviceDataProc();
        proc.start();
    }

    private void start() {
        log.info("Starting Device Data Proc");
        Properties streamsConfig = getConfig();

        KafkaStreams streams = new KafkaStreams(createTopology(), streamsConfig);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        final Serde<DeviceDataKey> deviceDataKeySerde = new SpecificAvroSerde<>();
        deviceDataKeySerde.configure(
                Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081"), true);

        final Serde<DeviceData> deviceDataSerde = new SpecificAvroSerde<>();
        deviceDataSerde.configure(
                Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081"), false);

        final Serde<DeviceDataStatistics> deviceDataStatisticsSerde = new SpecificAvroSerde<>();
        deviceDataStatisticsSerde.configure(
                Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081"), false);

        KStream<String, DeviceData> deviceData =
                builder.stream("device-data", Consumed.with(deviceDataKeySerde, deviceDataSerde))
                        .selectKey((deviceDataKey, deviceDataValue) -> "Device-" + deviceDataKey.getDeviceId());

        KTable<String, DeviceDataStatistics> deviceDataStatistics =
                deviceData.groupByKey()
                        .aggregate(this::emptyStats, this::dataAggregator,
                                Materialized.<String, DeviceDataStatistics, KeyValueStore<Bytes, byte[]>>as("device-data-stats")
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(deviceDataStatisticsSerde));

        deviceDataStatistics.toStream()
                .to("device-data-stats", Produced.with(Serdes.String(), deviceDataStatisticsSerde));

        return builder.build();
    }

    private DeviceDataStatistics emptyStats() {
        return DeviceDataStatistics.newBuilder()
                .setLastAggTime(Instant.EPOCH)
                .build();
    }

    private DeviceDataStatistics dataAggregator(String deviceId, DeviceData newDeviceData, DeviceDataStatistics currentStats) {
        DeviceDataStatistics.Builder deviceDataStatisticsBuilder = DeviceDataStatistics.newBuilder(currentStats);

        deviceDataStatisticsBuilder.setDeviceId(newDeviceData.getDeviceId());

        double newTemp = newDeviceData.getTemp();
        double newHumidity = newDeviceData.getHumidity();

        double currentAvgTemp = currentStats.getAvgTemp();
        double currentAvgHumidity = currentStats.getAvgHumidity();
        double currentMaxTemp = currentStats.getMaxTemp();
        double currentMinTemp = currentStats.getMinTemp();
        double currentMaxHumidity = currentStats.getMaxHumidity();
        double currentMinHumidity = currentStats.getMinHumidity();

        double newAvgTemp = (currentAvgTemp * currentStats.getNumReadings() + newTemp) / (currentStats.getNumReadings() + 1);
        double newAvgHumidity = (currentAvgHumidity * currentStats.getNumReadings() + newHumidity) / (currentStats.getNumReadings() + 1);

        if (newTemp > currentMaxTemp) {
            deviceDataStatisticsBuilder.setMaxTemp(newTemp);
        }

        if (currentMinTemp == 0 || newTemp < currentMinTemp) {
            deviceDataStatisticsBuilder.setMinTemp(newTemp);
        }

        if (newHumidity > currentMaxHumidity) {
            deviceDataStatisticsBuilder.setMaxHumidity(newHumidity);
        }

        if (currentMinHumidity == 0 || newHumidity < currentMinHumidity) {
            deviceDataStatisticsBuilder.setMinHumidity(newHumidity);
        }

        deviceDataStatisticsBuilder.setAvgTemp(newAvgTemp);
        deviceDataStatisticsBuilder.setAvgHumidity(newAvgHumidity);
        deviceDataStatisticsBuilder.setNumReadings(currentStats.getNumReadings() + 1);

        deviceDataStatisticsBuilder.setLastAggTime(
                latest(deviceDataStatisticsBuilder.getLastAggTime(), currentStats.getLastAggTime())
        );

        return deviceDataStatisticsBuilder.build();
    }

    private Instant latest(Instant a, Instant b) {
        return a.isAfter(b) ? a : b;
    }

    public Properties getConfig() {
        Properties streamsConfig = new Properties();

        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "device-data-processor");
        streamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, "device-data-processor-client");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        streamsConfig.put("schema.registry.url", "http://127.0.0.1:8081");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

        return streamsConfig;
    }
}
