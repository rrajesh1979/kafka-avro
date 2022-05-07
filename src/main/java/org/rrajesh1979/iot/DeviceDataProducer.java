package org.rrajesh1979.iot;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rrajesh1979.DeviceData;
import org.rrajesh1979.DeviceDataKey;

import java.util.Properties;
import java.util.Random;

@Slf4j
public class DeviceDataProducer {
    public static void main(String[] args) {
        log.info("Starting Device Data Producer");

        Properties properties = new Properties();
        // normal producer
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        // avro part
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        Producer<Object, Object> producer = new KafkaProducer<>(properties);

        String topic = "device-data";

        Random random = new Random();

        for (long deviceId = 0; deviceId <= 10; deviceId++) {
            for (long timestamp = 0; timestamp < 10; timestamp++) {
                DeviceDataKey deviceDataKey = DeviceDataKey.newBuilder()
                        .setDeviceId(deviceId)
                        .build();
                DeviceData deviceDataValue = DeviceData.newBuilder()
                        .setDeviceId(deviceId)
                        .setTemp(random.nextDouble(25, 35))
                        .setHumidity(random.nextDouble(30 ,60))
                        .setTimestamp(System.currentTimeMillis() + timestamp * 1000)
                        .build();

                ProducerRecord<Object, Object> producerRecord =
                        new ProducerRecord<>(topic, deviceDataKey, deviceDataValue);

                log.info("Sending message {} to topic {}", deviceDataValue, topic);

                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Received new metadata: {}}", metadata);
                    } else {
                        log.error("Failed to send message {}", exception.getMessage());
                    }
                });
            }

            producer.flush();
        }

        producer.close();
    }
}
