package org.rrajesh1979.iot;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rrajesh1979.Customer;
import org.rrajesh1979.DeviceData;

import java.util.Properties;

@Slf4j
public class DeviceDataProducer {
    public static void main(String[] args) {
        log.info("Starting Device Data Producer");

        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        Producer<String, DeviceData> producer = new KafkaProducer<>(properties);

        String topic = "device-data";

        for (int i = 0; i < 100; i++) {
            DeviceData deviceData = DeviceData.newBuilder()
                    .setDeviceId(i)
                    .setTemp(20.0)
                    .setHumidity(50.0)
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            ProducerRecord<String, DeviceData> producerRecord =
                    new ProducerRecord<>(topic, deviceData);

            log.info("Sending message {} to topic {}", deviceData, topic);

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Received new metadata: {}}", metadata);
                } else {
                    log.error("Failed to send message {}", exception.getMessage());
                }
            });

            producer.flush();
        }

        producer.close();
    }
}
