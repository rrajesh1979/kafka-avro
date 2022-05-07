package org.rrajesh1979.iot;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.rrajesh1979.DeviceData;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class DeviceDataConsumer {
    public static void main(String[] args) {
        log.info("Starting Device Data Consumer");
        Properties properties = new Properties();
        // normal consumer
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.put("group.id", "customer-consumer-group-v1");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");

        // avro part (deserializer)
        properties.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader", "true");

        String topic = "device-data";

        try (KafkaConsumer<String, DeviceData> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singleton(topic));

            log.info("Waiting for data...");

            while (true){
                log.info("Polling");
                ConsumerRecords<String, DeviceData> deviceDataRecords
                        = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, DeviceData> deviceDataRecord : deviceDataRecords){
                    DeviceData deviceData = deviceDataRecord.value();
                    log.info("Device Data Key: {}", deviceDataRecord.key());
                    log.info("Received message {}", deviceData.toString());
                }

                kafkaConsumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Error {}", e.getMessage());
        }
    }
}
