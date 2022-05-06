package org.rrajesh1979.v1;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.rrajesh1979.Customer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaAvroJavaConsumer {
    public static void main(String[] args) {
        log.info("Starting Kafka Avro Java Consumer");
        Properties properties = new Properties();
        // normal consumer
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.put("group.id", "customer-consumer-group-v1");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");

        // avro part (deserializer)
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader", "true");

        String topic = "customer-avro";

        try (KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singleton(topic));

            log.info("Waiting for data...");

            while (true){
                log.info("Polling");
                ConsumerRecords<String, Customer> customerRecords
                        = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Customer> customerRecord : customerRecords){
                    Customer customer = customerRecord.value();
                    log.info("Received message {}", customer.toString());
                }

                kafkaConsumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Error {}", e.getMessage());
        }

    }
}
