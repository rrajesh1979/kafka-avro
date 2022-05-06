package org.rrajesh1979.v1;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rrajesh1979.Customer;

import java.util.Properties;

@Slf4j
public class KafkaAvroJavaProducer {
    public static void main(String[] args) {
        log.info("Starting Kafka Avro Java Producer");

        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        Producer<String, Customer> producer = new KafkaProducer<>(properties);

        String topic = "customer-avro";

        Customer customer = Customer.newBuilder()
                .setAge(34)
                .setAutomatedEmail(false)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .build();

        ProducerRecord<String, Customer> producerRecord =
                new ProducerRecord<>(topic, customer);

        log.info("Sending message {} to topic {}", customer, topic);

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                log.info("Received new metadata: {}}", metadata);
            } else {
                log.error("Failed to send message {}", exception.getMessage());
            }
        });

        producer.flush();
        producer.close();
    }
}
