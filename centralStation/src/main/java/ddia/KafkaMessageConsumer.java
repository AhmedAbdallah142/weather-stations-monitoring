package ddia;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static ddia.ReceivedMessageHandler.processMessage;


public class KafkaMessageConsumer {
    private static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
    private static final String KAFKA_TOPIC_NAME = "stations-status";

    public static void main(String[] args) {
        startConsuming();
    }

    private static void startConsuming() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVER);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(KAFKA_TOPIC_NAME));
            //noinspection InfiniteLoopStatement
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    processMessage(record.value());
                }
            }
        }
    }
}
