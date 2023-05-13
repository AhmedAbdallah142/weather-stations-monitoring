package ddia.centralStation.messageReciever;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class KafkaMessageConsumer {

    public static void startConsuming(String kafkaServer, String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "central-station-group");
        ReceivedMessageHandler messageHandler = new ReceivedMessageHandler();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            //noinspection InfiniteLoopStatement
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println(record.value());
                    messageHandler.processMessage(record.value());
                }
            }
        }
    }
}
