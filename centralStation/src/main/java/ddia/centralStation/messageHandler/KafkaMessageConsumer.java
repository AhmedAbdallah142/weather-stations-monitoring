package ddia.centralStation.messageHandler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class KafkaMessageConsumer {

    private static final String DEAD_TOPIC_NAME = "dead-letter-queue";

    public static void startConsuming(String kafkaServer, String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "central-station-group");
        ReceivedMessageHandler messageHandler = new ReceivedMessageHandler();

        KafkaMessageProducer producer = new KafkaMessageProducer(DEAD_TOPIC_NAME);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            //noinspection InfiniteLoopStatement
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println(record.value());
                    String destination = new String(record.headers().toArray()[0].value());
                    if (destination.equalsIgnoreCase("central station"))
                        messageHandler.processMessage(record.value());
                    else
                        producer.sendMessage(record.value());

                }
            }
        }
    }
}
