package ddia.centralStation.messageHandler;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;

public class KafkaMessageProducer {

    private final KafkaProducer<String, String> producer;
    private final String topicName;

    public KafkaMessageProducer(String topicName) {
        String kafka = Optional.ofNullable(System.getenv("kafka")).orElse("localhost") + ":9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
        this.topicName = topicName;
    }

    public void sendMessage(String message) {
        producer.send(new ProducerRecord<>(topicName, message));
    }

}
