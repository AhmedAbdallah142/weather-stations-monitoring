package ddia.centralStation;

import ddia.centralStation.messageReciever.KafkaMessageConsumer;

public class CentralStationApp {
    private static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
    private static final String KAFKA_TOPIC_NAME = "stations-status";

    public static void main(String[] args) {
        KafkaMessageConsumer.startConsuming(KAFKA_BOOTSTRAP_SERVER, KAFKA_TOPIC_NAME);
    }
}
