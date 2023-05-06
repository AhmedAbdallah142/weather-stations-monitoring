package ddia.centralStation;

import ddia.centralStation.messageReciever.KafkaMessageConsumer;

import java.util.Optional;

public class CentralStationApp {
    private static final String KAFKA_TOPIC_NAME = "stations-status";

    public static void main(String[] args) {
        String kafka = Optional.ofNullable(System.getenv("kafka")).orElse("localhost:9092");

        KafkaMessageConsumer.startConsuming(kafka, KAFKA_TOPIC_NAME);
        //TODO: remove the commented code
//        String message = "{ \"station_id\": 1, \"s_no\": 2, \"battery_status\": \"low\", " +
//                "\"status_timestamp\": 18010021, \"weather\": { \"humidity\": 30, " +
//                "\"temperature\": 20, \"wind_speed\": 40 } }";
//        System.out.println(message);
//        System.out.println(StringToMessageConverter.toMessageStatus(message));
//
//        ReceivedMessageHandler messageHandler = new ReceivedMessageHandler();
//        while (true) {
//            messageHandler.processMessage(message);
//        }
    }
}
