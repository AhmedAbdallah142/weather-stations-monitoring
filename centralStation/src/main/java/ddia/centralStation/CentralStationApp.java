package ddia.centralStation;

import ddia.centralStation.messageReciever.KafkaMessageConsumer;
import ddia.centralStation.models.BitcaskSing;

import java.io.IOException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

public class CentralStationApp {
    private static final String KAFKA_TOPIC_NAME = "stations-status";
    private static final long mergeDelay = 1000L * 60 * 60 * 24;

    public static void main(String[] args) {
        startBitcaskMergeScheduler();
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

    private static void startBitcaskMergeScheduler() {
        Timer timer = new Timer();

        TimerTask task = new TimerTask() {
            public void run() {
                try {
                    BitcaskSing.getBitcask().merge();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        timer.scheduleAtFixedRate(task, mergeDelay, mergeDelay);
    }
}
