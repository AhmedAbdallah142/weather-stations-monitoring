package ddia.centralStation;

import ddia.centralStation.messageReciever.KafkaMessageConsumer;
import ddia.centralStation.models.BitcaskSing;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

@Service
public class CentralStationService {

    private static final String KAFKA_TOPIC_NAME = "stations-status";
    private static final long mergeDelay = 1000L * 60 * 60 * 24;

    @PostConstruct
    public void init() {
        Thread backgroundThread = new Thread(this::weatherMessagesHandler);
        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }

    private void weatherMessagesHandler() {
        startBitcaskMergeScheduler();
        String kafka = Optional.ofNullable(System.getenv("kafka")).orElse("localhost") + ":9092";

        KafkaMessageConsumer.startConsuming(kafka, KAFKA_TOPIC_NAME);
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
