package ddia.centralStation.messageHandler;

import ddia.bitcask.service.Bitcask;
import ddia.centralStation.messageArchive.InMemoryArchive;
import ddia.centralStation.models.BitcaskSing;
import ddia.centralStation.models.StationStatusMessage;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ReceivedMessageHandler {
    private static final int BATCH_SIZE = 10000;
    private static final String ARCHIVE_DIRECTORY = "data/archive";
    private static final String INVALID_TOPIC_NAME = "invalid-queue";
    KafkaMessageProducer producer;
    private final Bitcask bitcask;
    InMemoryArchive archive;

    public ReceivedMessageHandler() {
        archive = new InMemoryArchive(ARCHIVE_DIRECTORY, BATCH_SIZE);
        producer = new KafkaMessageProducer(INVALID_TOPIC_NAME);
        try {
            bitcask = BitcaskSing.getBitcask();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void processMessage(String message) {
        try {
            StationStatusMessage stationStatusMessage = StringToMessageConverter.toMessageStatus(message);
            archive.onNewMessageDelivered(stationStatusMessage);

            byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(stationStatusMessage.getStationId()).array();
            byte[] val = message.getBytes();
            bitcask.put(key, val);
        } catch (IOException e) {
            e.printStackTrace();
            producer.sendMessage(message);
        }
    }
}
