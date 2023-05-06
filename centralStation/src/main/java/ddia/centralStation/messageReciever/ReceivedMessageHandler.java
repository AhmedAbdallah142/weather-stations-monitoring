package ddia.centralStation.messageReciever;

import ddia.centralStation.messageArchive.InMemoryArchive;
import ddia.centralStation.models.StationStatusMessage;

import java.io.IOException;

public class ReceivedMessageHandler {
    private static final int BATCH_SIZE = 10000;
    private static final String ARCHIVE_DIRECTORY = "src/main/java/ddia/centralStation";
    InMemoryArchive archive;

    public ReceivedMessageHandler() {
        archive = new InMemoryArchive(ARCHIVE_DIRECTORY, BATCH_SIZE);
    }

    public void processMessage(String message) {
        try {
            StationStatusMessage stationStatusMessage = StringToMessageConverter.toMessageStatus(message);
            archive.onNewMessageDelivered(stationStatusMessage);
            //TODO: update bitCask state --> (bitCask manage class may be singleton)
        } catch (IOException e) {
            e.printStackTrace();
            //TODO: Handle invalid/dead messages
        }
    }
}
