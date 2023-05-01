package ddia.centralStation.messageArchive;

import ddia.centralStation.models.StationStatusMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InMemoryArchive {
    private final String directoryPath;
    private final int batchSize;
    List<StationStatusMessage> cachedData;

    public InMemoryArchive(String directoryPath, int batchSize) {
        this.cachedData = new ArrayList<>();
        this.directoryPath = directoryPath;
        this.batchSize = batchSize;
    }

    public void onNewMessageDelivered(StationStatusMessage message) {
        cachedData.add(message);
        if (cachedData.size() >= batchSize) {
            processAndWriteBatch();
        }
    }

    private void processAndWriteBatch() {
        //TODO convert time stamp to day --> and write the data into parquet
        Map<Long, Map<Long, List<StationStatusMessage>>> map = cachedData.stream().collect(Collectors.groupingBy(StationStatusMessage::getStationId, Collectors.groupingBy(message -> message.getStatusTimestamp() / 1000)));
        cachedData = new ArrayList<>();
    }
}
