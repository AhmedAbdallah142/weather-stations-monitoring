package ddia.centralStation.messageArchive;

import ddia.centralStation.models.StationStatusMessage;

import java.io.IOException;
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

    public void onNewMessageDelivered(StationStatusMessage message) throws IOException {
        cachedData.add(message);
        if (cachedData.size() >= batchSize) {
            processAndWriteBatch();
            cachedData = new ArrayList<>();
        }
    }

    private void processAndWriteBatch() throws IOException {
        Map<String, Map<Long, List<StationStatusMessage>>> map = cachedData.stream().collect(Collectors.groupingBy(StationStatusMessage::getStatusDayDate, Collectors.groupingBy(StationStatusMessage::getStationId)));
        for (String dayDate : map.keySet()) {
            Map<Long, List<StationStatusMessage>> stationIdMap = map.get(dayDate);
            for (Long stationId : stationIdMap.keySet()) {
                String partitionPath = String.format("%s/%s/%d", directoryPath, dayDate, stationId);
                ParquetFileWriter.writeListToParquetFile(stationIdMap.get(stationId), partitionPath);
            }
        }

    }
}
