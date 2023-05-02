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
        Map<Long, Map<String, List<StationStatusMessage>>> map = cachedData.stream().collect(Collectors.groupingBy(StationStatusMessage::getStationId, Collectors.groupingBy(StationStatusMessage::getStatusDayDate)));
        for (Long stationId : map.keySet()) {
            Map<String, List<StationStatusMessage>> dayDateMap = map.get(stationId);
            for (String dayDate : dayDateMap.keySet()) {
                String partitionPath = String.format("%s/%d/%s", directoryPath, stationId, dayDate);
                ParquetFileWriter.writeListToParquetFile(dayDateMap.get(dayDate), partitionPath);
            }
        }

    }
}
