package ddia.centralStation.messageReciever;

import ddia.centralStation.models.StationStatusMessage;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class StringToMessageConverter {
    public static StationStatusMessage toMessageStatus(String message) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(message, StationStatusMessage.class);
    }
}
