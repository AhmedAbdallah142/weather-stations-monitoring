package ddia.centralStation.messageHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import ddia.centralStation.models.StationStatusMessage;

import java.io.IOException;

public class StringToMessageConverter {
    public static StationStatusMessage toMessageStatus(String message) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(message, StationStatusMessage.class);
    }
}
