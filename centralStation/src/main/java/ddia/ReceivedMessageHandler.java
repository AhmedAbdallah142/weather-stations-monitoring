package ddia;

public class ReceivedMessageHandler {
    public static void processMessage(String message) {
        System.out.println(message);
        //TODO: Handle invalid/dead messages
        //TODO: Archive message in parquet Files
        //TODO: update bitCask state --> (bitCask manage class may be singleton)
    }
}
