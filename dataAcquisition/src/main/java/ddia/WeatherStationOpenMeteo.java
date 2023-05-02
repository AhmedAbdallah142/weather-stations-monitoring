package ddia;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class WeatherStationOpenMeteo {
    private static final String[] BATTERY_STATUS_OPTIONS = {"low", "medium", "high"};
    private static final double[] BATTERY_STATUS_PROBABILITIES = {0.3, 0.4, 0.3};
    private static final double DROP_RATE = 0.1;
    private static final String topicName = "stations-status";
    private static long sNo = 1;
    private static final Random random = new Random();
    static int STATION_ID = 1;

    public static Map<String,Object> jsonToMap(String str){
        return new Gson().fromJson(str,new
                TypeToken<HashMap<String,Object>>() {}.getType());
    }

    public static void  main(String[] args)  throws InterruptedException  {
        String urlString = "https://api.open-meteo.com/v1/forecast?latitude=31.20&longitude=29.92&current_weather=true&temperature_unit=fahrenheit&timeformat=unixtime&forecast_days=1&timezone=Africa%2FCairo";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (true) {
                StringBuilder result = new StringBuilder();
                URL url = new URL(urlString);
                URLConnection conn = url.openConnection();
                BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String line;
                while ((line = rd.readLine()) != null) {
                    result.append(line);
                }
                rd.close();
                String message = channelAdapter(result.toString());
                // Determine if the message should be dropped
                if (random.nextDouble() < DROP_RATE) {
                    System.out.println("Message dropped: " + message);
                } else {
                    // Producer send message
                    producer.send(new ProducerRecord<>(topicName, message));
                    System.out.println("Weather status message: " + message);
                }
                // Increment the message counter
                sNo++;

                // Wait for one second
                Thread.sleep(1000);
            }

        }catch (IOException e){
            System.out.println(e.getMessage());
        }

        }

        public static String channelAdapter(String result){
            Map<String, Object > respMap = jsonToMap (result);
            Map<String, Object > mainMap = jsonToMap (respMap.get("current_weather").toString());
            int humidity = random.nextInt(101);
            int temperature = (int) Math.round(Double.parseDouble(mainMap.get("temperature").toString()));
            int windSpeed = (int) Math.round(Double.parseDouble(mainMap.get("windspeed").toString()));
            long statusTimestamp = Math.round(Double.parseDouble(mainMap.get("time").toString()));
            String batteryStatus = WeatherStation.selectBatteryStatus(random);
            return String.format(
                    "{ \"station_id\": %d, \"s_no\": %d, \"battery_status\": \"%s\", " +
                            "\"status_timestamp\": %d, \"weather\": { \"humidity\": %d, " +
                            "\"temperature\": %d, \"wind_speed\": %d } }",
                    STATION_ID, sNo, batteryStatus, statusTimestamp, humidity, temperature, windSpeed);
        }
}
