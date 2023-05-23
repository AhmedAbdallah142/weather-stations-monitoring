package ddia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class WeatherStation {
    private static final String[] BATTERY_STATUS_OPTIONS = {"low", "medium", "high"};
    private static final double[] BATTERY_STATUS_PROBABILITIES = {0.3, 0.4, 0.3};
    private static final double DROP_RATE = 0.1;
    private static final String topicName = "stations-status";
    private static long sNo = 1;

    public static void main(String[] args) {
        Random random = new Random();

        int STATION_ID = Math.abs(random.nextInt());
        String kafka = Optional.ofNullable(System.getenv("kafka")).orElse("localhost") + ":9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            public void run() {
                // Generate the weather status message
                String batteryStatus = selectBatteryStatus(random);
                long statusTimestamp = System.currentTimeMillis();
                int humidity = random.nextInt(101);
                int temperature = random.nextInt(201) - 50;
                int windSpeed = random.nextInt(101);
                String message = String.format(
                        "{ \"station_id\": %d, \"s_no\": %d, \"battery_status\": \"%s\", " +
                                "\"status_timestamp\": %d, \"weather\": { \"humidity\": %d, " +
                                "\"temperature\": %d, \"wind_speed\": %d } }",
                        STATION_ID, sNo, batteryStatus, statusTimestamp, humidity, temperature, windSpeed);

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
            }
        };
        timer.scheduleAtFixedRate(task, 0, 1000);
    }

    static String selectBatteryStatus(Random random) {
        double p = random.nextDouble();
        double cumulativeProbability = 0.0;
        for (int i = 0; i < BATTERY_STATUS_OPTIONS.length; i++) {
            cumulativeProbability += BATTERY_STATUS_PROBABILITIES[i];
            if (p <= cumulativeProbability) {
                return BATTERY_STATUS_OPTIONS[i];
            }
        }
        return BATTERY_STATUS_OPTIONS[BATTERY_STATUS_OPTIONS.length - 1];
    }
}
