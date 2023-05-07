package rainingTriggers;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static ddia.WeatherStationOpenMeteo.jsonToMap;

public class RainingProcessor {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputTopic = builder.stream("stations-status");
        KStream<String, String> outputTopic = inputTopic.filter((key, value) ->
                ( (int) Math.round(Double.parseDouble(jsonToMap (jsonToMap (value).get("weather").toString()).get("humidity").toString())))>80);

        outputTopic.to("rain-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}