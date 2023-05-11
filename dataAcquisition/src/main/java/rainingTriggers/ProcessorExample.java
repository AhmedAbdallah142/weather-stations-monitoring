package rainingTriggers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Properties;

import static ddia.WeatherStationOpenMeteo.jsonToMap;

public class ProcessorExample {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-example");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        // create a stream
        KStream<String, String> stream = builder.stream("stations-status");

        // apply a processor
        stream.process(() -> new MyProcessor());

        // write the output to a new topic
        stream.to("rain-topic", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    private static class MyProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(String key, String value) {
            // do some processing
            if((int) Math.round(Double.parseDouble(jsonToMap(jsonToMap(value).get("weather").toString()).get("humidity").toString())) > 70){
                KeyValue<String, String> output = KeyValue.pair(key, value);
                // forward the output to the next processor or topic
                context().forward(output.key, output.value);
            }
        }

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
        }
    }
}