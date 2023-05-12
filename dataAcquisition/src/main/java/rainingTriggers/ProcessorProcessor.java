package rainingTriggers;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Properties;

import static ddia.WeatherStationOpenMeteo.jsonToMap;

public class ProcessorProcessor {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-example");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology builder = new Topology();
        builder.addSource("Source", "stations-status")
                .addProcessor("Process", RainingTriggerProcessor::new, "Source")
                .addSink("Sink", "rain-topic", "Process");


        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }

    private static class RainingTriggerProcessor implements Processor<String, String, String, String> {

        private ProcessorContext<String, String> context;

        @Override
        public void init(ProcessorContext<String, String> context) {
            Processor.super.init(context);
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            System.out.println(record.value());
            if ((int) Math.round(Double.parseDouble(jsonToMap(jsonToMap(record.value()).get("weather").toString()).get("humidity").toString())) > 70) {
                // forward the output to the next processor or topic
                context.forward(record);
            }
        }

        @Override
        public void close() {
            Processor.super.close();
        }
    }
}