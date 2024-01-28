package kafka.project.streams;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Named;

public class streamsMain {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "MN:9092,SN01:9092,SN02:9092,SN03:9092,SN04:9092,SN05:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        final long[] counts = {0, 0, 0};
        final long printInterval = 5000;
        boolean flag = false;

        AtomicLong start_time = new AtomicLong(0);
        AtomicLong finish_time = new AtomicLong(0);


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> eventFilter = builder.stream("streams-test7");

        Map<String, KStream<String, String>> branches = eventFilter.split(Named.as("Record-"))
                .branch((key, value) -> value.toLowerCase().contains("error"), Branched.as("Error"))
                .branch((key, value) -> value.toLowerCase().contains("warning"), Branched.as("Warning"))
                .branch((key, value) -> value.equals("finish"), Branched.as("Finish"))
                .branch((key, value) -> value.equals("start"), Branched.as("Start"))
                .defaultBranch(Branched.as("Default"));

        KStream<String, String> errorStream = branches.get("Record-Error");
        KStream<String, String> warningStream = branches.get("Record-Warning");
        KStream<String, String> defaultStream = branches.get("Record-Default");
        KStream<String, String> finishStream = branches.get("Record-Finish");
        KStream<String, String> startStream = branches.get("Record-Start");

        errorStream.foreach((key, value) -> {
            counts[0]++; //record count
            counts[1]++; //error count

            if (counts[0] % printInterval == 0)
                System.out.println("Iteration : " + counts[0]);
        });
        errorStream.to("error-topic7");
        
        warningStream.foreach((key, value) -> {
            counts[0]++; //record count
            counts[2]++; //warning count

            if(counts[0] % printInterval == 0)
                System.out.println("Iteration : " + counts[0]);
        });
        warningStream.to("warning-topic7");

        defaultStream.foreach((key, value) -> {
            counts[0]++; //record count
            if(counts[0] % printInterval == 0)
                System.out.println("Iteration : " + counts[0]);
        });

        finishStream.foreach((key, value) -> {
            finish_time.set(System.currentTimeMillis());
            System.out.println("Entire Record :\t" + counts[0]);
            System.out.println("Error Record :\t" + counts[1]);
            System.out.println("Warning Record :\t" + counts[2]);
            System.out.println("processing time : " + (finish_time.get() - start_time.get()) + "ms");
        });

        startStream.foreach((key, value) -> {
            start_time.set(System.currentTimeMillis());
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        streams.start();

        System.out.println("Run Streams...");
    }
}
