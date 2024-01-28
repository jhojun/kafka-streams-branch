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

        KStream<String, String> eventFilter = builder.stream(/* producer 실습때 만들었던 본인의 topic 이름 */);
        // 토폴로지 작성 코드 시작
        // 조건에 따라 eventFilter를 split하여 각각의 branch를 생성하고, 각 branch마다 처리하도록 구성
        /* 코드 작성 */
        //branch의 이름으로 각자 KStream마다 branch 할당 (접두사는 Named.as로, 접미사는 Branched.as로 지정)
        /* 코드 작성 */
        // 토폴로지 작성 부분 끝

        //각각의 branch마다 wordcount, 로그 출력 등 필요한 작업을 수행
        errorStream.foreach((key, value) -> {
            counts[0]++; //record count
            counts[1]++; //error count

            if (counts[0] % printInterval == 0)
                System.out.println("Iteration : " + counts[0]);
        });
        //branch마다 최종적으로 record를 보낼 topic을 선택
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
