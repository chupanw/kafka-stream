package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "LineSplit-<andrewID>");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "jenkins-vbc.isri.cmu.edu:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Topology
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("log-stream");
        // todo: 1. split each line into words (hint: Java regex for white space is "\\W+")
        //       2. prepend each word with your andrew id
        //       3. pipe the resulted words to the "split-line" topic
        KStream<String, String> words = null;
        words.to("split-line");

        final Topology topology = builder.build();
        // In case you want to check the topology:
//        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        // For shutting down gracefully, no need to change
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
               streams.close();
               latch.countDown();
            }
        });
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}