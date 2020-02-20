package com.github.vikasgautam18;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.ResourceBundle;
import java.util.concurrent.CountDownLatch;

import static com.github.vikasgautam18.Constants.*;

public class KStreamFilters {

    private static final ResourceBundle moduleProps = ResourceBundle.getBundle("experiments-with-kafka-streams");


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(KStreamFilters.class.getName());
        final CountDownLatch latch = new CountDownLatch(1);

        //Create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, moduleProps.getString(BOOTSTRAP_SERVERS));
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, moduleProps.getString(APPLICATION_ID));
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // build strambuilder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // stream input topic
        KStream<String, String> twitterStream = streamsBuilder.stream(moduleProps.getString(TWITTER_FEED_TOPIC));

        // filter stream
        KStream<String, String> importantStream = twitterStream.filter(
                (key, value) -> extractNumFollowers(value) > 10000
        );

        // write filtered stram to output topic
        importantStream.to(moduleProps.getString(TWITTER_FILTERED_TOPIC));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                logger.info("Shutdown hook called...");
                logger.info("Closing KStream Application ...");
                kafkaStreams.close();
                latch.countDown();

                logger.info("Kstream application closed !!");
            }
        });

        kafkaStreams.start();
    }

    private static int extractNumFollowers(String value) {
        try{
            return JsonParser.parseString(value)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e){
            return 0;
        }
    }
}
