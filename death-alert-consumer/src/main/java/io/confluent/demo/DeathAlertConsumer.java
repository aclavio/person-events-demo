package io.confluent.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class DeathAlertConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DeathAlertConsumer.class);

    private static final String DEFAULT_CONFIG_FILE = "consumer.properties";

    private final KafkaConsumer<String, GenericRecord> consumer;
    private final List<String> topics;
    private final CountDownLatch shutdownLatch;

    final ObjectMapper mapper;

    public DeathAlertConsumer(Properties config, List<String> topics) {
        // initialize the Kafka Consumer using the properties file
        this.consumer = new KafkaConsumer<>(config);
        this.topics = topics;
        this.shutdownLatch = new CountDownLatch(1);
        mapper = new JsonMapper();
    }

    @Override
    public void run() {
        try {
            // subscribe to the Kafka topics
            consumer.subscribe(topics);

            logger.info("Waiting for events...");

            // basic Kafka consumer "poll loop"
            while (true) {
                // poll for new kafka events
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Long.MAX_VALUE);

                // application specific processing...
                records.forEach(record -> {
                    // for demo purposes, just emit a log statement
                    GenericRecord alert = record.value();
                    logger.info("[{}] got record: [{}] {}", record.topic(), record.key(), alert);

                    if (logger.isDebugEnabled()) {
                        try {
                            logger.debug(mapper.readTree(alert.toString()).toPrettyString());
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

                // commit the offsets back to kafka
                //consumer.commitSync();
            }

        } catch (WakeupException ex) {
            // awake from poll
        } catch (Exception ex) {
            logger.error("An unexpected error occurred!", ex);
        } finally {
            // gracefully shutdown the consumer!
            consumer.close();
            shutdownLatch.countDown();
        }
    }

    public void shutdown() throws InterruptedException {
        consumer.wakeup();
        shutdownLatch.await();
    }

    public static void main(String[] args) throws Exception {
        // load passed in properties
        String configPath = args.length > 0 ? args[0] : DEFAULT_CONFIG_FILE;
        final Properties cfg = new Properties();
        cfg.load(new FileInputStream(configPath));
        // get the topic to consumer from
        String kafkaTopic = args.length > 1 ? args[1] : cfg.getProperty("death.alert.topic", "death-alerts");
        List<String> topics = Arrays.asList(kafkaTopic.split(","));

        // Start up our consumer thread
        DeathAlertConsumer dac = new DeathAlertConsumer(cfg, topics);
        Thread thread = new Thread(dac);
        thread.start();

        try {
            thread.join();
        } catch (InterruptedException e) {
            dac.shutdown();
        }
    }

}
