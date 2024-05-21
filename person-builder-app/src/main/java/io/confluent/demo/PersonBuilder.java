package io.confluent.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.confluent.demo.util.ZipcodeReferenceUtil;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PersonBuilder implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PersonBuilder.class);

    private static final String DEFAULT_CONFIG_FILE = "streams.properties";

    private Properties config;
    private final KafkaStreams streams;
    private final JedisPool jedisPool;
    private ObjectMapper mapper;
    private ZipcodeReferenceUtil zipcodeUtil;

    public PersonBuilder(final Properties config) {
        setConfig(config);
        jedisPool = new JedisPool(
                config.getProperty("redis.host"),
                Integer.parseInt(config.getProperty("redis.port")));
        mapper = new JsonMapper();
        zipcodeUtil = new ZipcodeReferenceUtil(jedisPool, mapper);
        streams = new KafkaStreams(getTopology(), config);
    }

    public void setConfig(final Properties config) {
        if (!config.containsKey("person.raw.topic"))
            throw new RuntimeException("Missing required property ".concat("person.raw.topic"));
        if (!config.containsKey("person.enriched.topic"))
            throw new RuntimeException("Missing required property ".concat("person.enriched.topic"));
        if (!config.containsKey("redis.host"))
            throw new RuntimeException("Missing required property ".concat("redis.host"));
        if (!config.containsKey("redis.port"))
            throw new RuntimeException("Missing required property ".concat("redis.port"));
        this.config = config;
    }

    private Topology getTopology() {
        final String INPUT_TOPIC = config.getProperty("person.raw.topic");
        final String OUTPUT_TOPIC = config.getProperty("person.enriched.topic");

        // configure Serdes
        final Serde<String> stringSerde = Serdes.String();
        final GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure((Map)config, false);
        final SpecificAvroSerde<Person> personAvroSerde = new SpecificAvroSerde<>();
        personAvroSerde.configure((Map)config, false);

        // build the streaming topology
        final StreamsBuilder builder = new StreamsBuilder();

        // TODO app logic
        builder
                .stream(INPUT_TOPIC, Consumed.with(stringSerde, genericAvroSerde))
                .peek((key, genericRecord) -> logger.info("got record: [{}]{}", key, genericRecord.toString()))
                .peek((key, genericRecord) -> {
                    GenericRecord after = (GenericRecord) genericRecord.get("after");
                    String zipcode = after.get("zip_code").toString();
                    zipcodeUtil.getZipcodeReference((String) zipcode);
                });

        return builder.build();
    }

    @Override
    public void run() {
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            logger.info("state change: {} -> {}", oldState, newState);
            if ((oldState == KafkaStreams.State.RUNNING ||
                    oldState == KafkaStreams.State.REBALANCING )
                    && newState != KafkaStreams.State.RUNNING) {
                logger.info("latch decrementing");
                latch.countDown();
            }
        });

        // Use this in development to clean the local streams state
        streams.cleanUp();
        // start the streams app
        streams.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            logger.debug("PersonBuilder closed!");
        }
    }

    public void stop() {
        streams.close();
    }

    public static void main(String ...args) throws IOException {
        String configPath = args.length > 0 ? args[0] : DEFAULT_CONFIG_FILE;
        final Properties cfg = new Properties();
        cfg.load(new FileInputStream(configPath));

        final PersonBuilder personBuilder = new PersonBuilder(cfg);
        Runtime.getRuntime().addShutdownHook(new Thread(personBuilder::stop));
        personBuilder.run();
    }
}