package io.confluent.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.confluent.demo.processors.DedupProcessor;
import io.confluent.demo.processors.DedupProcessorSupplier;
import io.confluent.demo.util.CompareUtil;
import io.confluent.demo.util.DeathSourceReferenceUtil;
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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("unchecked")
public class PersonBuilder implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PersonBuilder.class);

    private static final String DEFAULT_CONFIG_FILE = "streams.properties";

    private Properties config;
    private final KafkaStreams streams;
    private final JedisPool jedisPool;
    private ObjectMapper mapper;
    private ZipcodeReferenceUtil zipcodeUtil;
    private DeathSourceReferenceUtil dsUtil;

    public PersonBuilder(final Properties config) {
        setConfig(config);
        mapper = new JsonMapper();
        // Redis clients init
        jedisPool = new JedisPool(
                config.getProperty("redis.host"),
                Integer.parseInt(config.getProperty("redis.port")));
        zipcodeUtil = new ZipcodeReferenceUtil(jedisPool, mapper);
        dsUtil = new DeathSourceReferenceUtil(jedisPool, mapper);
        // Kafka streams init
        Topology topology = getTopology();
        logger.debug(topology.describe().toString());
        streams = new KafkaStreams(topology, config);
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

    public String sexCodeToName(String code) {
        return switch (code.toLowerCase()) {
            case "m" -> "Male";
            case "f" -> "Female";
            case "x" -> "X";
            default -> "Unknown";
        };
    }

    public Person cdcToPerson(GenericRecord key, GenericRecord record) {
        // Build Person entity
        Person.Builder person = Person.newBuilder();
        // build the identity info
        person
                .setSsn(record.get("cossn").toString())
                .setFirstName(record.get("fnm").toString())
                .setMiddleName(record.get("mnm") != null ? record.get("mnm").toString() : null)
                .setLastName(record.get("lnm").toString())
                .setGender(sexCodeToName(record.get("sex_cd").toString()))
                .setDateOfBirth(LocalDate.ofEpochDay(Integer.parseInt(record.get("dob").toString())));

        // fetch location reference data
        if ("D".equals(record.get("brth_addr_loc_cd").toString())) {
            String zipcode = record.get("brthloclt_cd").toString();
            JsonNode zipcodeData = zipcodeUtil.getZipcodeReference(zipcode);
            person
                    .setBirthLocationCountry("United States of America")
                    .setBirthLocationState(zipcodeData.get("official_state_name").asText())
                    .setBirthLocationName(zipcodeData.get("official_usps_city_name").asText())
                    .setBirthLocationZipcode(zipcode);
        } else {
            // not handled
        }

        person
                .setCreateTs(Instant.now())
                .setUpdateTs(Instant.now());
        return person.build();
    }

    private Topology getTopology() {
        final String INPUT_TOPIC = config.getProperty("person.raw.topic");
        final String OUTPUT_TOPIC = config.getProperty("person.enriched.topic");

        // configure Serdes
        final Serde<String> stringSerde = Serdes.String();
        final GenericAvroSerde genericKeySerde = new GenericAvroSerde();
        genericKeySerde.configure((Map)config, true);
        final GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure((Map)config, false);
        final SpecificAvroSerde<Person> personAvroSerde = new SpecificAvroSerde<>();
        personAvroSerde.configure((Map)config, false);

        // build the streaming topology
        final StreamsBuilder builder = new StreamsBuilder();

        // create new Person entities/products
        builder
                // select data source topic
                .stream(INPUT_TOPIC, Consumed.with(genericKeySerde, genericAvroSerde))
                // debug
                .peek((key, genericRecord) -> logger.info("got record: [{}]{}", key, genericRecord.toString()))
                // transform raw data into an entity
                .mapValues((key, genericRecord) -> cdcToPerson(key, genericRecord))
                // check for and suppress duplicates
                .process(new DedupProcessorSupplier(config))
                // stream to the output topic
                .to(OUTPUT_TOPIC, Produced.with(genericKeySerde, personAvroSerde));

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