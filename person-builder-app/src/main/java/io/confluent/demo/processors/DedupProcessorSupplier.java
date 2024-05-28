package io.confluent.demo.processors;

import io.confluent.demo.Person;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class DedupProcessorSupplier implements ProcessorSupplier<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

    final Properties config;
    final GenericAvroSerde genericKeySerde;
    final GenericAvroSerde genericAvroSerde;
    final SpecificAvroSerde<Person> personAvroSerde;

    public DedupProcessorSupplier(final Properties config) {
        this.config = config;

        genericKeySerde = new GenericAvroSerde();
        genericKeySerde.configure((Map)config, true);
        genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure((Map)config, false);
        personAvroSerde = new SpecificAvroSerde<>();
        personAvroSerde.configure((Map)config, false);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        final StoreBuilder<KeyValueStore<GenericRecord, GenericRecord>> personStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("PersonStore"),
                        genericKeySerde,
                        genericAvroSerde);
        return Collections.singleton(personStoreBuilder);
    }

    @Override
    public Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> get() {
        return new DedupProcessor();
    }

}
