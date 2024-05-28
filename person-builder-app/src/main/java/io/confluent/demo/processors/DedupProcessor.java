package io.confluent.demo.processors;

import io.confluent.demo.Person;
import io.confluent.demo.util.CompareUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DedupProcessor implements Processor<GenericRecord, Person, GenericRecord, Person> {

    private static final Logger logger = LoggerFactory.getLogger(DedupProcessor.class);

    ProcessorContext<GenericRecord, Person> context;
    KeyValueStore<GenericRecord, Integer> personStore;

    @Override
    public void init(ProcessorContext<GenericRecord, Person> context) {
        Processor.super.init(context);
        this.context = context;
        personStore = context.getStateStore("PersonStore");
    }

    @Override
    public void process(Record<GenericRecord, Person> record) {
        Integer oldHash = personStore.get(record.key());
        logger.debug("got old value for [{}]:{}", record.key(), oldHash);

        Person personRecord = record.value();
        Integer newHash = CompareUtil.computePersonHash(personRecord);

        // see if it's a noop then decide to forward or squash
        if (oldHash != null && oldHash.equals(newHash)) {
            logger.info("squashing duplicate update");
        } else {
            // first time seeing this key, or hash difference.  Forward the record
            personStore.put(record.key(), newHash);
            context.forward(record);
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }

}
