package io.confluent.demo.processors;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DedupProcessor implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

    private static final Logger logger = LoggerFactory.getLogger(DedupProcessor.class);

    ProcessorContext<GenericRecord, GenericRecord> context;
    KeyValueStore<GenericRecord, GenericRecord> personStore;

    @Override
    public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
        Processor.super.init(context);
        this.context = context;
        personStore = context.getStateStore("PersonStore");
    }

    @Override
    public void process(Record<GenericRecord, GenericRecord> record) {
        GenericRecord oldValue = personStore.get(record.key());
        logger.debug("got old value for [{}]:{}", record.key(), oldValue);
        if (oldValue != null) {
            // TODO see if it's a noop then decide to forward or squash
            logger.info("squashing duplicate update");
        } else {
            // first time seeing this, forward it
            personStore.put(record.key(), record.value());
            context.forward(record);
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }

}
