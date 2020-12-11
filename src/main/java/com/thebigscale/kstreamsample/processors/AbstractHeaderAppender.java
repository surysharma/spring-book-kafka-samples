package com.thebigscale.kstreamsample.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public abstract class AbstractHeaderAppender implements Transformer<String, String, KeyValue<String, String>> {

    protected ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        System.out.println("CamelcaseTransformer called...");
        updateHeaders();
        return KeyValue.pair(key, value);
    }

    abstract void updateHeaders();

    @Override
    public void close() {
    }

}
