package com.thebigscale.kstreamsample.processors;

import org.apache.kafka.streams.processor.AbstractProcessor;

public class CapitalCaseProcessor extends AbstractProcessor<String, String> {
    @Override
    public void process(String key, String value) {
        System.out.println("Capital case..." + value);
    }
}
