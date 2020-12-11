package com.thebigscale.kstreamsample.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class UppercaseTopologyProcessor {


    private final String inputTopic;
    private final String outputTopic;

    UppercaseTopologyProcessor(Environment env) {
        this.inputTopic = env.getProperty("spring.kafka.input-lowercase-topic");
        this.outputTopic = env.getProperty("spring.kafka.output-uppercase-topic");
    }

    @Bean
    public KStream<String, String> kStreamPromoToUppercase(StreamsBuilder builder) {

        KStream<String, String> sourceStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream.print(Printed.<String, String>toSysOut().withLabel("Original KStream in getTopology..."));

        sourceStream.process(CapitalCaseProcessor::new);

        KStream<String, String> camelCaseStream = sourceStream.transform(HeaderAppender::new, Named.as("camelcase-processor"));

        KStream<String, String> upperCaseStream = camelCaseStream.mapValues((ValueMapper<String, String>) String::toUpperCase);

        upperCaseStream.print(Printed.<String, String>toSysOut().withLabel("Uppercase KStream..."));

        upperCaseStream.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        System.out.println(topology.describe());
        return upperCaseStream;
    }
}
