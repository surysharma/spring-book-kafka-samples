package com.thebigscale.kstreamsample.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class UppercaseTopologyConfiguration {


    private final String inputTopic;
    private final String outputTopic;

    UppercaseTopologyConfiguration(Environment env) {
        this.inputTopic = env.getProperty("spring.kafka.input-lowercase-topic");
        this.outputTopic = env.getProperty("spring.kafka.output-uppercase-topic");
    }

    @Bean
    public Topology getTopology(StreamsBuilder builder) {

        KStream<String, String> sourceStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream.print(Printed.<String, String>toSysOut().withLabel("Original KStream in getTopology..."));

        KStream<String, String> upperCaseStream = sourceStream.mapValues(text -> text.toUpperCase());

        upperCaseStream.print(Printed.<String, String>toSysOut().withLabel("Uppercase KStream..."));

        upperCaseStream.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        System.out.println("!!!!!!!!!!!");
        System.out.println(topology.describe());
        System.out.println("!!!!!!!!!!!");
        return topology;
    }
}
