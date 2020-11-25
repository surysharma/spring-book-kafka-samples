package com.thebigscale.kstreamsample.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UppercaseStream {

    @Bean
    public KStream<String, String> kStreamPromoToUppercase(StreamsBuilder builder) {
        KStream<String, String> sourceStream = builder
                .stream("t.upper.case",
                        Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream.print(Printed.<String, String>toSysOut().withLabel("Original KStream..."));

        KStream<String, String> upperCaseStream = sourceStream.mapValues(text -> text.toUpperCase());

        upperCaseStream.print(Printed.<String, String>toSysOut().withLabel("Uppercase KStream..."));

        return upperCaseStream;

    }
}
