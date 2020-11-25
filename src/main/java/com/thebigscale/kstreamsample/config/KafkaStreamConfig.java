package com.thebigscale.kstreamsample.config;

import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;

@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {

    private final Environment env;

    public KafkaStreamConfig(Environment env) {
        this.env = env;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration createKafkaStreamConfigBean() {

        val props = new HashMap<String, Object>();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("sping.kafka.bootstrap-servers", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return new KafkaStreamsConfiguration(props);
    }
}
