package com.thebigscale.kstreamsample.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Configuration
@EnableKafkaStreams
public class KafkaStreamConfiguration {

    public static final String APP_ID = "upper-case-demo";

    private final KafkaProperties kafkaProperties;

    private final String inputTopic;

    private final String outputTopic;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration getStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public NewTopic createInputTopic() { return new NewTopic(inputTopic,Optional.of(1), Optional.empty()); }

    @Bean
    public NewTopic createOutputTopic() {  return new NewTopic(outputTopic,Optional.of(1), Optional.empty()); }


    public KafkaStreamConfiguration(KafkaProperties kafkaProperties, Environment env) {
        this.kafkaProperties = kafkaProperties;
        this.inputTopic = env.getProperty("spring.kafka.input-lowercase-topic");
        this.outputTopic = env.getProperty("spring.kafka.output-uppercase-topic");

    }
}
