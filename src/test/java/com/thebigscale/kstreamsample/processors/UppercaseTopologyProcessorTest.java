package com.thebigscale.kstreamsample.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

import java.util.Properties;

import static com.thebigscale.kstreamsample.config.KafkaStreamConfiguration.APP_ID;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class UppercaseTopologyProcessorTest {

    @Mock Environment env;
    StreamsBuilder sb = new StreamsBuilder();
    TopologyTestDriver topologyTestDriver;

    String INPUT_TOPIC = "input-topic";
    String OUTPUT_TOPIC = "out-topic";

    @BeforeEach
    public void setup() {
        given(env.getProperty("spring.kafka.input-lowercase-topic")).willReturn(INPUT_TOPIC);
        given(env.getProperty("spring.kafka.output-uppercase-topic")).willReturn(OUTPUT_TOPIC);
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        new UppercaseTopologyProcessor(env).kStreamPromoToUppercase(sb);
        topologyTestDriver = new TopologyTestDriver(
                sb.build(), kafkaProperties);
    }

    @Test
    @DisplayName("should convert lower to uppercase topology")
    void shouldConvertLowerToUppercaseTopology() {
        //Given
        TestInputTopic<String, String> inputTopic = topologyTestDriver.createInputTopic(INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, String> outputTopic = topologyTestDriver.createOutputTopic(OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());

        //When
        inputTopic.pipeInput("test");

        //Then
        Assertions.assertThat(outputTopic.readValue()).isEqualTo("TEST");
    }

    @AfterEach
    public void tearDown(){
        topologyTestDriver.close();
    }


}