package com.thebigscale.kstreamsample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class KStreamSampleApplicationTests {


	private final KafkaProperties kafkaProperties;
	private final String inputTopic;
	private final String outputTopic;

	@Autowired
	public KStreamSampleApplicationTests(KafkaProperties kafkaProperties, Environment env) {
		this.kafkaProperties = kafkaProperties;
		this.inputTopic = env.getProperty("spring.kafka.input-lowercase-topic");
		this.outputTopic = env.getProperty("spring.kafka.output-uppercase-topic");

	}
	
	@Test
	@DisplayName("should test uppercaseStream topology")        
	void shouldTestUppercaseStreamTopology() {
	    //Given
		Producer<String, String> producer = configureProducer();
		Consumer<String, String> consumer = configureConsumer(outputTopic);

	    //When
		producer.send(new ProducerRecord<>(inputTopic, "test"), (metadata, exception) -> {
			String topic = metadata.topic();
			assertThat(topic).isEqualTo(inputTopic);
		});
		producer.flush();

	    //Then
		assertThat(producer).isNotNull();
		//And
		ConsumerRecords<String, String> rec = consumer.poll(Duration.ofSeconds(3));

		Iterable<ConsumerRecord<String, String>> records = rec.records(outputTopic);
		Iterator<ConsumerRecord<String, String>> iterator = records.iterator();

		if (!iterator.hasNext()) Assertions.fail();

		ConsumerRecord<String, String> next = iterator.next();
		assertThat(next.value()).isEqualTo("TEST");
	}

	private Producer<String, String> configureProducer() {
		Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(
				String.join(",", kafkaProperties.getBootstrapServers())));
		return new DefaultKafkaProducerFactory<>(producerProps,
				new StringSerializer(), new StringSerializer()).createProducer();
	}

	private Consumer<String, String> configureConsumer(String outputTopicName) {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
				String.join(",", kafkaProperties.getBootstrapServers()), "testGroup", "true");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(consumerProps,
				new StringDeserializer(), new StringDeserializer())
				.createConsumer();
		consumer.subscribe(Collections.singleton(outputTopicName));
		return consumer;
	}
}
