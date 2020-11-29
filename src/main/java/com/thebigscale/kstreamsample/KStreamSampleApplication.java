package com.thebigscale.kstreamsample;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KStreamSampleApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KStreamSampleApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		System.out.println("Started the KStream spring boot CLI...");
	}
}
