package com.thebigscale.kstreamsample;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KStreamSampleApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KStreamSampleApplication.class, args);
	}

	@Override
	public void run(String... args) {
		System.out.println("Started the KStream spring boot CLI...");
	}
}
