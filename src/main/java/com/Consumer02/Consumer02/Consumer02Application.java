package com.Consumer02.Consumer02;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
public class Consumer02Application {

	public static void main(String[] args) {
		SpringApplication.run(Consumer02Application.class, args);
	}

}
