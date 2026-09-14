package com.example.mini_kafka;

import com.example.mini_kafka.broker.Broker;
import com.example.mini_kafka.consumer.OffsetManager;
import com.example.mini_kafka.producer.Producer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class MiniKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(MiniKafkaApplication.class, args);
	}

	@Bean
	public Broker broker() {
		return new Broker();
	}

	@Bean
	public OffsetManager offsetManager() {
		return new OffsetManager();
	}

	@Bean
	public Producer producer(Broker broker) {
		return new Producer(broker);
	}
}