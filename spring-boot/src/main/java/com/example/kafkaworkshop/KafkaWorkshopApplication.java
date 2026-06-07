package com.example.kafkaworkshop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Kafka tuning workshop app.
 *
 * Spring Boot auto-configures a KafkaTemplate (producer) and the listener
 * container factory used by @KafkaListener from the properties in
 * src/main/resources/application.yml — so there's no boilerplate config class.
 *
 * Run it with:  ./mvnw spring-boot:run
 */
@SpringBootApplication
public class KafkaWorkshopApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaWorkshopApplication.class, args);
    }
}
