package com.numaansystems.solace.examples.nondurabletopic;

import com.numaansystems.solace.ConsumerOptions;
import com.numaansystems.solace.SolaceConsumer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Example D: non-durable topic subscription with a wildcard. It receives only
 * what is published while it is connected; anything published while it is
 * down is never delivered to it, regardless of the delivery mode.
 */
@SpringBootApplication
public class NonDurableTopicSubscriberExample {

    public static void main(String[] args) {
        SpringApplication.run(NonDurableTopicSubscriberExample.class, "--numaan.solace.publisher.enabled=false");
    }

    @Bean
    ApplicationRunner subscribe(SolaceConsumer consumer) {
        return args -> consumer.register(
                ConsumerOptions.topic("examples/orders/>").build(),
                String.class,
                message -> System.out.println("Live subscriber received on " + message.getDestination()
                        + ": " + message.getPayload()));
    }
}
