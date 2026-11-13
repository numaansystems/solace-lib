package com.numaansystems.solace.examples.durabletopic;

import com.numaansystems.solace.ConsumerOptions;
import com.numaansystems.solace.SolaceConsumer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Example C: durable topic subscription. Run it once, stop it, run Example A,
 * then start it again: the message published while it was down is delivered,
 * because the broker kept it on the durable topic endpoint
 * "examples-orders-audit". Only persistent messages are retained this way.
 */
@SpringBootApplication
public class DurableTopicSubscriberExample {

    public static void main(String[] args) {
        SpringApplication.run(DurableTopicSubscriberExample.class, "--numaan.solace.publisher.enabled=false");
    }

    @Bean
    ApplicationRunner subscribe(SolaceConsumer consumer) {
        return args -> consumer.register(
                ConsumerOptions.durableTopic("examples/orders/created", "examples-orders-audit").build(),
                String.class,
                message -> System.out.println("Durable subscriber received: " + message.getPayload()));
    }
}
