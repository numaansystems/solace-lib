package com.numaansystems.solace.examples.queueconsumer;

import com.numaansystems.solace.ConsumerOptions;
import com.numaansystems.solace.SolaceConsumer;
import com.numaansystems.solace.SolaceMessage;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.math.BigDecimal;

/**
 * Example B: consume a queue. With the default AUTO acknowledgement mode the
 * message is acknowledged only after {@code handle} returns normally; an
 * exception triggers the retry policy and then the dead-letter queue.
 */
@SpringBootApplication
public class QueueConsumerExample {

    public record OrderCreated(String orderId, String customer, BigDecimal amount) {
    }

    public static void main(String[] args) {
        SpringApplication.run(QueueConsumerExample.class, "--numaan.solace.publisher.enabled=false");
    }

    @Bean
    ApplicationRunner consume(SolaceConsumer consumer) {
        return args -> consumer.register(
                ConsumerOptions.queue("examples.orders.inbound")
                        .concurrency(2)
                        .deadLetterQueue("examples.orders.inbound.dlq")
                        .build(),
                OrderCreated.class,
                this::handle);
    }

    private void handle(SolaceMessage<OrderCreated> message) {
        // Redelivery is possible: use message.getMessageId() as an idempotency key.
        System.out.printf("Order %s for %s (messageId=%s, redelivered=%s)%n",
                message.getPayload().orderId(), message.getPayload().customer(),
                message.getMessageId(), message.isRedelivered());
    }
}
