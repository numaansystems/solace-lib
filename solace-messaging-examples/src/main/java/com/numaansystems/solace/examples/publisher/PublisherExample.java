package com.numaansystems.solace.examples.publisher;

import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.DestinationType;
import com.numaansystems.solace.PublishOptions;
import com.numaansystems.solace.PublishResult;
import com.numaansystems.solace.SolacePublisher;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.math.BigDecimal;

/**
 * Example A: publish persistent JSON messages, one to a queue (read by
 * Example B) and one to a topic (read by Examples C and D).
 */
@SpringBootApplication
public class PublisherExample {

    public record OrderCreated(String orderId, String customer, BigDecimal amount) {
    }

    public static void main(String[] args) {
        SpringApplication.run(PublisherExample.class, "--numaan.solace.consumer.enabled=false").close();
    }

    @Bean
    ApplicationRunner publish(SolacePublisher publisher) {
        return args -> {
            OrderCreated order = new OrderCreated("ORD-1001", "acme", new BigDecimal("249.90"));

            PublishResult toQueue = publisher.publish("examples.orders.inbound", order, PublishOptions.builder()
                    .destinationType(DestinationType.QUEUE)
                    .deliveryMode(DeliveryMode.PERSISTENT)
                    .correlationId(order.orderId())
                    .property("tenant", "acme")
                    .build());
            System.out.println("Queue publish acknowledged by broker, messageId=" + toQueue.messageId());

            PublishResult toTopic = publisher.publish("examples/orders/created", order, PublishOptions.builder()
                    .deliveryMode(DeliveryMode.PERSISTENT)
                    .correlationId(order.orderId())
                    .build());
            System.out.println("Topic publish acknowledged by broker, messageId=" + toTopic.messageId());
        };
    }
}
