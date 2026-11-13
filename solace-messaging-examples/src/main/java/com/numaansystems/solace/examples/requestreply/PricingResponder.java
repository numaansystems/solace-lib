package com.numaansystems.solace.examples.requestreply;

import com.numaansystems.solace.ConsumerOptions;
import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.PublishOptions;
import com.numaansystems.solace.SolaceConsumer;
import com.numaansystems.solace.SolaceMessage;
import com.numaansystems.solace.SolacePublisher;
import com.numaansystems.solace.examples.requestreply.PricingMessages.PricingRequest;
import com.numaansystems.solace.examples.requestreply.PricingMessages.PricingResponse;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * The receiving side: consumes requests from a queue and publishes each
 * response to the request's reply-to destination, echoing the correlation ID.
 */
@Component
public class PricingResponder {

    static final String REQUEST_QUEUE = "examples.pricing.request";

    private final SolacePublisher publisher;

    public PricingResponder(SolaceConsumer consumer, SolacePublisher publisher) {
        this.publisher = publisher;
        consumer.register(ConsumerOptions.queue(REQUEST_QUEUE).name("pricing-responder").build(),
                PricingRequest.class, this::process);
    }

    private void process(SolaceMessage<PricingRequest> message) {
        if (message.getReplyTo() == null) {
            return; // nobody is waiting for an answer
        }
        PricingRequest request = message.getPayload();
        PricingResponse response =
                new PricingResponse(request.product(), new BigDecimal("9.99").multiply(BigDecimal.valueOf(request.quantity())));

        publisher.publish(message.getReplyTo(), response, PublishOptions.builder()
                .destinationType(message.getReplyToType())
                .deliveryMode(DeliveryMode.NON_PERSISTENT) // the requester has a timeout; a late reply is useless
                .correlationId(message.getCorrelationId())
                .build());
    }
}
