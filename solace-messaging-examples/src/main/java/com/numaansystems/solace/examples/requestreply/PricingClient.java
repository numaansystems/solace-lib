package com.numaansystems.solace.examples.requestreply;

import com.numaansystems.solace.ConsumerOptions;
import com.numaansystems.solace.ConsumerRegistration;
import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.DestinationType;
import com.numaansystems.solace.PublishOptions;
import com.numaansystems.solace.SolaceConsumer;
import com.numaansystems.solace.SolaceMessage;
import com.numaansystems.solace.SolacePublisher;
import com.numaansystems.solace.examples.requestreply.PricingMessages.PricingRequest;
import com.numaansystems.solace.examples.requestreply.PricingMessages.PricingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The requesting side. All request/response orchestration lives here, in the
 * application: pending-request state, the response listener, correlation
 * matching, timeouts and duplicate handling. The library only supplies
 * publish, consume, correlation ID and reply-to.
 *
 * <p>Replies arrive on a non-durable topic unique to this instance. If this
 * instance disconnects, replies in flight are lost and the caller sees a
 * timeout. A requester that must not lose replies needs a durable endpoint
 * (a queue, or a durable subscription) provisioned for its reply destination.
 */
@Component
public class PricingClient {

    private static final Logger log = LoggerFactory.getLogger(PricingClient.class);

    private final SolacePublisher publisher;
    private final String replyTopic = "examples/pricing/response/" + UUID.randomUUID();
    private final Map<String, CompletableFuture<PricingResponse>> pending = new ConcurrentHashMap<>();
    private final ConsumerRegistration replyListener;

    public PricingClient(SolaceConsumer consumer, SolacePublisher publisher) {
        this.publisher = publisher;
        this.replyListener = consumer.register(
                ConsumerOptions.topic(replyTopic).name("pricing-replies").build(),
                PricingResponse.class, this::onReply);
    }

    /** True once the reply subscription is active; requests sent earlier would never see their reply. */
    public boolean isReady() {
        return replyListener.isReady();
    }

    public CompletableFuture<PricingResponse> price(PricingRequest request, Duration timeout) {
        String correlationId = UUID.randomUUID().toString();
        CompletableFuture<PricingResponse> future = new CompletableFuture<>();
        pending.put(correlationId, future);
        // Whatever way the future ends (reply, timeout, publish failure), forget the request.
        future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((response, error) -> pending.remove(correlationId));

        try {
            publisher.publish(PricingResponder.REQUEST_QUEUE, request, PublishOptions.builder()
                    .destinationType(DestinationType.QUEUE)
                    .deliveryMode(DeliveryMode.PERSISTENT)
                    .correlationId(correlationId)
                    .replyTo(replyTopic)
                    .timeToLive(timeout) // no point processing a request nobody waits for
                    .build());
        } catch (RuntimeException ex) {
            future.completeExceptionally(ex);
        }
        return future;
    }

    private void onReply(SolaceMessage<PricingResponse> message) {
        CompletableFuture<PricingResponse> future =
                message.getCorrelationId() == null ? null : pending.remove(message.getCorrelationId());
        if (future == null) {
            // Late (already timed out), duplicate, or not ours.
            log.warn("Ignoring reply with unknown correlationId={}", message.getCorrelationId());
            return;
        }
        future.complete(message.getPayload());
    }
}
