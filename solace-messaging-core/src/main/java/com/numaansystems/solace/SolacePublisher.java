package com.numaansystems.solace;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes messages to Solace topics and queues. Implementations are
 * thread-safe and share one broker connection.
 *
 * <p>The library never claims exactly-once delivery. See
 * {@link SolacePublishException#isOutcomeUnknown()} for when a failed publish
 * may still have reached the broker.
 */
public interface SolacePublisher {

    /**
     * Publish to a topic with default options.
     */
    default PublishResult publish(String destination, Object payload) {
        return publish(destination, payload, PublishOptions.defaults());
    }

    /**
     * Publish and wait for the result, up to the send timeout.
     *
     * <p>For {@link DeliveryMode#PERSISTENT} the call returns after the broker
     * has acknowledged the message. For {@link DeliveryMode#NON_PERSISTENT} a
     * normal return only means the message was handed to the client library.
     *
     * @param destination topic or queue name
     * @param payload     {@code String}, {@code byte[]}, or any object the
     *                    configured {@link PayloadSerializer} can convert
     * @throws SolaceSerializationException if the payload cannot be serialized
     * @throws SolacePublishException       if the publish failed or timed out
     */
    PublishResult publish(String destination, Object payload, PublishOptions options);

    /**
     * Publish on the library's publisher threads. The future completes
     * exceptionally with the same exceptions {@link #publish} throws. Ordering
     * between separate asynchronous publishes is not guaranteed.
     */
    CompletableFuture<PublishResult> publishAsync(String destination, Object payload, PublishOptions options);
}
