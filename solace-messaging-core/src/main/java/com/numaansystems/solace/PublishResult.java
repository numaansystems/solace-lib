package com.numaansystems.solace;

import java.time.Instant;

/**
 * Outcome of a successful publish.
 *
 * @param destination       name the message was sent to
 * @param destinationType   topic or queue
 * @param messageId         application message ID carried with the message
 * @param providerMessageId ID assigned by the Solace client, may be {@code null}
 * @param deliveryMode      delivery mode actually used
 * @param timestamp         send timestamp
 * @param attempts          number of send attempts made (1 unless retried)
 */
public record PublishResult(
        String destination,
        DestinationType destinationType,
        String messageId,
        String providerMessageId,
        DeliveryMode deliveryMode,
        Instant timestamp,
        int attempts) {
}
