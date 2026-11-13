package com.numaansystems.solace.internal;

import com.numaansystems.solace.AcknowledgeMode;
import com.numaansystems.solace.ConsumerOptions;
import com.numaansystems.solace.DestinationType;
import com.numaansystems.solace.RetryPolicy;
import com.numaansystems.solace.SubscriptionType;

import java.time.Duration;
import java.util.Objects;

/**
 * Consumer defaults, applied where {@code ConsumerOptions} leaves a value unset.
 *
 * @param concurrency      concurrent consumers per queue registration
 * @param acknowledgeMode  default acknowledgement mode
 * @param retry            default in-process retry policy
 * @param receiveTimeout   how long a consumer thread blocks waiting for a message;
 *                         also bounds how quickly it notices a stop request
 * @param recoveryInterval wait between attempts to re-establish a failed consumer connection
 * @param deadLetterQueue  default dead-letter queue, or {@code null} for none. May contain
 *                         the placeholder <code>{destination}</code>
 */
public record ConsumerSettings(
        int concurrency,
        AcknowledgeMode acknowledgeMode,
        RetryPolicy retry,
        Duration receiveTimeout,
        Duration recoveryInterval,
        String deadLetterQueue) {

    /** Placeholder replaced by the consumed queue name or the durable subscription name. */
    public static final String DESTINATION_PLACEHOLDER = "{destination}";

    public ConsumerSettings {
        Objects.requireNonNull(acknowledgeMode, "acknowledgeMode");
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(receiveTimeout, "receiveTimeout");
        Objects.requireNonNull(recoveryInterval, "recoveryInterval");
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be at least 1");
        }
        if (deadLetterQueue != null && deadLetterQueue.isBlank()) {
            deadLetterQueue = null;
        }
    }

    /**
     * The dead-letter queue for a consumer, or {@code null} for none.
     * Precedence: the queue set on the options; then "no dead-letter queue"
     * if the options opted out; then the configured default. A default that
     * would resolve to the queue being consumed is ignored, so that the
     * consumer of a dead-letter queue never forwards to itself.
     */
    public String deadLetterQueueFor(ConsumerOptions options) {
        if (options.getDeadLetterQueue() != null) {
            return options.getDeadLetterQueue();
        }
        if (options.isDeadLetterQueueDisabled() || deadLetterQueue == null) {
            return null;
        }
        String resolved = deadLetterQueue;
        if (resolved.contains(DESTINATION_PLACEHOLDER)) {
            String source;
            if (options.getDestinationType() == DestinationType.QUEUE) {
                source = options.getDestination();
            } else if (options.getSubscriptionType() == SubscriptionType.DURABLE) {
                source = options.getSubscriptionName();
            } else {
                // A non-durable topic subscription has no stable endpoint name to derive from.
                return null;
            }
            resolved = resolved.replace(DESTINATION_PLACEHOLDER, source);
        }
        if (options.getDestinationType() == DestinationType.QUEUE && resolved.equals(options.getDestination())) {
            return null;
        }
        return resolved;
    }

    public static ConsumerSettings defaults() {
        return new ConsumerSettings(1, AcknowledgeMode.AUTO,
                new RetryPolicy(3, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10)),
                Duration.ofSeconds(1), Duration.ofSeconds(5), null);
    }
}
