package com.numaansystems.solace.internal;

import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.RetryPolicy;

import java.time.Duration;
import java.util.Objects;

/**
 * Publisher defaults.
 *
 * @param defaultDeliveryMode   delivery mode when the caller does not choose one
 * @param sendTimeout           how long a blocking publish waits
 * @param dmqEligible           default dead-message-queue eligibility
 * @param retry                 retry policy for failed publishes
 * @param retryOnUnknownOutcome also retry when the message may already have
 *                              reached the broker (can produce duplicates)
 * @param threads               publisher threads
 * @param queueCapacity         publishes that may wait for a publisher thread
 */
public record PublisherSettings(
        DeliveryMode defaultDeliveryMode,
        Duration sendTimeout,
        boolean dmqEligible,
        RetryPolicy retry,
        boolean retryOnUnknownOutcome,
        int threads,
        int queueCapacity) {

    public PublisherSettings {
        Objects.requireNonNull(defaultDeliveryMode, "defaultDeliveryMode");
        Objects.requireNonNull(sendTimeout, "sendTimeout");
        Objects.requireNonNull(retry, "retry");
        if (sendTimeout.isNegative() || sendTimeout.isZero()) {
            throw new IllegalArgumentException("sendTimeout must be positive");
        }
        if (threads < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("threads and queueCapacity must be at least 1");
        }
    }

    public static PublisherSettings defaults() {
        return new PublisherSettings(DeliveryMode.PERSISTENT, Duration.ofSeconds(5), true,
                RetryPolicy.none(), false, 4, 1000);
    }
}
