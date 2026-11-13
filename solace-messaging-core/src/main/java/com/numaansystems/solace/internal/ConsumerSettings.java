package com.numaansystems.solace.internal;

import com.numaansystems.solace.AcknowledgeMode;
import com.numaansystems.solace.RetryPolicy;

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
 */
public record ConsumerSettings(
        int concurrency,
        AcknowledgeMode acknowledgeMode,
        RetryPolicy retry,
        Duration receiveTimeout,
        Duration recoveryInterval) {

    public ConsumerSettings {
        Objects.requireNonNull(acknowledgeMode, "acknowledgeMode");
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(receiveTimeout, "receiveTimeout");
        Objects.requireNonNull(recoveryInterval, "recoveryInterval");
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be at least 1");
        }
    }

    public static ConsumerSettings defaults() {
        return new ConsumerSettings(1, AcknowledgeMode.AUTO,
                new RetryPolicy(3, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10)),
                Duration.ofSeconds(1), Duration.ofSeconds(5));
    }
}
