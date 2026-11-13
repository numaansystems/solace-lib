package com.numaansystems.solace;

import java.time.Duration;
import java.util.Objects;

/**
 * Bounded retry with exponential backoff.
 *
 * @param maxAttempts     total attempts including the first one; 1 disables retry
 * @param initialInterval delay before the second attempt
 * @param multiplier      factor applied to the delay after each attempt (at least 1)
 * @param maxInterval     upper bound for a single delay
 */
public record RetryPolicy(int maxAttempts, Duration initialInterval, double multiplier, Duration maxInterval) {

    public RetryPolicy {
        Objects.requireNonNull(initialInterval, "initialInterval");
        Objects.requireNonNull(maxInterval, "maxInterval");
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be at least 1");
        }
        if (initialInterval.isNegative() || maxInterval.isNegative()) {
            throw new IllegalArgumentException("intervals must not be negative");
        }
    }

    /** A single attempt, no retry. */
    public static RetryPolicy none() {
        return new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
    }

    /**
     * Delay to wait after the given failed attempt (1-based) before the next one.
     */
    public Duration delayAfterAttempt(int failedAttempt) {
        double millis = initialInterval.toMillis() * Math.pow(multiplier, Math.max(0, failedAttempt - 1));
        long capped = (long) Math.min(millis, (double) maxInterval.toMillis());
        return Duration.ofMillis(Math.max(0, capped));
    }
}
