package com.numaansystems.solace.internal;

import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.DestinationType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Micrometer instrumentation. Destination names are deliberately not used as
 * tags: reply topics are often unique per client and would explode cardinality.
 */
public class SolaceMetrics {

    private final MeterRegistry registry;

    public SolaceMetrics(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    /** Metrics that go nowhere, for use without a meter registry. */
    public static SolaceMetrics noop() {
        return new SolaceMetrics(new CompositeMeterRegistry());
    }

    public void recordPublish(DestinationType destinationType, DeliveryMode deliveryMode, boolean success,
            long durationNanos) {
        Timer.builder("solace.publish")
                .description("Publish latency and count")
                .tag("destination.type", destinationType.name())
                .tag("delivery.mode", deliveryMode.name())
                .tag("outcome", success ? "success" : "failure")
                .register(registry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordPublishRetry() {
        Counter.builder("solace.publish.retries")
                .description("Publish attempts repeated after a failure")
                .register(registry)
                .increment();
    }

    public void recordReceived(String consumer, boolean redelivered) {
        Counter.builder("solace.consumer.received")
                .description("Messages delivered to the consumer")
                .tag("consumer", consumer)
                .tag("redelivered", Boolean.toString(redelivered))
                .register(registry)
                .increment();
    }

    /**
     * @param outcome {@code success}, {@code failure} or {@code dead_lettered}
     */
    public void recordProcessed(String consumer, String outcome, long durationNanos) {
        Timer.builder("solace.consumer.processing")
                .description("Message processing latency and count, including in-process retries")
                .tag("consumer", consumer)
                .tag("outcome", outcome)
                .register(registry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordProcessingRetry(String consumer) {
        Counter.builder("solace.consumer.retries")
                .description("Handler invocations repeated after a failure")
                .tag("consumer", consumer)
                .register(registry)
                .increment();
    }

    /** Registers the active-consumer gauge; pass the result to {@link #remove(Meter)} on close. */
    public Meter registerConcurrencyGauge(String consumer, Supplier<Number> activeConsumers) {
        return Gauge.builder("solace.consumer.active", activeConsumers)
                .description("Currently active concurrent consumers")
                .tag("consumer", consumer)
                .register(registry);
    }

    public void remove(Meter meter) {
        if (meter != null) {
            registry.remove(meter);
        }
    }
}
