package com.numaansystems.solace;

/**
 * Handle for a registered consumer, used to manage its lifecycle.
 */
public interface ConsumerRegistration extends AutoCloseable {

    /** Unique name of this registration; also the {@code consumer} metric tag. */
    String getName();

    /** Start (or resume) message delivery. */
    void start();

    /**
     * Pause message delivery. Broker-side resources (a durable subscription, a
     * queue binding) stay in place; in-flight handlers are allowed to finish.
     */
    void stop();

    boolean isRunning();

    /**
     * {@code true} once at least one consumer is attached to the destination.
     * A topic subscription is established asynchronously after
     * {@link #start()}; messages published before this returns {@code true}
     * are not delivered to a non-durable subscription. Useful before sending a
     * request whose reply arrives on this registration.
     */
    boolean isReady();

    /**
     * Deregister: waits for in-flight handlers, then releases the consumer's
     * sessions and connection. A durable topic endpoint is not removed from
     * the broker. The registration cannot be restarted afterwards.
     */
    @Override
    void close();
}
