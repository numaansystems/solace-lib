package com.numaansystems.solace;

import java.util.Collection;

/**
 * Registers message handlers against queues and topic subscriptions.
 */
public interface SolaceConsumer {

    /**
     * Register a handler that receives payloads as sent: {@code String} for
     * text messages, {@code byte[]} for binary messages.
     *
     * @throws SolaceConfigurationException if the options are invalid or describe
     *                                      a combination Solace does not support
     */
    default ConsumerRegistration register(ConsumerOptions options, SolaceMessageHandler<Object> handler) {
        return register(options, Object.class, handler);
    }

    /**
     * Register a handler whose payloads are deserialized to {@code payloadType}.
     *
     * @throws SolaceConfigurationException if the options are invalid or describe
     *                                      a combination Solace does not support
     */
    <T> ConsumerRegistration register(ConsumerOptions options, Class<T> payloadType, SolaceMessageHandler<T> handler);

    /** A snapshot of the currently open registrations. */
    Collection<ConsumerRegistration> getRegistrations();
}
