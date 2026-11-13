package com.numaansystems.solace;

/**
 * Extension point invoked when a message could not be processed: either the
 * handler failed on every attempt, or the payload could not be deserialized.
 * It runs before the message is dead-lettered or released for redelivery and
 * must not throw.
 */
@FunctionalInterface
public interface SolaceErrorHandler {

    /**
     * @param consumerName name of the consumer registration
     * @param message      the message; its payload is {@code null} if
     *                     deserialization failed
     * @param error        the last failure
     */
    void handle(String consumerName, SolaceMessage<?> message, Throwable error);
}
