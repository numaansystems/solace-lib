package com.numaansystems.solace;

/**
 * Application callback for consumed messages. Handlers may be invoked
 * concurrently when the consumer is registered with a concurrency above one,
 * and may see the same message more than once; make processing idempotent.
 *
 * @param <T> payload type
 */
@FunctionalInterface
public interface SolaceMessageHandler<T> {

    /**
     * Process one message. Throwing triggers the consumer's retry policy and,
     * once exhausted, dead-lettering or redelivery.
     */
    void onMessage(SolaceMessage<T> message) throws Exception;
}
