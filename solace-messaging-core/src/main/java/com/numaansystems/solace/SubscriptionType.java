package com.numaansystems.solace;

/**
 * Lifetime of a topic subscription. Only meaningful for
 * {@link DestinationType#TOPIC}; queue consumers have no subscription type.
 */
public enum SubscriptionType {

    /**
     * Backed by a durable topic endpoint on the broker, identified by the
     * subscription name. Messages published while the consumer is offline are
     * retained (persistent messages only) and delivered when it returns.
     */
    DURABLE,

    /**
     * Exists only while the consumer is connected. Messages published while
     * the consumer is offline are not delivered to it.
     */
    NON_DURABLE
}
