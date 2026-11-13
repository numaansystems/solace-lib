package com.numaansystems.solace;

/**
 * How the broker treats a published message. This is independent of whether a
 * subscription is durable (see {@link SubscriptionType}).
 */
public enum DeliveryMode {

    /**
     * Guaranteed messaging: the broker spools the message and acknowledges the
     * publisher. A blocking publish returns only after that acknowledgement.
     */
    PERSISTENT,

    /**
     * Not spooled to disk. With {@code direct-transport=false} (the library
     * default) the message still uses the guaranteed transport but may be lost
     * on broker failover; with {@code direct-transport=true} it is sent as a
     * Solace direct message with no publisher acknowledgement at all.
     */
    NON_PERSISTENT
}
