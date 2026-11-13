package com.numaansystems.solace;

/** When a consumed message is acknowledged to the broker. */
public enum AcknowledgeMode {

    /**
     * The library acknowledges after the handler returns normally. If the
     * handler keeps failing after the configured retries, the message is
     * dead-lettered (if configured) or left unacknowledged for redelivery.
     * At-least-once.
     */
    AUTO,

    /**
     * The handler calls {@link SolaceMessage#acknowledge()} on the handler
     * thread before returning. Returning without acknowledging, or throwing
     * after the retries are exhausted, requests redelivery. At-least-once.
     */
    MANUAL,

    /**
     * A local JMS transaction per message: committed when the handler returns
     * normally, rolled back otherwise. Requires {@code direct-transport=false}.
     * At-least-once; this is not a distributed (XA) transaction.
     */
    TRANSACTED
}
