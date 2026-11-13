package com.numaansystems.solace;

import java.util.Objects;

/**
 * Describes what to consume and how. Immutable; anything left unset falls back
 * to the consumer's configured defaults.
 *
 * <p>Start from one of the factory methods:
 * <pre>{@code
 * ConsumerOptions.queue("orders.inbound").concurrency(4).build();
 * ConsumerOptions.durableTopic("orders/created/>", "billing-orders").build();
 * ConsumerOptions.topic("prices/fx/>").build();
 * }</pre>
 */
public final class ConsumerOptions {

    private final String name;
    private final String destination;
    private final DestinationType destinationType;
    private final SubscriptionType subscriptionType;
    private final String subscriptionName;
    private final Integer concurrency;
    private final Integer maxConcurrency;
    private final AcknowledgeMode acknowledgeMode;
    private final RetryPolicy retryPolicy;
    private final String deadLetterQueue;
    private final boolean deadLetterQueueDisabled;
    private final String messageSelector;
    private final boolean autoStartup;
    private final SolaceErrorHandler errorHandler;

    private ConsumerOptions(Builder builder) {
        this.name = builder.name;
        this.destination = builder.destination;
        this.destinationType = builder.destinationType;
        this.subscriptionType = builder.subscriptionType;
        this.subscriptionName = builder.subscriptionName;
        this.concurrency = builder.concurrency;
        this.maxConcurrency = builder.maxConcurrency;
        this.acknowledgeMode = builder.acknowledgeMode;
        this.retryPolicy = builder.retryPolicy;
        this.deadLetterQueue = builder.deadLetterQueue;
        this.deadLetterQueueDisabled = builder.deadLetterQueueDisabled;
        this.messageSelector = builder.messageSelector;
        this.autoStartup = builder.autoStartup;
        this.errorHandler = builder.errorHandler;
        validate();
    }

    /** Consume from a queue. Several consumers on one queue compete for messages. */
    public static Builder queue(String queueName) {
        return new Builder(queueName, DestinationType.QUEUE, null, null);
    }

    /**
     * Durable topic subscription. {@code subscriptionName} identifies the
     * durable topic endpoint on the broker and must be stable across restarts.
     */
    public static Builder durableTopic(String topic, String subscriptionName) {
        return new Builder(topic, DestinationType.TOPIC, SubscriptionType.DURABLE, subscriptionName);
    }

    /** Non-durable topic subscription; it exists only while connected. */
    public static Builder topic(String topic) {
        return new Builder(topic, DestinationType.TOPIC, SubscriptionType.NON_DURABLE, null);
    }

    private void validate() {
        if (destination == null || destination.isBlank()) {
            throw new SolaceConfigurationException("Consumer destination must not be blank");
        }
        if (destinationType == DestinationType.QUEUE && subscriptionType != null) {
            throw new SolaceConfigurationException(
                    "Subscription type does not apply to queue consumers (queue '" + destination + "')");
        }
        if (destinationType == DestinationType.TOPIC) {
            if (subscriptionType == null) {
                throw new SolaceConfigurationException("Topic consumers need a subscription type");
            }
            if (subscriptionType == SubscriptionType.DURABLE
                    && (subscriptionName == null || subscriptionName.isBlank())) {
                throw new SolaceConfigurationException(
                        "Durable topic subscription on '" + destination + "' needs a subscription name");
            }
            if ((concurrency != null && concurrency > 1) || (maxConcurrency != null && maxConcurrency > 1)) {
                // Each concurrent consumer on a topic would be its own subscriber:
                // a non-durable one receives every message again, and a durable
                // topic endpoint accepts a single active subscriber.
                throw new SolaceConfigurationException("Topic subscription on '" + destination
                        + "' must use concurrency 1. To scale out, attach the topic to a queue on the broker "
                        + "and consume the queue.");
            }
        }
        if (concurrency != null && concurrency < 1) {
            throw new SolaceConfigurationException("concurrency must be at least 1");
        }
        if (maxConcurrency != null && concurrency != null && maxConcurrency < concurrency) {
            throw new SolaceConfigurationException("maxConcurrency must not be lower than concurrency");
        }
        if (deadLetterQueue != null && deadLetterQueue.isBlank()) {
            throw new SolaceConfigurationException("deadLetterQueue must not be blank");
        }
    }

    /** {@code null} means "derive a name from the destination". */
    public String getName() {
        return name;
    }

    public String getDestination() {
        return destination;
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }

    /** {@code null} for queue consumers. */
    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public Integer getMaxConcurrency() {
        return maxConcurrency;
    }

    public AcknowledgeMode getAcknowledgeMode() {
        return acknowledgeMode;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /** {@code null} means "use the configured default, if any". */
    public String getDeadLetterQueue() {
        return deadLetterQueue;
    }

    /** {@code true} if this consumer opted out of the configured default dead-letter queue. */
    public boolean isDeadLetterQueueDisabled() {
        return deadLetterQueueDisabled;
    }

    public String getMessageSelector() {
        return messageSelector;
    }

    public boolean isAutoStartup() {
        return autoStartup;
    }

    public SolaceErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public static final class Builder {

        private final String destination;
        private final DestinationType destinationType;
        private final SubscriptionType subscriptionType;
        private final String subscriptionName;
        private String name;
        private Integer concurrency;
        private Integer maxConcurrency;
        private AcknowledgeMode acknowledgeMode;
        private RetryPolicy retryPolicy;
        private String deadLetterQueue;
        private boolean deadLetterQueueDisabled;
        private String messageSelector;
        private boolean autoStartup = true;
        private SolaceErrorHandler errorHandler;

        private Builder(String destination, DestinationType destinationType,
                SubscriptionType subscriptionType, String subscriptionName) {
            this.destination = destination;
            this.destinationType = destinationType;
            this.subscriptionType = subscriptionType;
            this.subscriptionName = subscriptionName;
        }

        /** Unique registration name, used in logs, metrics and thread names. */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /** Number of concurrent consumers. Queues only; topics must stay at 1. */
        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        /** Upper bound the consumer may scale to under load. Queues only. */
        public Builder maxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            return this;
        }

        public Builder acknowledgeMode(AcknowledgeMode acknowledgeMode) {
            this.acknowledgeMode = Objects.requireNonNull(acknowledgeMode, "acknowledgeMode");
            return this;
        }

        /** In-process retry of a failing handler before the message is given up on. */
        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
            return this;
        }

        /**
         * Queue that receives messages the handler could not process after all
         * retries. Overrides the default from configuration
         * ({@code consumer.dead-letter-queue}). With neither, such messages
         * are left for broker redelivery and the endpoint's own max-redelivery
         * / dead message queue settings.
         */
        public Builder deadLetterQueue(String deadLetterQueue) {
            this.deadLetterQueue = deadLetterQueue;
            this.deadLetterQueueDisabled = false;
            return this;
        }

        /**
         * Do not dead-letter for this consumer, even if configuration supplies
         * a default dead-letter queue. Failed messages go back to the broker.
         */
        public Builder noDeadLetterQueue() {
            this.deadLetterQueue = null;
            this.deadLetterQueueDisabled = true;
            return this;
        }

        /** JMS message selector evaluated by the broker. */
        public Builder messageSelector(String messageSelector) {
            this.messageSelector = messageSelector;
            return this;
        }

        /** Whether delivery starts on registration (default) or on {@code start()}. */
        public Builder autoStartup(boolean autoStartup) {
            this.autoStartup = autoStartup;
            return this;
        }

        /** Overrides the default error handler for this consumer. */
        public Builder errorHandler(SolaceErrorHandler errorHandler) {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * @throws SolaceConfigurationException if the options are invalid
         */
        public ConsumerOptions build() {
            return new ConsumerOptions(this);
        }
    }
}
