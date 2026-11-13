package com.numaansystems.solace;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Per-message publish settings. Immutable; anything left unset falls back to
 * the publisher's configured defaults.
 */
public final class PublishOptions {

    private static final PublishOptions DEFAULTS = builder().build();

    private final DestinationType destinationType;
    private final DeliveryMode deliveryMode;
    private final String correlationId;
    private final String replyTo;
    private final DestinationType replyToType;
    private final String messageId;
    private final String contentType;
    private final Duration timeToLive;
    private final Integer priority;
    private final Duration sendTimeout;
    private final Boolean dmqEligible;
    private final Map<String, Object> properties;

    private PublishOptions(Builder builder) {
        this.destinationType = builder.destinationType;
        this.deliveryMode = builder.deliveryMode;
        this.correlationId = builder.correlationId;
        this.replyTo = builder.replyTo;
        this.replyToType = builder.replyToType;
        this.messageId = builder.messageId;
        this.contentType = builder.contentType;
        this.timeToLive = builder.timeToLive;
        this.priority = builder.priority;
        this.sendTimeout = builder.sendTimeout;
        this.dmqEligible = builder.dmqEligible;
        this.properties = MessageProperties.validatedCopy(builder.properties);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Topic destination, everything else from the publisher's defaults. */
    public static PublishOptions defaults() {
        return DEFAULTS;
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }

    /** {@code null} means "use the configured default delivery mode". */
    public DeliveryMode getDeliveryMode() {
        return deliveryMode;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public DestinationType getReplyToType() {
        return replyToType;
    }

    /** {@code null} means "generate one". */
    public String getMessageId() {
        return messageId;
    }

    public String getContentType() {
        return contentType;
    }

    /** {@code null} means the message does not expire. */
    public Duration getTimeToLive() {
        return timeToLive;
    }

    public Integer getPriority() {
        return priority;
    }

    public Duration getSendTimeout() {
        return sendTimeout;
    }

    public Boolean getDmqEligible() {
        return dmqEligible;
    }

    /** Unmodifiable application-defined properties. */
    public Map<String, Object> getProperties() {
        return properties;
    }

    public static final class Builder {

        private DestinationType destinationType = DestinationType.TOPIC;
        private DeliveryMode deliveryMode;
        private String correlationId;
        private String replyTo;
        private DestinationType replyToType = DestinationType.TOPIC;
        private String messageId;
        private String contentType;
        private Duration timeToLive;
        private Integer priority;
        private Duration sendTimeout;
        private Boolean dmqEligible;
        private final Map<String, Object> properties = new LinkedHashMap<>();

        private Builder() {
        }

        /** Whether the destination name is a topic (default) or a queue. */
        public Builder destinationType(DestinationType destinationType) {
            this.destinationType = Objects.requireNonNull(destinationType, "destinationType");
            return this;
        }

        public Builder deliveryMode(DeliveryMode deliveryMode) {
            this.deliveryMode = deliveryMode;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        /** Reply-to topic. */
        public Builder replyTo(String topic) {
            return replyTo(topic, DestinationType.TOPIC);
        }

        public Builder replyTo(String destination, DestinationType type) {
            this.replyTo = destination;
            this.replyToType = Objects.requireNonNull(type, "type");
            return this;
        }

        /**
         * Application message ID. If not set, the publisher generates a UUID.
         * It travels as an application property and is distinct from the ID
         * the broker client assigns.
         */
        public Builder messageId(String messageId) {
            this.messageId = messageId;
            return this;
        }

        /** Overrides the content type chosen by the serializer. */
        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        /**
         * Message lifetime. Queues honour it only when "respect TTL" is enabled
         * on the endpoint.
         */
        public Builder timeToLive(Duration timeToLive) {
            if (timeToLive != null && (timeToLive.isNegative() || timeToLive.isZero())) {
                throw new IllegalArgumentException("timeToLive must be positive");
            }
            this.timeToLive = timeToLive;
            return this;
        }

        /**
         * Priority 0 (lowest) to 9. Only takes effect on endpoints that have
         * "respect message priority" enabled.
         */
        public Builder priority(int priority) {
            if (priority < 0 || priority > 9) {
                throw new IllegalArgumentException("priority must be between 0 and 9");
            }
            this.priority = priority;
            return this;
        }

        /** How long a blocking publish waits before giving up. */
        public Builder sendTimeout(Duration sendTimeout) {
            if (sendTimeout != null && (sendTimeout.isNegative() || sendTimeout.isZero())) {
                throw new IllegalArgumentException("sendTimeout must be positive");
            }
            this.sendTimeout = sendTimeout;
            return this;
        }

        /**
         * Whether the broker may move the message to a dead message queue when
         * it expires or exceeds an endpoint's maximum redelivery count.
         */
        public Builder dmqEligible(boolean dmqEligible) {
            this.dmqEligible = dmqEligible;
            return this;
        }

        /**
         * Add an application-defined property.
         *
         * @throws IllegalArgumentException if the name or value type is not allowed
         */
        public Builder property(String name, Object value) {
            MessageProperties.validate(name, value);
            this.properties.put(name, value);
            return this;
        }

        public Builder properties(Map<String, ?> properties) {
            properties.forEach(this::property);
            return this;
        }

        public PublishOptions build() {
            return new PublishOptions(this);
        }
    }
}
