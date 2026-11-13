package com.numaansystems.solace;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A received message: payload plus transport metadata. Immutable.
 *
 * <p>Broker-managed metadata ({@link #getProviderMessageId()},
 * {@link #getTimestamp()}, {@link #getExpiration()}, {@link #isRedelivered()},
 * {@link #getDeliveryCount()}) is set by the Solace client or broker.
 * Application-defined metadata is whatever the publisher supplied:
 * {@link #getMessageId()}, {@link #getCorrelationId()}, {@link #getReplyTo()},
 * {@link #getContentType()} and {@link #getProperties()}.
 *
 * @param <T> payload type
 */
public final class SolaceMessage<T> {

    /** Name of the application property that carries {@link #getMessageId()}. */
    public static final String APPLICATION_MESSAGE_ID_PROPERTY = "app_message_id";

    private final T payload;
    private final String messageId;
    private final String providerMessageId;
    private final String correlationId;
    private final String replyTo;
    private final DestinationType replyToType;
    private final String contentType;
    private final Instant expiration;
    private final Integer priority;
    private final Instant timestamp;
    private final DeliveryMode deliveryMode;
    private final String destination;
    private final DestinationType destinationType;
    private final boolean redelivered;
    private final Integer deliveryCount;
    private final Map<String, Object> properties;
    private final Runnable acknowledger;

    private SolaceMessage(Builder<T> builder) {
        this.payload = builder.payload;
        this.messageId = builder.messageId;
        this.providerMessageId = builder.providerMessageId;
        this.correlationId = builder.correlationId;
        this.replyTo = builder.replyTo;
        this.replyToType = builder.replyToType;
        this.contentType = builder.contentType;
        this.expiration = builder.expiration;
        this.priority = builder.priority;
        this.timestamp = builder.timestamp;
        this.deliveryMode = builder.deliveryMode;
        this.destination = builder.destination;
        this.destinationType = builder.destinationType;
        this.redelivered = builder.redelivered;
        this.deliveryCount = builder.deliveryCount;
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(builder.properties));
        this.acknowledger = builder.acknowledger;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** The same message and metadata with a different payload. */
    public <R> SolaceMessage<R> withPayload(R newPayload) {
        Builder<R> copy = new Builder<>();
        copy.payload = newPayload;
        copy.messageId = messageId;
        copy.providerMessageId = providerMessageId;
        copy.correlationId = correlationId;
        copy.replyTo = replyTo;
        copy.replyToType = replyToType;
        copy.contentType = contentType;
        copy.expiration = expiration;
        copy.priority = priority;
        copy.timestamp = timestamp;
        copy.deliveryMode = deliveryMode;
        copy.destination = destination;
        copy.destinationType = destinationType;
        copy.redelivered = redelivered;
        copy.deliveryCount = deliveryCount;
        copy.properties.putAll(properties);
        copy.acknowledger = acknowledger;
        return copy.build();
    }

    /** May be {@code null} for a message without a body. */
    public T getPayload() {
        return payload;
    }

    /**
     * Application message ID set by the publisher; falls back to the provider
     * ID for messages not published through this library. Stable across
     * redeliveries, so it is suitable as an idempotency key.
     */
    public String getMessageId() {
        return messageId != null ? messageId : providerMessageId;
    }

    /** ID assigned by the Solace client (JMSMessageID). */
    public String getProviderMessageId() {
        return providerMessageId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    /** Reply destination name, or {@code null}. */
    public String getReplyTo() {
        return replyTo;
    }

    /** Whether {@link #getReplyTo()} names a topic or a queue; {@code null} if there is no reply-to. */
    public DestinationType getReplyToType() {
        return replyToType;
    }

    public String getContentType() {
        return contentType;
    }

    /** When the message expires, or {@code null} if it does not. */
    public Instant getExpiration() {
        return expiration;
    }

    public Integer getPriority() {
        return priority;
    }

    /** When the publisher sent the message, or {@code null} if not recorded. */
    public Instant getTimestamp() {
        return timestamp;
    }

    public DeliveryMode getDeliveryMode() {
        return deliveryMode;
    }

    /** The destination the message was published to. */
    public String getDestination() {
        return destination;
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }

    /** {@code true} if the broker has attempted to deliver this message before. */
    public boolean isRedelivered() {
        return redelivered;
    }

    /** Broker delivery count, or {@code null} if the endpoint does not report it. */
    public Integer getDeliveryCount() {
        return deliveryCount;
    }

    /** Unmodifiable application-defined properties. */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * Acknowledge this message. Only has an effect for consumers registered
     * with {@link AcknowledgeMode#MANUAL}, and must be called on the handler
     * thread before the handler returns.
     *
     * @throws SolaceConsumeException if the acknowledgement fails
     */
    public void acknowledge() {
        if (acknowledger != null) {
            acknowledger.run();
        }
    }

    @Override
    public String toString() {
        // Deliberately excludes the payload and property values.
        return "SolaceMessage{messageId=" + getMessageId() + ", correlationId=" + correlationId
                + ", destination=" + destination + ", redelivered=" + redelivered + '}';
    }

    public static final class Builder<T> {

        private T payload;
        private String messageId;
        private String providerMessageId;
        private String correlationId;
        private String replyTo;
        private DestinationType replyToType;
        private String contentType;
        private Instant expiration;
        private Integer priority;
        private Instant timestamp;
        private DeliveryMode deliveryMode;
        private String destination;
        private DestinationType destinationType;
        private boolean redelivered;
        private Integer deliveryCount;
        private final Map<String, Object> properties = new LinkedHashMap<>();
        private Runnable acknowledger;

        private Builder() {
        }

        public Builder<T> payload(T payload) {
            this.payload = payload;
            return this;
        }

        public Builder<T> messageId(String messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder<T> providerMessageId(String providerMessageId) {
            this.providerMessageId = providerMessageId;
            return this;
        }

        public Builder<T> correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder<T> replyTo(String replyTo, DestinationType replyToType) {
            this.replyTo = replyTo;
            this.replyToType = replyTo == null ? null : replyToType;
            return this;
        }

        public Builder<T> contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder<T> expiration(Instant expiration) {
            this.expiration = expiration;
            return this;
        }

        public Builder<T> priority(Integer priority) {
            this.priority = priority;
            return this;
        }

        public Builder<T> timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder<T> deliveryMode(DeliveryMode deliveryMode) {
            this.deliveryMode = deliveryMode;
            return this;
        }

        public Builder<T> destination(String destination, DestinationType destinationType) {
            this.destination = destination;
            this.destinationType = destination == null ? null : destinationType;
            return this;
        }

        public Builder<T> redelivered(boolean redelivered) {
            this.redelivered = redelivered;
            return this;
        }

        public Builder<T> deliveryCount(Integer deliveryCount) {
            this.deliveryCount = deliveryCount;
            return this;
        }

        public Builder<T> property(String name, Object value) {
            this.properties.put(name, value);
            return this;
        }

        public Builder<T> acknowledger(Runnable acknowledger) {
            this.acknowledger = acknowledger;
            return this;
        }

        public SolaceMessage<T> build() {
            return new SolaceMessage<>(this);
        }
    }
}
