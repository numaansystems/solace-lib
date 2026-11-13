package com.numaansystems.solace.internal;

import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.DestinationType;
import com.numaansystems.solace.PublishOptions;
import com.numaansystems.solace.SerializedPayload;
import com.numaansystems.solace.SolaceMessage;
import com.numaansystems.solace.SolaceSerializationException;
import com.solacesystems.jms.SupportedProperty;
import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

import java.time.Instant;
import java.util.Enumeration;
import java.util.Map;

/**
 * Maps between the library's message model and JMS messages.
 *
 * <p>Standard JMS headers carry correlation ID, reply-to, expiration, priority
 * and timestamp. Content type uses the Solace content-type property so it is
 * visible to non-JMS clients (REST, MQTT 5, AMQP). The application message ID
 * travels as an ordinary application property.
 */
public class JmsMessageMapper {

    static final String CONTENT_TYPE_PROPERTY = SupportedProperty.SOLACE_JMS_PROP_HTTP_CONTENT_TYPE;
    static final String DMQ_ELIGIBLE_PROPERTY = SupportedProperty.SOLACE_JMS_PROP_DEAD_MSG_QUEUE_ELIGIBLE;
    private static final String DELIVERY_COUNT_PROPERTY = "JMSXDeliveryCount";

    public static Destination destination(Session session, String name, DestinationType type) throws JMSException {
        return type == DestinationType.QUEUE ? session.createQueue(name) : session.createTopic(name);
    }

    public Message toJms(Session session, SerializedPayload payload, PublishOptions options, String messageId,
            boolean dmqEligible) throws JMSException {
        Message message;
        if (payload.isText()) {
            message = session.createTextMessage(payload.getText());
        } else {
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(payload.getBytes());
            message = bytesMessage;
        }
        for (Map.Entry<String, Object> property : options.getProperties().entrySet()) {
            message.setObjectProperty(property.getKey(), property.getValue());
        }
        message.setStringProperty(SolaceMessage.APPLICATION_MESSAGE_ID_PROPERTY, messageId);
        if (payload.getContentType() != null) {
            message.setStringProperty(CONTENT_TYPE_PROPERTY, payload.getContentType());
        }
        if (dmqEligible) {
            message.setBooleanProperty(DMQ_ELIGIBLE_PROPERTY, true);
        }
        if (options.getCorrelationId() != null) {
            message.setJMSCorrelationID(options.getCorrelationId());
        }
        if (options.getReplyTo() != null) {
            message.setJMSReplyTo(destination(session, options.getReplyTo(), options.getReplyToType()));
        }
        return message;
    }

    /**
     * Reads everything except the body. The returned message has a
     * {@code null} payload.
     */
    public SolaceMessage<Object> readMetadata(Message jms, Runnable acknowledger) throws JMSException {
        SolaceMessage.Builder<Object> builder = SolaceMessage.builder();
        builder.providerMessageId(jms.getJMSMessageID());
        builder.correlationId(jms.getJMSCorrelationID());
        builder.priority(jms.getJMSPriority());
        builder.redelivered(jms.getJMSRedelivered());
        builder.deliveryMode(jms.getJMSDeliveryMode() == jakarta.jms.DeliveryMode.PERSISTENT
                ? DeliveryMode.PERSISTENT
                : DeliveryMode.NON_PERSISTENT);
        if (jms.getJMSTimestamp() > 0) {
            builder.timestamp(Instant.ofEpochMilli(jms.getJMSTimestamp()));
        }
        if (jms.getJMSExpiration() > 0) {
            builder.expiration(Instant.ofEpochMilli(jms.getJMSExpiration()));
        }
        Destination replyTo = jms.getJMSReplyTo();
        if (replyTo != null) {
            builder.replyTo(name(replyTo), type(replyTo));
        }
        Destination destination = jms.getJMSDestination();
        if (destination != null) {
            builder.destination(name(destination), type(destination));
        }

        Enumeration<?> names = jms.getPropertyNames();
        while (names != null && names.hasMoreElements()) {
            String name = String.valueOf(names.nextElement());
            if (SolaceMessage.APPLICATION_MESSAGE_ID_PROPERTY.equals(name)) {
                builder.messageId(jms.getStringProperty(name));
            } else if (CONTENT_TYPE_PROPERTY.equals(name)) {
                builder.contentType(jms.getStringProperty(name));
            } else if (DELIVERY_COUNT_PROPERTY.equals(name)) {
                builder.deliveryCount(jms.getIntProperty(name));
            } else if (!name.startsWith("JMS")) {
                // Remaining JMS-prefixed names are broker/client managed.
                builder.property(name, jms.getObjectProperty(name));
            }
        }
        builder.acknowledger(acknowledger);
        return builder.build();
    }

    /**
     * Reads the body. Returns {@code null} for a message without one.
     *
     * @throws SolaceSerializationException for JMS message types the library does not map
     */
    public SerializedPayload readBody(Message jms) throws JMSException {
        String contentType = jms.propertyExists(CONTENT_TYPE_PROPERTY)
                ? jms.getStringProperty(CONTENT_TYPE_PROPERTY)
                : null;
        if (jms instanceof TextMessage textMessage) {
            String text = textMessage.getText();
            return text == null ? null : SerializedPayload.text(text, contentType);
        }
        if (jms instanceof BytesMessage bytesMessage) {
            byte[] bytes = new byte[Math.toIntExact(bytesMessage.getBodyLength())];
            if (bytes.length > 0) {
                bytesMessage.readBytes(bytes);
            }
            bytesMessage.reset(); // keep the body readable, e.g. for dead-lettering
            return SerializedPayload.bytes(bytes, contentType);
        }
        if (jms instanceof jakarta.jms.MapMessage || jms instanceof jakarta.jms.StreamMessage
                || jms instanceof jakarta.jms.ObjectMessage) {
            throw new SolaceSerializationException(
                    "Unsupported JMS message type " + jms.getClass().getSimpleName()
                            + "; only text and bytes messages are supported");
        }
        return null;
    }

    private static String name(Destination destination) throws JMSException {
        if (destination instanceof Queue queue) {
            return queue.getQueueName();
        }
        if (destination instanceof Topic topic) {
            return topic.getTopicName();
        }
        return destination.toString();
    }

    private static DestinationType type(Destination destination) {
        return destination instanceof Queue ? DestinationType.QUEUE : DestinationType.TOPIC;
    }
}
