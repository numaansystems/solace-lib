package com.numaansystems.solace.internal;

import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.DestinationType;
import com.numaansystems.solace.PublishOptions;
import com.numaansystems.solace.SerializedPayload;
import com.numaansystems.solace.SolaceMessage;
import com.numaansystems.solace.SolaceSerializationException;
import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JmsMessageMapperTest {

    private final JmsMessageMapper mapper = new JmsMessageMapper();

    @Test
    void outboundTextMessageCarriesStandardHeadersAndProperties() throws Exception {
        Session session = mock(Session.class);
        TextMessage jms = mock(TextMessage.class);
        Topic replyTopic = mock(Topic.class);
        when(session.createTextMessage("{}")).thenReturn(jms);
        when(session.createTopic("pricing/response/clientA")).thenReturn(replyTopic);

        PublishOptions options = PublishOptions.builder()
                .correlationId("REQ-123456")
                .replyTo("pricing/response/clientA")
                .property("tenant", "acme")
                .build();

        mapper.toJms(session, SerializedPayload.text("{}", "application/json"), options, "msg-1", true);

        verify(jms).setJMSCorrelationID("REQ-123456");
        verify(jms).setJMSReplyTo(replyTopic);
        verify(jms).setObjectProperty("tenant", "acme");
        verify(jms).setStringProperty(SolaceMessage.APPLICATION_MESSAGE_ID_PROPERTY, "msg-1");
        verify(jms).setStringProperty(JmsMessageMapper.CONTENT_TYPE_PROPERTY, "application/json");
        verify(jms).setBooleanProperty(JmsMessageMapper.DMQ_ELIGIBLE_PROPERTY, true);
    }

    @Test
    void outboundBytesMessageWithQueueReplyToAndNoOptionalMetadata() throws Exception {
        Session session = mock(Session.class);
        BytesMessage jms = mock(BytesMessage.class);
        Queue replyQueue = mock(Queue.class);
        when(session.createBytesMessage()).thenReturn(jms);
        when(session.createQueue("replies")).thenReturn(replyQueue);

        PublishOptions options = PublishOptions.builder().replyTo("replies", DestinationType.QUEUE).build();

        mapper.toJms(session, SerializedPayload.bytes(new byte[] {1, 2}, null), options, "msg-2", false);

        verify(jms).writeBytes(new byte[] {1, 2});
        verify(jms).setJMSReplyTo(replyQueue);
        verify(jms, never()).setJMSCorrelationID(any());
        verify(jms, never()).setBooleanProperty(eq(JmsMessageMapper.DMQ_ELIGIBLE_PROPERTY), anyBoolean());
        verify(jms, never()).setStringProperty(eq(JmsMessageMapper.CONTENT_TYPE_PROPERTY), any());
    }

    @Test
    void inboundMetadataIsPreservedAndBrokerPropertiesAreSeparated() throws Exception {
        TextMessage jms = mock(TextMessage.class);
        Topic replyTopic = mock(Topic.class);
        Queue queue = mock(Queue.class);
        when(replyTopic.getTopicName()).thenReturn("pricing/response/clientA");
        when(queue.getQueueName()).thenReturn("pricing.request");
        when(jms.getJMSMessageID()).thenReturn("ID:broker-1");
        when(jms.getJMSCorrelationID()).thenReturn("REQ-123456");
        when(jms.getJMSReplyTo()).thenReturn(replyTopic);
        when(jms.getJMSDestination()).thenReturn(queue);
        when(jms.getJMSPriority()).thenReturn(7);
        when(jms.getJMSRedelivered()).thenReturn(true);
        when(jms.getJMSDeliveryMode()).thenReturn(jakarta.jms.DeliveryMode.PERSISTENT);
        when(jms.getJMSTimestamp()).thenReturn(1_000L);
        when(jms.getJMSExpiration()).thenReturn(5_000L);
        when(jms.getPropertyNames()).thenAnswer(invocation -> Collections.enumeration(List.of(
                SolaceMessage.APPLICATION_MESSAGE_ID_PROPERTY, JmsMessageMapper.CONTENT_TYPE_PROPERTY,
                "JMSXDeliveryCount", "JMS_Solace_isXML", "tenant")));
        when(jms.getStringProperty(SolaceMessage.APPLICATION_MESSAGE_ID_PROPERTY)).thenReturn("msg-1");
        when(jms.getStringProperty(JmsMessageMapper.CONTENT_TYPE_PROPERTY)).thenReturn("application/json");
        when(jms.getIntProperty("JMSXDeliveryCount")).thenReturn(3);
        when(jms.getObjectProperty("tenant")).thenReturn("acme");

        SolaceMessage<Object> message = mapper.readMetadata(jms, null);

        assertThat(message.getMessageId()).isEqualTo("msg-1");
        assertThat(message.getProviderMessageId()).isEqualTo("ID:broker-1");
        assertThat(message.getCorrelationId()).isEqualTo("REQ-123456");
        assertThat(message.getReplyTo()).isEqualTo("pricing/response/clientA");
        assertThat(message.getReplyToType()).isEqualTo(DestinationType.TOPIC);
        assertThat(message.getDestination()).isEqualTo("pricing.request");
        assertThat(message.getDestinationType()).isEqualTo(DestinationType.QUEUE);
        assertThat(message.getContentType()).isEqualTo("application/json");
        assertThat(message.getPriority()).isEqualTo(7);
        assertThat(message.isRedelivered()).isTrue();
        assertThat(message.getDeliveryCount()).isEqualTo(3);
        assertThat(message.getDeliveryMode()).isEqualTo(DeliveryMode.PERSISTENT);
        assertThat(message.getTimestamp()).isEqualTo(Instant.ofEpochMilli(1_000L));
        assertThat(message.getExpiration()).isEqualTo(Instant.ofEpochMilli(5_000L));
        assertThat(message.getProperties()).containsOnlyKeys("tenant");
        assertThat(message.getPayload()).isNull();
    }

    @Test
    void messageWithoutApplicationIdFallsBackToProviderId() throws Exception {
        TextMessage jms = mock(TextMessage.class);
        when(jms.getJMSMessageID()).thenReturn("ID:broker-9");

        SolaceMessage<Object> message = mapper.readMetadata(jms, null);

        assertThat(message.getMessageId()).isEqualTo("ID:broker-9");
        assertThat(message.getReplyTo()).isNull();
        assertThat(message.getExpiration()).isNull();
        assertThat(message.getDeliveryMode()).isEqualTo(DeliveryMode.NON_PERSISTENT);
    }

    @Test
    void bodiesAreReadFromTextAndBytesMessages() throws Exception {
        TextMessage text = mock(TextMessage.class);
        when(text.getText()).thenReturn("hello");
        BytesMessage bytes = mock(BytesMessage.class);
        when(bytes.getBodyLength()).thenReturn(2L);
        when(bytes.readBytes(any(byte[].class))).thenAnswer(invocation -> {
            byte[] target = invocation.getArgument(0);
            target[0] = 4;
            target[1] = 2;
            return 2;
        });

        assertThat(mapper.readBody(text).getText()).isEqualTo("hello");
        assertThat(mapper.readBody(bytes).getBytes()).containsExactly(4, 2);
        verify(bytes).reset();
    }

    @Test
    void unsupportedMessageTypesAreRejected() {
        assertThatThrownBy(() -> mapper.readBody(mock(MapMessage.class)))
                .isInstanceOf(SolaceSerializationException.class);
    }
}
