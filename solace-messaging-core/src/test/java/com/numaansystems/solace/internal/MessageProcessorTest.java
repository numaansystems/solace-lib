package com.numaansystems.solace.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.numaansystems.solace.AcknowledgeMode;
import com.numaansystems.solace.RetryPolicy;
import com.numaansystems.solace.SolaceConsumeException;
import com.numaansystems.solace.SolaceErrorHandler;
import com.numaansystems.solace.SolaceMessage;
import com.numaansystems.solace.SolaceMessageHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MessageProcessorTest {

    public record Order(String id) {
    }

    private final TextMessage jms = mock(TextMessage.class);
    private final Session session = mock(Session.class);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final List<Throwable> handledErrors = new ArrayList<>();
    private final SolaceErrorHandler errorHandler = (consumer, message, error) -> handledErrors.add(error);

    @BeforeEach
    void setUp() throws Exception {
        when(jms.getText()).thenReturn("{\"id\":\"42\"}");
        when(jms.getJMSCorrelationID()).thenReturn("REQ-1");
    }

    private <T> MessageProcessor<T> processor(AcknowledgeMode mode, int maxAttempts, String deadLetterQueue,
            Class<T> type, SolaceMessageHandler<T> handler) {
        return new MessageProcessor<>("test-consumer", mode,
                new RetryPolicy(maxAttempts, Duration.ZERO, 1.0, Duration.ZERO), deadLetterQueue, type, handler,
                new JacksonPayloadSerializer(new ObjectMapper()), new JmsMessageMapper(), errorHandler,
                new SolaceMetrics(meterRegistry), () -> false);
    }

    @Test
    void autoModeAcknowledgesOnlyAfterTheHandlerSucceeds() throws Exception {
        AtomicReference<SolaceMessage<Order>> received = new AtomicReference<>();
        SolaceMessageHandler<Order> handler = message -> {
            verify(jms, never()).acknowledge();
            received.set(message);
        };

        processor(AcknowledgeMode.AUTO, 1, null, Order.class, handler).onMessage(jms, session);

        verify(jms).acknowledge();
        verify(session, never()).recover();
        assertThat(received.get().getPayload()).isEqualTo(new Order("42"));
        assertThat(received.get().getCorrelationId()).isEqualTo("REQ-1");
        assertThat(meterRegistry.get("solace.consumer.processing").tag("outcome", "success").timer().count())
                .isEqualTo(1);
    }

    @Test
    void autoModeRetriesThenRequestsRedeliveryWithoutAcknowledging() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        SolaceMessageHandler<Order> handler = message -> {
            calls.incrementAndGet();
            throw new IllegalStateException("boom");
        };

        processor(AcknowledgeMode.AUTO, 3, null, Order.class, handler).onMessage(jms, session);

        assertThat(calls).hasValue(3);
        verify(jms, never()).acknowledge();
        verify(session).recover();
        assertThat(handledErrors).hasSize(1);
        assertThat(meterRegistry.get("solace.consumer.retries").counter().count()).isEqualTo(2);
    }

    @Test
    void handlerThatRecoversWithinRetriesIsAcknowledged() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        SolaceMessageHandler<Order> handler = message -> {
            if (calls.incrementAndGet() < 2) {
                throw new IllegalStateException("transient");
            }
        };

        processor(AcknowledgeMode.AUTO, 3, null, Order.class, handler).onMessage(jms, session);

        assertThat(calls).hasValue(2);
        verify(jms).acknowledge();
        verify(session, never()).recover();
        assertThat(handledErrors).isEmpty();
    }

    @Test
    void exhaustedRetriesMoveTheMessageToTheDeadLetterQueueThenAcknowledge() throws Exception {
        Queue deadLetterQueue = mock(Queue.class);
        MessageProducer producer = mock(MessageProducer.class);
        when(session.createQueue("orders.dlq")).thenReturn(deadLetterQueue);
        when(session.createProducer(deadLetterQueue)).thenReturn(producer);
        when(jms.getJMSPriority()).thenReturn(4);
        SolaceMessageHandler<Order> handler = message -> {
            throw new IllegalStateException("boom");
        };

        processor(AcknowledgeMode.AUTO, 2, "orders.dlq", Order.class, handler).onMessage(jms, session);

        verify(producer).send(jms, jakarta.jms.DeliveryMode.PERSISTENT, 4, 0L);
        verify(jms).acknowledge();
        verify(session, never()).recover();
        assertThat(meterRegistry.get("solace.consumer.processing").tag("outcome", "dead_lettered").timer().count())
                .isEqualTo(1);
    }

    @Test
    void transactedModeRollsBackByThrowingAndNeverAcknowledges() throws Exception {
        SolaceMessageHandler<Order> handler = message -> {
            throw new IllegalStateException("boom");
        };
        MessageProcessor<Order> processor = processor(AcknowledgeMode.TRANSACTED, 1, null, Order.class, handler);

        assertThatThrownBy(() -> processor.onMessage(jms, session))
                .isInstanceOf(SolaceConsumeException.class)
                .hasCauseInstanceOf(IllegalStateException.class);

        verify(jms, never()).acknowledge();
        verify(session, never()).recover();
    }

    @Test
    void transactedModeLeavesCommitToTheContainerOnSuccess() throws Exception {
        processor(AcknowledgeMode.TRANSACTED, 1, null, Order.class, message -> { }).onMessage(jms, session);

        verify(jms, never()).acknowledge();
        verify(session, never()).recover();
        verify(session, never()).commit();
    }

    @Test
    void manualModeAcknowledgesOnlyWhenTheHandlerAsks() throws Exception {
        processor(AcknowledgeMode.MANUAL, 1, null, Order.class, SolaceMessage::acknowledge).onMessage(jms, session);

        verify(jms).acknowledge();
        verify(session, never()).recover();
    }

    @Test
    void manualModeWithoutAcknowledgementRequestsRedelivery() throws Exception {
        processor(AcknowledgeMode.MANUAL, 1, null, Order.class, message -> { }).onMessage(jms, session);

        verify(jms, never()).acknowledge();
        verify(session).recover();
    }

    @Test
    void acknowledgeIsANoOpOutsideManualMode() throws Exception {
        SolaceMessageHandler<Order> handler = message -> {
            message.acknowledge();
            verify(jms, never()).acknowledge();
        };

        processor(AcknowledgeMode.AUTO, 1, null, Order.class, handler).onMessage(jms, session);

        verify(jms).acknowledge();
    }

    @Test
    void undecodablePayloadSkipsRetriesAndGoesToTheErrorHandler() throws Exception {
        when(jms.getText()).thenReturn("not json");
        AtomicInteger calls = new AtomicInteger();

        processor(AcknowledgeMode.AUTO, 3, null, Order.class, message -> calls.incrementAndGet())
                .onMessage(jms, session);

        assertThat(calls).hasValue(0);
        assertThat(handledErrors).hasSize(1);
        verify(jms, never()).acknowledge();
        verify(session).recover();
    }

    @Test
    void replyToMetadataReachesTheHandler() throws Exception {
        Topic replyTopic = mock(Topic.class);
        when(replyTopic.getTopicName()).thenReturn("pricing/response/clientA");
        when(jms.getJMSReplyTo()).thenReturn(replyTopic);
        AtomicReference<String> replyTo = new AtomicReference<>();

        processor(AcknowledgeMode.AUTO, 1, null, Order.class, message -> replyTo.set(message.getReplyTo()))
                .onMessage(jms, session);

        assertThat(replyTo).hasValue("pricing/response/clientA");
    }

    @Test
    void errorHandlerFailureDoesNotPreventRedelivery() throws Exception {
        SolaceErrorHandler throwing = (consumer, message, error) -> {
            throw new IllegalStateException("handler bug");
        };
        MessageProcessor<Order> processor = new MessageProcessor<>("test-consumer", AcknowledgeMode.AUTO,
                RetryPolicy.none(), null, Order.class, message -> {
                    throw new IllegalStateException("boom");
                },
                new JacksonPayloadSerializer(new ObjectMapper()), new JmsMessageMapper(), throwing,
                new SolaceMetrics(meterRegistry), () -> false);

        processor.onMessage(jms, session);

        verify(session).recover();
        verify(session, never()).createProducer(any());
    }
}
